import { EnvironmentId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as ServerEnrollment from "../../../apps/server/src/relay/EnrollmentProof.ts";
import * as ServerSecrets from "../../../apps/server/src/auth/ServerSecretStore.ts";
import * as ServerEnvironment from "../../../apps/server/src/environment/ServerEnvironment.ts";
import { verifyEnrollmentProof } from "../src/enroll/EnrollmentProof.ts";
import { assert, describe, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Redacted from "effect/Redacted";
import { decodeJwt } from "jose";
import * as HostedSignatures from "t3code-relay/environments/EnvironmentPublishSignatures";
import * as RelayConfig from "t3code-relay/Config";
import * as DpopProofs from "t3code-relay/auth/DpopProofs";
import {
  makeTestRelay,
  THREAD_ID,
  unused,
} from "../../../apps/server/src/relay/AgentAwarenessRelay.testkit.ts";
describe("hosted proof compatibility", () => {
  it.effect("round-trips the server enrollment signer through the personal verifier", () =>
    Effect.gen(function* () {
      const { secrets } = yield* makeTestRelay();
      const { proof } = yield* ServerEnrollment.make({
        relayUrl: "https://private.test",
        label: "Test host",
      }).pipe(
        Effect.provideService(ServerSecrets.ServerSecretStore, secrets),
        Effect.provideService(ServerEnvironment.ServerEnvironmentIdentity, {
          getEnvironmentId: Effect.succeed(EnvironmentId.make("test-existing-environment")),
        }),
      );
      const claims = yield* verifyEnrollmentProof(proof, {
        publicOrigin: "https://private.test",
        nowMs: (yield* DateTime.now).epochMilliseconds,
      });
      assert.equal(claims.environmentId, "test-existing-environment");
      assert.equal(claims.label, "Test host");
    }).pipe(Effect.provide(NodeCrypto.layer)),
  );

  it.effect(
    "keeps the unchanged hosted proof verifier compatible with the signed personal claims",
    () =>
      Effect.gen(function* () {
        const { relay, secrets, publications } = yield* makeTestRelay();
        yield* relay.publishThread(THREAD_ID);
        const publication = publications[0]!;
        const keys = yield* secrets.get("cloud-link-ed25519-key-pair");
        const publicKey = JSON.parse(new TextDecoder().decode(Option.getOrThrow(keys)))
          .publicKey as string;
        const verifier = yield* HostedSignatures.EnvironmentPublishSignatures.pipe(
          Effect.provide(
            HostedSignatures.layer.pipe(
              Layer.provide(
                Layer.succeed(RelayConfig.RelayConfiguration, {
                  relayIssuer: "https://relay.example.test",
                  apns: {
                    environment: "sandbox",
                    teamId: "test",
                    keyId: "test",
                    privateKey: Redacted.make("test"),
                    bundleId: "test",
                  },
                  apnsDeliveryJobSigningSecret: Redacted.make("test"),
                  clerkSecretKey: Redacted.make("test"),
                  clerkPublishableKey: "test",
                  clerkJwtAudience: "test",
                  cloudMintPrivateKey: Redacted.make("test"),
                  cloudMintPublicKey: "test",
                  managedEndpointBaseDomain: undefined,
                  managedEndpointNamespace: undefined,
                }),
              ),
              Layer.provide(
                Layer.succeed(DpopProofs.DpopProofReplay, {
                  consume: () => Effect.succeed(true),
                  verifyAndConsume: unused,
                  pruneExpired: Effect.void,
                }),
              ),
              Layer.provide(NodeCrypto.layer),
            ),
          ),
        );
        const request = { state: publication.state, proof: publication.proof };
        const input = {
          environmentId: "relay-environment",
          environmentPublicKey: publicKey,
          threadId: THREAD_ID,
          request,
        };
        yield* verifier.verify(input).pipe(Effect.provide(NodeCrypto.layer));
        const parts = request.proof.split(".");
        const tampered = {
          ...decodeJwt(request.proof),
          t3_publish_seq: publication.claims.t3_publish_seq + 1,
        };
        parts[1] = Buffer.from(JSON.stringify(tampered)).toString("base64url");
        const error = yield* verifier
          .verify({ ...input, request: { ...request, proof: parts.join(".") } })
          .pipe(Effect.flip, Effect.provide(NodeCrypto.layer));
        assert.equal(error._tag, "EnvironmentPublishSignatureInvalid");
      }),
  );
});
