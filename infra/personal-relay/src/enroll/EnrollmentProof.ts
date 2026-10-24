import {
  PersonalRelayEnrollmentClaims,
  PersonalRelayEnrollmentHeader,
} from "@t3tools/shared/personalRelay";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { decodeJwt, decodeProtectedHeader, importSPKI, jwtVerify } from "jose";

export class EnrollmentProofError extends Schema.TaggedError<EnrollmentProofError>()(
  "EnrollmentProofError",
  {
    reason: Schema.Literals(["invalid-proof", "replayed-nonce"]),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "Environment enrollment proof rejected.";
  }
}

const decodeHeader = Schema.decodeUnknownSync(PersonalRelayEnrollmentHeader);
const decodeClaims = Schema.decodeUnknownSync(PersonalRelayEnrollmentClaims);
const decodeKey = Schema.decodeUnknownSync(
  Schema.Struct({ environmentPublicKey: Schema.String.check(Schema.isMinLength(1)) }),
);

export const verifyEnrollmentProof = Effect.fnUntraced(function* (
  proof: string,
  input: { readonly publicOrigin: string; readonly nowMs: number },
) {
  return yield* Effect.tryPromise({
    try: async () => {
      decodeHeader(decodeProtectedHeader(proof));
      // Only the verification key is read before verifying the signed payload.
      const { environmentPublicKey } = decodeKey(decodeJwt(proof));
      const key = await importSPKI(environmentPublicKey, "EdDSA");
      const verified = await jwtVerify(proof, key, {
        algorithms: ["EdDSA"],
        audience: input.publicOrigin,
        currentDate: DateTime.toDateUtc(DateTime.makeUnsafe(input.nowMs)),
        requiredClaims: ["exp", "iat", "jti", "iss", "sub", "aud"],
      });
      const claims = decodeClaims(verified.payload);
      if (claims.iat > Math.floor(input.nowMs / 1000) + 60) {
        throw new Error("Future enrollment proof");
      }
      return claims;
    },
    catch: (cause) => new EnrollmentProofError({ reason: "invalid-proof", cause }),
  });
});
