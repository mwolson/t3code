import type { OrchestrationV2ThreadShell } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import type { AgentAwarenessState } from "./agentAwareness.ts";

const NonEmpty = Schema.String.check(Schema.isMinLength(1));
const IsoTimestamp = Schema.String.check(
  Schema.makeFilter(
    (value) => /^\d{4}-\d{2}-\d{2}T/.test(value) && DateTime.make(value)._tag === "Some",
  ),
);
export const PersonalRelayTransition = Schema.Struct({ id: NonEmpty, at: IsoTimestamp });
export type PersonalRelayTransition = typeof PersonalRelayTransition.Type;
export const PersonalPublishClaims = Schema.Struct({
  t3_publish_seq: Schema.Int.check(Schema.isGreaterThan(0)),
  t3_transition: Schema.NullOr(PersonalRelayTransition),
  t3_transition_eligible_at: Schema.NullOr(IsoTimestamp),
}).check(
  Schema.makeFilter(
    (claims) => (claims.t3_transition === null) === (claims.t3_transition_eligible_at === null),
  ),
);
export type PersonalPublishClaims = typeof PersonalPublishClaims.Type;

export function agentAwarenessTransition(input: {
  readonly thread: Pick<
    OrchestrationV2ThreadShell,
    | "latestRunId"
    | "latestRunCompletedAt"
    | "pendingRuntimeRequest"
    | "createdAt"
    | "latestRunRequestedAt"
    | "latestRunStartedAt"
  >;
  readonly state: AgentAwarenessState | null;
}): PersonalRelayTransition | null {
  const { thread, state } = input;
  if (state === null) return null;
  if (state.phase === "waiting_for_approval" || state.phase === "waiting_for_input") {
    const request = thread.pendingRuntimeRequest;
    return request === null
      ? null
      : { id: `request:${request.id}`, at: DateTime.formatIso(request.createdAt) };
  }
  if (state.phase !== "completed" && state.phase !== "failed") return null;
  const at = DateTime.formatIso(
    thread.latestRunCompletedAt ??
      thread.latestRunStartedAt ??
      thread.latestRunRequestedAt ??
      thread.createdAt,
  );
  const id =
    thread.latestRunId === null
      ? `completed-at:${at}:${state.phase}`
      : `run:${thread.latestRunId}:${state.phase}`;
  return { id, at };
}

export function personalPublishClaims(input: {
  readonly sequence: number;
  readonly thread: Parameters<typeof agentAwarenessTransition>[0]["thread"];
  readonly state: AgentAwarenessState | null;
  // The publisher derives this from the durable event releasing the last completion hold.
  readonly completionEligibleAt?: DateTime.Utc | null;
}): PersonalPublishClaims {
  const transition = agentAwarenessTransition(input);
  let eligibleAt = transition?.at ?? null;
  if (
    transition !== null &&
    input.state?.phase === "completed" &&
    input.completionEligibleAt != null
  ) {
    eligibleAt = DateTime.formatIso(input.completionEligibleAt);
  }
  return {
    t3_publish_seq: input.sequence,
    t3_transition: transition,
    t3_transition_eligible_at: eligibleAt,
  };
}

export const PERSONAL_RELAY_ENROLLMENT_TYP = "t3-env-background-enroll+jwt";
export const PersonalRelayEnrollmentHeader = Schema.Struct({
  typ: Schema.Literal(PERSONAL_RELAY_ENROLLMENT_TYP),
  alg: Schema.Literal("EdDSA"),
});
export const PersonalRelayEnrollmentClaims = Schema.Struct({
  iss: NonEmpty,
  sub: NonEmpty,
  aud: NonEmpty,
  environmentId: NonEmpty,
  environmentPublicKey: NonEmpty,
  label: NonEmpty,
  jti: NonEmpty,
  iat: Schema.Int,
  exp: Schema.Int,
}).check(
  Schema.makeFilter(
    (claims) =>
      claims.iss === `t3-env:${claims.environmentId}` &&
      claims.sub === claims.environmentId &&
      claims.exp > claims.iat &&
      claims.exp - claims.iat <= 600,
  ),
);
export type PersonalRelayEnrollmentClaims = typeof PersonalRelayEnrollmentClaims.Type;
