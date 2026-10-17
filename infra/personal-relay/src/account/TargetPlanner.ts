import type {
  RelayAgentActivityAggregateState,
  RelayAgentActivityState,
} from "@t3tools/contracts/relay";
import { RelayAgentAwarenessPreferences } from "@t3tools/contracts/relay";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as ApnsClient from "t3code-relay/agentActivity/ApnsClient";
import * as ApnsDeliveries from "t3code-relay/agentActivity/ApnsDeliveries";
import * as ApnsDeliveryQueue from "t3code-relay/agentActivity/ApnsDeliveryQueue";
import * as AgentActivityRows from "t3code-relay/agentActivity/AgentActivityRows";
import * as DeliveryAttempts from "t3code-relay/agentActivity/DeliveryAttempts";
import * as LiveActivities from "t3code-relay/agentActivity/LiveActivities";
import * as RelayConfig from "t3code-relay/Config";
import {
  makeAggregateState,
  activityPhasePriority,
} from "t3code-relay/agentActivity/agentActivityAggregate";
import {
  alertAllowedForPhase,
  alertForActivityRows,
} from "t3code-relay/agentActivity/agentActivityAlerts";
import { notificationForActivity } from "t3code-relay/agentActivity/agentActivityPayloads";
import { expiresAtForJob } from "t3code-relay/agentActivity/apnsDeliveryJobs";
import type { PlannedApnsJob } from "../apns/ApnsSender.ts";
import type { AlertDeliveryIdentity, PendingAlertDelivery } from "./AlertLedger.ts";
import * as PassJobBuffer from "./PassJobBuffer.ts";

const decodePreferences = Schema.decodeEffect(
  Schema.fromJsonString(RelayAgentAwarenessPreferences),
);

export interface TargetSnapshot {
  readonly generation: string;
  readonly target: LiveActivities.TargetRow;
  readonly rows: ReadonlyArray<RelayAgentActivityState>;
  readonly pending: ReadonlyArray<PendingAlertDelivery>;
}
export interface TargetPlan {
  readonly aggregate: RelayAgentActivityAggregateState | null;
  readonly preferences: RelayAgentAwarenessPreferences;
  readonly jobs: ReadonlyArray<PlannedApnsJob>;
  readonly suppressed: ReadonlyArray<AlertDeliveryIdentity>;
}
export class TargetPlanError extends Schema.TaggedError<TargetPlanError>()("TargetPlanError", {
  cause: Schema.Defect(),
}) {
  override get message(): string {
    return "Personal relay target planning failed.";
  }
}
export class TargetPlanner extends Context.Service<
  TargetPlanner,
  {
    readonly plan: (
      snapshot: TargetSnapshot,
      nowMs: number,
    ) => Effect.Effect<TargetPlan, TargetPlanError>;
  }
>()("t3code-personal-relay/account/TargetPlanner") {}

export const make = Effect.gen(function* () {
  const config = yield* RelayConfig.RelayConfiguration;
  const crypto = yield* Crypto.Crypto;
  const apns = yield* ApnsClient.ApnsClient;
  const rows = yield* AgentActivityRows.AgentActivityRows;
  const attempts = yield* DeliveryAttempts.DeliveryAttempts;
  const activities = yield* LiveActivities.LiveActivities;
  return TargetPlanner.of({
    plan: Effect.fn("TargetPlanner.plan")(
      function* (snapshot, nowMs) {
        const aggregate = makeAggregateState({
          activeStates: snapshot.rows,
          terminalState: null,
          nowMs,
        });
        const preferences = yield* decodePreferences(snapshot.target.preferences_json);
        if (config.apns === null) {
          return { aggregate, preferences, jobs: [], suppressed: snapshot.pending };
        }
        const buffer = yield* PassJobBuffer.make;
        const queue = yield* ApnsDeliveryQueue.make.pipe(
          Effect.provideService(ApnsDeliveryQueue.ApnsDeliveryQueueSender, buffer.sender),
          Effect.provideService(RelayConfig.RelayConfiguration, config),
          Effect.provideService(Crypto.Crypto, crypto),
        );
        const reused = yield* ApnsDeliveries.make.pipe(
          Effect.provideService(ApnsDeliveryQueue.ApnsDeliveryQueue, queue),
          Effect.provideService(ApnsClient.ApnsClient, apns),
          Effect.provideService(RelayConfig.RelayConfiguration, config),
          Effect.provideService(AgentActivityRows.AgentActivityRows, rows),
          Effect.provideService(DeliveryAttempts.DeliveryAttempts, attempts),
          Effect.provideService(LiveActivities.LiveActivities, activities),
        );
        yield* reused.sendForTarget({ target: snapshot.target, aggregate, nowMs, replay: true });
        const captured = yield* buffer.drain;
        let live = captured.find((job) => job.payload.kind !== "push_notification")?.payload;
        const allowed = snapshot.pending.filter((delivery) =>
          alertAllowedForPhase(preferences, delivery.row.phase),
        );
        const suppressed: AlertDeliveryIdentity[] = snapshot.pending.filter(
          (delivery) => !alertAllowedForPhase(preferences, delivery.row.phase),
        );
        const jobs: PlannedApnsJob[] = [];
        const target = snapshot.target;
        if (allowed.length > 0) {
          const deliveryRows = allowed
            .toSorted(
              (a, b) => activityPhasePriority(a.row.phase) - activityPhasePriority(b.row.phase),
            )
            .map((delivery) => delivery.row);
          const alert = alertForActivityRows(deliveryRows);
          const firstAlertTitle = deliveryRows[0]!.threadTitle;
          if (
            target.activity_push_token &&
            preferences.liveActivitiesEnabled &&
            aggregate !== null
          ) {
            live = live ?? {
              version: 1,
              jobId: yield* crypto.randomUUIDv4(),
              kind: "live_activity_update",
              target: {
                userId: target.user_id,
                deviceId: target.device_id,
                token: target.activity_push_token,
                bundleId: target.bundle_id,
                apsEnvironment: target.aps_environment,
              },
              aggregate,
              notification: null,
              alert: null,
              createdAt: DateTime.formatIso(DateTime.makeUnsafe(nowMs)),
              expiresAt: expiresAtForJob(nowMs),
            };
            live = { ...live, alert };
            jobs.push({
              payload: live,
              channel: "live_activity",
              deliveries: allowed,
              firstAlertTitle,
            });
            live = undefined;
          } else if (target.push_token && preferences.notificationsEnabled) {
            for (const delivery of allowed.slice(0, 5))
              jobs.push({
                payload: {
                  version: 1,
                  jobId: yield* crypto.randomUUIDv4(),
                  kind: "push_notification",
                  target: {
                    userId: target.user_id,
                    deviceId: target.device_id,
                    token: target.push_token,
                    bundleId: target.bundle_id,
                    apsEnvironment: target.aps_environment,
                  },
                  aggregate: null,
                  notification: notificationForActivity(delivery.row),
                  alert: null,
                  createdAt: DateTime.formatIso(DateTime.makeUnsafe(nowMs)),
                  expiresAt: expiresAtForJob(nowMs),
                },
                channel: "notification",
                firstAlertTitle: delivery.row.threadTitle,
                deliveries: [delivery],
              });
          } else if (live?.kind === "live_activity_end") {
            jobs.push({
              payload: { ...live, alert },
              firstAlertTitle,
              channel: "live_activity",
              deliveries: allowed,
            });
            live = undefined;
          } else if (!preferences.notificationsEnabled) {
            suppressed.push(...allowed);
          }
        }
        if (live)
          jobs.push({
            payload: live,
            channel: "live_activity",
            deliveries: [],
            firstAlertTitle: "",
          });
        return {
          aggregate,
          preferences,
          jobs,
          suppressed,
        };
      },
      Effect.mapError((cause) => new TargetPlanError({ cause })),
    ),
  });
});
export const layer = Layer.effect(TargetPlanner, make);
