import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import type { ApnsClient } from "t3code-relay/agentActivity/ApnsClient";
import {
  sanitizeAgentActivityAggregateState,
  sanitizeApnsNotificationPayload,
} from "t3code-relay/agentActivity/agentActivityPayloads";
import type { PlannedApnsJob } from "./ApnsSender.ts";

export const APNS_PAYLOAD_BUDGET = 3800;

export class ApnsPayloadBudgetError extends Schema.TaggedError<ApnsPayloadBudgetError>()(
  "ApnsPayloadBudgetError",
  { reason: Schema.Literals(["invalid-job", "routing-fields-too-large"]) },
) {
  override get message(): string {
    return "APNs payload cannot fit the request budget.";
  }
}

export class ApnsRequestEncodingError extends Schema.TaggedError<ApnsRequestEncodingError>()(
  "ApnsRequestEncodingError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "APNs request encoding failed.";
  }
}

type Client = ApnsClient["Service"];
type PreparedRequest =
  | {
      readonly kind: "notification";
      readonly request: ReturnType<Client["makePushNotificationRequest"]>;
    }
  | {
      readonly kind: "live_activity";
      readonly request: ReturnType<Client["makeLiveActivityRequest"]>;
    };

export function prepareRequest(
  job: PlannedApnsJob,
  client: Client,
  nowMs: number,
): PreparedRequest {
  const payload = job.payload;
  let aggregate =
    payload.aggregate === null ? null : sanitizeAgentActivityAggregateState(payload.aggregate);
  let notification =
    payload.notification === null ? null : sanitizeApnsNotificationPayload(payload.notification);
  let alert = payload.alert ?? null;
  if (alert !== null) {
    const count = job.deliveries.length;
    alert = {
      title: truncate(alert.title, 120),
      body:
        count > 1
          ? `${truncate(job.firstAlertTitle, 100)} and ${count - 1} more`
          : truncate(alert.body, 120),
    };
  }
  const build = (): PreparedRequest => {
    if (payload.kind === "push_notification") {
      if (notification === null) throw new ApnsPayloadBudgetError({ reason: "invalid-job" });
      return {
        kind: "notification",
        request: client.makePushNotificationRequest({ token: payload.target.token, notification }),
      };
    }
    const base = {
      token: payload.target.token,
      nowEpochSeconds: Math.floor(nowMs / 1000),
      nowIso: DateTime.formatIso(DateTime.makeUnsafe(nowMs)),
      alert,
    };
    if (payload.kind === "live_activity_end") {
      return {
        kind: "live_activity",
        request: client.makeLiveActivityRequest({ ...base, event: "end", state: aggregate }),
      };
    }
    if (aggregate === null) throw new ApnsPayloadBudgetError({ reason: "invalid-job" });
    return {
      kind: "live_activity",
      request: client.makeLiveActivityRequest({
        ...base,
        event: "update",
        state: aggregate,
      }),
    };
  };
  let request = build();
  if (fits(request)) return request;
  // Measure the outer JSON too: Live Activity props are a JSON string inside it.
  const originalAggregate = aggregate;
  const originalNotification = notification;
  for (const length of [60, 30, 12]) {
    if (aggregate !== null) {
      aggregate = {
        ...originalAggregate!,
        title: truncate(aggregate.title, length),
        subtitle: truncate(aggregate.subtitle, length),
        activities: aggregate.activities.map((row) => ({
          ...row,
          projectTitle: truncate(row.projectTitle, length),
          threadTitle: truncate(row.threadTitle, length),
          modelTitle: truncate(row.modelTitle, length),
          status: truncate(row.status, length),
        })),
      };
    }
    if (notification !== null)
      notification = {
        ...originalNotification!,
        title: truncate(notification.title, length),
        body: truncate(notification.body, length),
      };
    if (alert !== null)
      alert = {
        title: truncate(alert.title, length),
        body: `${job.deliveries.length} agent update${job.deliveries.length === 1 ? "" : "s"}`,
      };
    request = build();
    if (fits(request)) return request;
  }
  while (aggregate !== null && aggregate.activities.length > 0) {
    aggregate = { ...aggregate, activities: aggregate.activities.slice(0, -1) };
    request = build();
    if (fits(request)) return request;
  }
  throw new ApnsPayloadBudgetError({ reason: "routing-fields-too-large" });
}

function fits(request: PreparedRequest): boolean {
  return Buffer.byteLength(JSON.stringify(request.request.payload), "utf8") <= APNS_PAYLOAD_BUDGET;
}

function truncate(value: string, length: number): string {
  const points = Array.from(value);
  return points.length <= length ? value : `${points.slice(0, length - 3).join("")}...`;
}
