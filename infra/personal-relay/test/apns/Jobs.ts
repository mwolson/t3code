import * as Schema from "effect/Schema";
import { ApnsDeliveryJobPayload } from "t3code-relay/agentActivity/apnsDeliveryJobs";
import type { PlannedApnsJob } from "../../src/apns/ApnsSender.ts";

const decodePayload = Schema.decodeUnknownSync(ApnsDeliveryJobPayload);

export function makeJob(
  options: {
    readonly kind?: ApnsDeliveryJobPayload["kind"];
    readonly count?: number;
    readonly text?: string;
  } = {},
): PlannedApnsJob {
  const kind = options.kind ?? "live_activity_update";
  const text = options.text ?? "Thread title";
  const now = "2026-10-17T12:00:00.000Z";
  return {
    firstAlertTitle: text,
    channel: kind === "push_notification" ? "notification" : "live_activity",
    deliveries: Array.from({ length: options.count ?? 1 }, (_, index) => ({
      alertId: `alert-${index}`,
      deviceId: "test-device",
    })),
    payload: decodePayload({
      version: 1,
      jobId: "job-1",
      kind,
      target: {
        userId: "owner",
        deviceId: "test-device",
        token: "0123456789abcdef".repeat(4),
        bundleId: "com.example.test",
        apsEnvironment: "sandbox",
      },
      aggregate:
        kind === "push_notification"
          ? null
          : {
              title: text,
              subtitle: text,
              activeCount: 20,
              updatedAt: now,
              activities: Array.from({ length: 5 }, (_, index) => ({
                environmentId: "e".repeat(36),
                threadId: `${index}${"t".repeat(35)}`,
                projectTitle: text,
                threadTitle: text,
                modelTitle: text,
                phase: "completed",
                status: text,
                updatedAt: now,
                deepLink: `/environments/${"e".repeat(36)}/threads/${index}${"t".repeat(35)}`,
              })),
            },
      notification:
        kind === "push_notification"
          ? {
              title: text,
              body: text,
              environmentId: "e".repeat(36),
              threadId: "t".repeat(36),
              deepLink: "/test",
            }
          : null,
      alert:
        kind === "push_notification"
          ? null
          : {
              title: "Agents finished",
              body: Array.from({ length: options.count ?? 1 }, () => text).join(", "),
            },
      createdAt: now,
      expiresAt: "2026-10-17T12:10:00.000Z",
    }),
  };
}
