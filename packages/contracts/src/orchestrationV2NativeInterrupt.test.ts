import { describe, expect, it } from "vite-plus/test";
import * as DateTime from "effect/DateTime";
import * as Schema from "effect/Schema";
import { EventId, ProviderThreadId, ProviderTurnId, RunId, ThreadId } from "./index.ts";
import { OrchestrationV2Command, OrchestrationV2DomainEventJson } from "./orchestrationV2.ts";

describe("native interrupt wire contract", () => {
  const common = { type: "run.interrupt", commandId: "stop", threadId: "thread" };
  it("retains ordinary explicit targets and optional native-only targets", () => {
    const decode = Schema.decodeUnknownSync(OrchestrationV2Command);
    expect(decode({ ...common, runId: "run" })).toEqual({ ...common, runId: "run" });
    for (const target of [{}, { runId: "run" }]) {
      const command = { ...common, ...target, intent: "provider_native_only" };
      expect(decode(command)).toEqual(command);
    }
    expect(() => decode(common)).toThrow();
  });
  it("round trips native request and no-op receipts", () => {
    const base = {
      id: EventId.make("event"),
      threadId: ThreadId.make("thread"),
      occurredAt: DateTime.makeUnsafe("2026-09-22T00:00:00Z"),
    };
    const events = [
      {
        ...base,
        type: "provider-turn.interrupt-requested" as const,
        payload: {
          targetThreadId: ThreadId.make("child"),
          providerThreadId: ProviderThreadId.make("native"),
          providerTurnId: ProviderTurnId.make("turn"),
          reason: null,
        },
      },
      {
        ...base,
        type: "run.interrupt-noop" as const,
        runId: RunId.make("run"),
        payload: { reason: "Already completed" },
      },
    ];
    for (const event of events) {
      const encoded = Schema.encodeSync(OrchestrationV2DomainEventJson)(event);
      expect(Schema.decodeUnknownSync(OrchestrationV2DomainEventJson)(encoded)).toEqual(event);
    }
  });
});
