import type { OpencodeClient, V2Event } from "@opencode-ai/sdk-next/v2";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  ProviderInstanceId,
  ProviderReplayEntry,
  type ProviderReplayTranscript,
} from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as P from "effect/Predicate";
import * as Schema from "effect/Schema";

import { ServerConfig } from "../../config.ts";
import {
  NoOpProviderEventLoggers,
  ProviderEventLoggers,
} from "../../provider/Layers/ProviderEventLoggers.ts";
import {
  OpenCode2Runtime,
  OpenCode2RuntimeError,
  type OpenCode2RuntimeShape,
} from "../../provider/opencode2Runtime.ts";
import { layer as idAllocatorLayer } from "../IdAllocator.ts";
import { ProviderAdapterDriverCreateError } from "../ProviderAdapterDriver.ts";
import { makeDriverLayer as makeProviderAdapterRegistryDriverLayer } from "../ProviderAdapterRegistry.ts";
import {
  makeReplayServerConfig,
  type OrchestratorV2ProviderReplayHarness,
} from "../testkit/ProviderReplayHarness.ts";
import {
  OPENCODE2_DRIVER_KIND,
  OPENCODE2_PROVIDER,
  OPENCODE2_SDK_PROTOCOL,
  OpenCode2AdapterV2Driver,
} from "./OpenCode2AdapterV2.ts";

export const OPENCODE2_SDK_REPLAY_PROTOCOL = OPENCODE2_SDK_PROTOCOL;
export const OPENCODE2_REPLAY_INSTANCE_ID = ProviderInstanceId.make("opencode2");

const OpenCode2SdkReplayTranscript = Schema.Struct({
  provider: Schema.Literal(OPENCODE2_PROVIDER),
  protocol: Schema.Literal(OPENCODE2_SDK_REPLAY_PROTOCOL),
  version: Schema.String,
  scenario: Schema.String,
  metadata: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  entries: Schema.Array(ProviderReplayEntry),
});
export type OpenCode2SdkReplayTranscript = typeof OpenCode2SdkReplayTranscript.Type;
const decodeOpenCode2SdkReplayTranscript = Schema.decodeUnknownEffect(OpenCode2SdkReplayTranscript);

export class OpenCode2ReplayTranscriptDecodeError extends Schema.TaggedErrorClass<OpenCode2ReplayTranscriptDecodeError>()(
  "OpenCode2ReplayTranscriptDecodeError",
  {
    driver: Schema.optional(Schema.String),
    protocol: Schema.optional(Schema.String),
    scenario: Schema.optional(Schema.String),
    cause: Schema.Defect(),
  },
) {
  override get message(): string {
    return `Failed to decode OpenCode 2 replay transcript for scenario ${this.scenario ?? "<unknown>"}.`;
  }
}

export class OpenCode2ReplayMismatchError extends Schema.TaggedErrorClass<OpenCode2ReplayMismatchError>()(
  "OpenCode2ReplayMismatchError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    expected: Schema.Unknown,
    actual: Schema.Unknown,
  },
) {
  override get message(): string {
    return `OpenCode 2 replay frame mismatch at cursor ${this.cursor} in scenario ${this.scenario}.`;
  }
}

export class OpenCode2ReplayIncompleteError extends Schema.TaggedErrorClass<OpenCode2ReplayIncompleteError>()(
  "OpenCode2ReplayIncompleteError",
  {
    scenario: Schema.String,
    cursor: Schema.Number,
    remaining: Schema.Number,
  },
) {
  override get message(): string {
    return `OpenCode 2 replay ended with ${this.remaining} unconsumed entries in scenario ${this.scenario}.`;
  }
}

export const OpenCode2ReplayError = Schema.Union([
  OpenCode2ReplayTranscriptDecodeError,
  OpenCode2ReplayMismatchError,
  OpenCode2ReplayIncompleteError,
]);
export type OpenCode2ReplayError = typeof OpenCode2ReplayError.Type;
export const OpenCode2OrchestratorReplayHarnessError = Schema.Union([
  OpenCode2ReplayError,
  ProviderAdapterDriverCreateError,
]);
export type OpenCode2OrchestratorReplayHarnessError =
  typeof OpenCode2OrchestratorReplayHarnessError.Type;

function replayValueMatches(expected: unknown, actual: unknown): boolean {
  if (expected === "<any>" || expected === "<workspace>") return true;
  if (Array.isArray(expected)) {
    return (
      Array.isArray(actual) &&
      expected.length === actual.length &&
      expected.every((entry, index) => replayValueMatches(entry, actual[index]))
    );
  }
  if (P.isObject(expected)) {
    if (!P.isObject(actual)) return false;
    return Object.entries(expected).every(([key, value]) => replayValueMatches(value, actual[key]));
  }
  return Object.is(expected, actual);
}

function frameRecord(frame: unknown): Record<string, unknown> | null {
  return P.isObject(frame) ? frame : null;
}

class OpenCode2ReplayController {
  private cursor = 0;
  private readonly waiters = new Set<() => void>();
  private failure: unknown = null;
  private readonly transcript: OpenCode2SdkReplayTranscript;

  constructor(transcript: OpenCode2SdkReplayTranscript) {
    this.transcript = transcript;
  }

  async expectOutbound(actual: unknown): Promise<void> {
    try {
      const entry = this.transcript.entries[this.cursor];
      if (entry?.type !== "expect_outbound" || !replayValueMatches(entry.frame, actual)) {
        throw new OpenCode2ReplayMismatchError({
          scenario: this.transcript.scenario,
          cursor: this.cursor,
          expected: entry?.type === "expect_outbound" ? entry.frame : (entry ?? null),
          actual,
        });
      }
      this.advance();
    } catch (cause) {
      this.fail(cause);
      throw cause;
    }
  }

  async response(operation: string): Promise<unknown> {
    while (true) {
      this.throwFailure();
      const entry = this.transcript.entries[this.cursor];
      if (entry?.type === "emit_inbound") {
        const frame = frameRecord(entry.frame);
        if (frame?.type === "sdk.response" && frame.operation === operation) {
          if (entry.afterMs !== undefined && entry.afterMs > 0) {
            await Effect.runPromise(Effect.sleep(Duration.millis(entry.afterMs)));
          }
          const data = frame.data;
          this.advance();
          return data;
        }
      }
      if (entry?.type === "runtime_exit") {
        const mismatch = new OpenCode2ReplayMismatchError({
          scenario: this.transcript.scenario,
          cursor: this.cursor,
          expected: { type: "sdk.response", operation },
          actual: entry,
        });
        this.fail(mismatch);
        throw mismatch;
      }
      await this.changed();
    }
  }

  async *events(signal?: AbortSignal): AsyncIterable<V2Event> {
    while (true) {
      if (signal?.aborted === true) return;
      this.throwFailure();
      const entry = this.transcript.entries[this.cursor];
      if (entry?.type === "emit_inbound") {
        const frame = frameRecord(entry.frame);
        if (frame?.type === "sdk.event") {
          if (entry.afterMs !== undefined && entry.afterMs > 0) {
            await Effect.runPromise(Effect.sleep(Duration.millis(entry.afterMs)));
          }
          const event = frame.event as V2Event;
          this.advance();
          yield event;
          continue;
        }
      }
      if (entry?.type === "runtime_exit") {
        this.advance();
        if (entry.status === "success") return;
        const mismatch = new OpenCode2ReplayMismatchError({
          scenario: this.transcript.scenario,
          cursor: this.cursor - 1,
          expected: { status: "success" },
          actual: entry,
        });
        this.fail(mismatch);
        throw mismatch;
      }
      await this.changed(signal);
    }
  }

  assertComplete(): void {
    while (this.transcript.entries[this.cursor]?.type === "runtime_exit") {
      const exit = this.transcript.entries[this.cursor];
      if (exit?.type !== "runtime_exit" || exit.status !== "success") break;
      this.cursor += 1;
    }
    this.throwFailure();
    if (this.cursor !== this.transcript.entries.length) {
      throw new OpenCode2ReplayIncompleteError({
        scenario: this.transcript.scenario,
        cursor: this.cursor,
        remaining: this.transcript.entries.length - this.cursor,
      });
    }
  }

  private advance(): void {
    this.cursor += 1;
    for (const waiter of this.waiters) waiter();
    this.waiters.clear();
  }

  private fail(cause: unknown): void {
    this.failure = cause;
    for (const waiter of this.waiters) waiter();
    this.waiters.clear();
  }

  private throwFailure(): void {
    if (this.failure !== null) throw this.failure;
  }

  private changed(signal?: AbortSignal): Promise<void> {
    if (signal?.aborted === true) return Promise.resolve();
    return new Promise((resolve) => {
      const done = () => {
        signal?.removeEventListener("abort", done);
        this.waiters.delete(done);
        resolve();
      };
      this.waiters.add(done);
      signal?.addEventListener("abort", done, { once: true });
    });
  }
}

function makeReplayClient(controller: OpenCode2ReplayController): OpencodeClient {
  const request = async (operation: string, input: unknown) => {
    await controller.expectOutbound({ type: operation, input });
    return { data: { data: await controller.response(operation) } };
  };
  return {
    v2: {
      event: {
        subscribe: async (options?: { readonly signal?: AbortSignal }) => {
          await controller.expectOutbound({ type: "event.subscribe" });
          return { stream: controller.events(options?.signal) };
        },
      },
      message: {
        list: (input: unknown) => request("message.list", input),
      },
      session: {
        compact: (input: unknown) => request("session.compact", input),
        create: (input: unknown) => request("session.create", input),
        fork: (input: unknown) => request("session.fork", input),
        get: (input: unknown) => request("session.get", input),
        interrupt: (input: unknown) => request("session.interrupt", input),
        pending: {
          list: (input: unknown) => request("session.pending.list", input),
        },
        permission: {
          reply: (input: unknown) => request("session.permission.reply", input),
        },
        prompt: (input: unknown) => request("session.prompt", input),
        remove: (input: unknown) => request("session.remove", input),
        question: {
          reply: (input: unknown) => request("session.question.reply", input),
        },
        revert: {
          commit: (input: unknown) => request("session.revert.commit", input),
          stage: (input: unknown) => request("session.revert.stage", input),
        },
        switchAgent: (input: unknown) => request("session.switchAgent", input),
        switchModel: (input: unknown) => request("session.switchModel", input),
        wait: (input: unknown) => request("session.wait", input),
      },
      shell: {
        list: (input: unknown) => request("shell.list", input),
        output: (input: unknown) => request("shell.output", input),
        remove: (input: unknown) => request("shell.remove", input),
      },
    },
  } as unknown as OpencodeClient;
}

function makeOpenCode2ReplayRuntimeLayer(transcript: OpenCode2SdkReplayTranscript) {
  return Layer.effect(
    OpenCode2Runtime,
    Effect.gen(function* () {
      const controller = new OpenCode2ReplayController(transcript);
      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          controller.assertComplete();
        }),
      );
      const client = makeReplayClient(controller);
      return OpenCode2Runtime.of({
        startOpenCode2ServerProcess: () =>
          Effect.fail(
            new OpenCode2RuntimeError({
              operation: "startOpenCode2ServerProcess",
              detail: "OpenCode 2 replay uses an external in-memory SDK boundary.",
            }),
          ),
        connectToOpenCode2Server: () =>
          Effect.succeed({
            url: "replay://opencode2",
            password: "replay-password",
            exitCode: null,
            external: true,
          }),
        createOpenCode2SdkClient: () => client,
      } satisfies OpenCode2RuntimeShape);
    }),
  );
}

export function makeOpenCode2ProviderAdapterRegistryReplayLayer(
  transcript: OpenCode2SdkReplayTranscript,
) {
  const serverConfigLayer = Layer.effect(
    ServerConfig,
    makeReplayServerConfig(transcript.scenario).pipe(Effect.orDie),
  ).pipe(Layer.provide(NodeServices.layer));
  return makeProviderAdapterRegistryDriverLayer({
    drivers: [OpenCode2AdapterV2Driver],
    configMap: {
      [OPENCODE2_REPLAY_INSTANCE_ID]: {
        driver: OPENCODE2_DRIVER_KIND,
        config: {
          serverUrl: "replay://opencode2",
          serverPassword: "replay-password",
        },
      },
    },
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        makeOpenCode2ReplayRuntimeLayer(transcript),
        serverConfigLayer,
        NodeServices.layer,
        idAllocatorLayer,
        Layer.succeed(ProviderEventLoggers, NoOpProviderEventLoggers),
      ),
    ),
  );
}

function transcriptMetadata(transcript: ProviderReplayTranscript) {
  return {
    driver: transcript.provider,
    protocol: transcript.protocol,
    scenario: transcript.scenario,
  };
}

export const OpenCode2OrchestratorReplayHarness: OrchestratorV2ProviderReplayHarness<
  OpenCode2SdkReplayTranscript,
  OpenCode2OrchestratorReplayHarnessError
> = {
  driver: OPENCODE2_PROVIDER,
  decodeTranscript: (transcript) =>
    decodeOpenCode2SdkReplayTranscript(transcript).pipe(
      Effect.mapError(
        (cause) =>
          new OpenCode2ReplayTranscriptDecodeError({
            ...transcriptMetadata(transcript),
            cause,
          }),
      ),
    ),
  makeProviderAdapterRegistryLayer: makeOpenCode2ProviderAdapterRegistryReplayLayer,
};
