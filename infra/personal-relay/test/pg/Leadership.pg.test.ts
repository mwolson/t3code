// @effect-diagnostics-next-line nodeBuiltinImport:off -- Independent processes are the boundary under test.
import * as NodeChildProcess from "node:child_process";
import * as NodeProcess from "node:process";
import { describe, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import { RelayDb } from "t3code-relay/db";
import { stableStringify } from "@t3tools/shared/relaySigning";
import { services } from "./Harness.ts";

describe.skipIf(!process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL)(
  "owned Postgres fixture",
  () => {
    it.live(
      "leadership: second process exits 3 before listening, handover and connection loss",
      () =>
        Effect.gen(function* () {
          const url = new URL("../../src/maintenance/WorkerLeadership.ts", import.meta.url).href;
          const code = `
    import * as Effect from 'effect/Effect';
    import * as Redacted from 'effect/Redacted';
    import * as Net from 'node:net';
    import { make } from ${stableStringify(url)};
    const parsed = new URL(process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL);
    Effect.runPromise(Effect.scoped(Effect.gen(function* () {
      const leader = yield* make({ host: '127.0.0.1', port: Number(parsed.port), username: 't3relay', database: 't3relay', password: Redacted.make('test-only') });
      const server = Net.createServer();
      yield* Effect.acquireRelease(
        Effect.promise(() => new Promise((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); })),
        () => Effect.promise(() => new Promise((resolve) => server.close(resolve)))
      );
      process.send({ type: 'ready', port: server.address().port });
      yield* leader.guard(Effect.never);
    }))).catch((error) => { process.exitCode = error.exitCode ?? 1; });
  `;

          const children: Array<NodeChildProcess.ChildProcess> = [];
          const start = () => {
            const child = NodeChildProcess.spawn(
              NodeProcess.execPath,
              ["--input-type=module", "-e", code],
              {
                cwd: `${import.meta.dirname}/../..`,
                stdio: ["ignore", "pipe", "pipe", "ipc"],
                env: NodeProcess.env,
              },
            );
            children.push(child);
            let stderr = "";
            child.stderr?.on("data", (chunk) => {
              stderr += String(chunk);
            });
            const exitedPromise = new Promise<number | null>((resolve) =>
              child.once("exit", (code) => resolve(code)),
            );
            const exited = Effect.promise(() => exitedPromise).pipe(Effect.timeout("10 seconds"));
            const ready = Effect.promise(
              () =>
                new Promise<void>((resolve, reject) => {
                  child.once("message", () => resolve());
                  child.once("exit", () => reject(new Error(`Leadership child exited: ${stderr}`)));
                }),
            ).pipe(Effect.timeout("10 seconds"));
            return { child, exited, ready };
          };
          yield* Effect.addFinalizer(() =>
            Effect.promise(async () => {
              const exits: Array<Promise<void>> = [];
              for (const child of children) {
                if (child.exitCode !== null || child.signalCode !== null) continue;
                exits.push(new Promise((resolve) => child.once("exit", () => resolve())));
                child.kill("SIGTERM");
              }
              await Promise.all(exits);
            }),
          );
          const first = start();
          yield* first.ready;
          const second = start();
          expect(yield* second.exited).toBe(3);
          first.child.kill("SIGTERM");
          yield* first.exited;
          const third = start();
          yield* third.ready;
          const sql = (yield* RelayDb).$client;
          const result = yield* sql<{
            terminated: boolean;
          }>`SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity WHERE application_name = 't3-relay-worker-leadership' AND datname = 't3relay' AND pid <> pg_backend_pid()`;
          expect(result.length).toBe(1);
          expect(result[0]?.terminated).toBe(true);
          expect(yield* third.exited).toBe(1);
          const fourth = start();
          yield* fourth.ready;
          fourth.child.kill("SIGTERM");
          yield* fourth.exited;
        }).pipe(Effect.scoped, Effect.provide(services)),
    );
  },
);
