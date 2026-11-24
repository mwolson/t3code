import * as Effect from "effect/Effect";
import { describe, it } from "@effect/vitest";
import { heldCompletionScenario } from "../../../../apps/server/src/relay/HeldCompletionTestSupport.ts";
import { assertSignedCompletion } from "./HeldCompletion.ts";

describe.skipIf(!process.env.T3CODE_PERSONAL_RELAY_TEST_DATABASE_URL)(
  "server held-completion ledger integration",
  () => {
    it.effect("delivers a long-held completion exactly once and ignores stale replay", () =>
      heldCompletionScenario("monitor", (input) =>
        assertSignedCompletion(input).pipe(Effect.orDie),
      ),
    );
  },
);
