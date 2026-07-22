import { describe, expect, it } from "vite-plus/test";

import { decideThreadResume, THREAD_RESUME_MAX_REPLAY_EVENTS } from "./ThreadStream.ts";

describe("decideThreadResume", () => {
  it("replays when the gap is zero", () => {
    expect(decideThreadResume({ afterSequence: 10, highWater: 10, replayEventCount: 0 })).toEqual({
      mode: "replay",
      afterSequence: 10,
      throughSequence: 10,
    });
  });

  it("replays when the event count is within the bound", () => {
    expect(
      decideThreadResume({
        afterSequence: 10,
        highWater: 20_000,
        replayEventCount: THREAD_RESUME_MAX_REPLAY_EVENTS,
      }),
    ).toEqual({
      mode: "replay",
      afterSequence: 10,
      throughSequence: 20_000,
    });
  });

  it("falls back to a snapshot when the event count exceeds the bound", () => {
    expect(
      decideThreadResume({
        afterSequence: 10,
        highWater: 20_000,
        replayEventCount: THREAD_RESUME_MAX_REPLAY_EVENTS + 1,
      }),
    ).toEqual({ mode: "snapshot" });
  });

  it("falls back to a snapshot when the client cursor is ahead of the store", () => {
    expect(decideThreadResume({ afterSequence: 50, highWater: 40, replayEventCount: 0 })).toEqual({
      mode: "snapshot",
    });
  });
});
