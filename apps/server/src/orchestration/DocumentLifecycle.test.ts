import { describe, expect, it } from "vite-plus/test";
import * as Schema from "effect/Schema";
import { OrchestrationEvent } from "@t3tools/contracts";
import { documentLifecycleReason } from "./DocumentLifecycle.ts";
const timestamp = "2026-10-03T05:00:00.000Z";
const base = {
  sequence: 1,
  eventId: "event",
  aggregateKind: "thread",
  aggregateId: "thread",
  occurredAt: timestamp,
  commandId: null,
  causationEventId: null,
  correlationId: null,
  metadata: {},
};
const sessionEvent = (status: string) =>
  Schema.decodeUnknownSync(OrchestrationEvent)({
    ...base,
    type: "thread.session-set",
    payload: {
      threadId: "thread",
      session: {
        threadId: "thread",
        status,
        providerName: null,
        activeTurnId: null,
        lastError: null,
        updatedAt: timestamp,
      },
    },
  });
describe("document lifecycle event classification", () => {
  it.each(["ready", "interrupted", "stopped", "error"])(
    "closes ephemeral documents for terminal session %s",
    (status) => {
      expect(documentLifecycleReason(sessionEvent(status))).toBe("turnEnded");
    },
  );
  it.each(["starting", "running", "idle"])(
    "does not close documents for nonterminal session %s",
    (status) => {
      expect(documentLifecycleReason(sessionEvent(status))).toBeUndefined();
    },
  );
  it("distinguishes deletion from ordinary settlement", () => {
    expect(
      documentLifecycleReason(
        Schema.decodeUnknownSync(OrchestrationEvent)({
          ...base,
          type: "thread.deleted",
          payload: { threadId: "thread", deletedAt: timestamp },
        }),
      ),
    ).toBe("deleted");
    expect(
      documentLifecycleReason(
        Schema.decodeUnknownSync(OrchestrationEvent)({
          ...base,
          type: "thread.settled",
          payload: { threadId: "thread", settledAt: timestamp, updatedAt: timestamp },
        }),
      ),
    ).toBe("turnEnded");
  });
});
