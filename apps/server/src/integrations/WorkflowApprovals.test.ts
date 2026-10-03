import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Approvals from "./WorkflowApprovals.ts";
import * as Attempts from "./WorkflowAttempts.ts";
import { testPersistence } from "./integrationTestSupport.ts";

it.effect("session-bound consumption rejects other humans without burning the owner's grant", () =>
  Effect.gen(function* () {
    const approvals = yield* Approvals.WorkflowApprovals;
    const id = yield* approvals.grant({
      humanSessionId: "owner",
      operation: "doctor.full",
      review: "reviewed-run",
    });
    expect(
      (yield* Effect.flip(
        approvals.consumeForSession(id, "doctor.full", "reviewed-run", "intruder"),
      )).reason,
    ).toBe("required");
    expect(
      (yield* Effect.flip(approvals.consumeForSession(id, "doctor.full", "changed-run", "owner")))
        .reason,
    ).toBe("required");
    yield* approvals.consumeForSession(id, "doctor.full", "reviewed-run", "owner");
    expect(
      (yield* Effect.flip(approvals.consumeForSession(id, "doctor.full", "reviewed-run", "owner")))
        .reason,
    ).toBe("required");
  }).pipe(Effect.provide(testPersistence())),
);

it.effect(
  "consumes exact human approval once and denies changed reviews and revoked sessions",
  () => {
    return Effect.gen(function* () {
      const approvals = yield* Approvals.WorkflowApprovals;
      const id = yield* approvals.grant({
        humanSessionId: "human",
        operation: "gmail.send",
        review: "reviewed-bytes",
      });
      expect(
        (yield* approvals.consume(id, "gmail.send", "changed-bytes").pipe(Effect.result))._tag,
      ).toBe("Failure");
      yield* approvals.consume(id, "gmail.send", "reviewed-bytes");
      expect(
        (yield* approvals.consume(id, "gmail.send", "reviewed-bytes").pipe(Effect.result))._tag,
      ).toBe("Failure");
      const revoked = yield* approvals.grant({
        humanSessionId: "human",
        operation: "image.create",
        review: "prompt",
      });
      yield* approvals.revokeSession("human");
      expect(
        (yield* approvals.consume(revoked, "image.create", "prompt").pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(testPersistence()));
  },
);
it.effect("expired approvals fail and durable attempts block ambiguous retry", () => {
  return Effect.gen(function* () {
    const approvals = yield* Approvals.WorkflowApprovals;
    const sql = yield* SqlClient.SqlClient;
    const id = yield* approvals.grant({
      humanSessionId: "human",
      operation: "build.submit",
      review: "manifest",
    });
    yield* sql`UPDATE integration_approvals SET expires_at = 0 WHERE approval_id = ${id}`;
    expect(
      (yield* approvals.consume(id, "build.submit", "manifest").pipe(Effect.result))._tag,
    ).toBe("Failure");
    const attempts = yield* Attempts.WorkflowAttempts;
    yield* attempts.claim("request", "build.submit", "manifest");
    expect(
      (yield* attempts.claim("request", "build.submit", "manifest").pipe(Effect.result))._tag,
    ).toBe("Failure");
    const rows = yield* sql<{
      state: string;
    }>`SELECT state FROM integration_attempts WHERE operation_id = 'request'`;
    expect(rows).toEqual([{ state: "unknown" }]);
  }).pipe(Effect.provide(testPersistence()));
});
