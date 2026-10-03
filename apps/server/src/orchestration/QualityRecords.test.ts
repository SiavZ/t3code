import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Layer from "effect/Layer";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ThreadId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/058_QualityRecords.ts";
import * as Quality from "./QualityRecords.ts";
const services = Quality.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const threadId = ThreadId.make("quality-thread");
const authority = { threadId, source: "agent-reported" as const };
const todo = {
  id: "todo",
  content: "Run real tests",
  group: "tests",
  status: "pending" as const,
  priority: "high" as const,
  confidence: "plausible" as const,
};
it.layer(services)("QualityRecords", (it) => {
  it.effect(
    "keeps agent claims distinct from verification and appends history only on changes",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const quality = yield* Quality.QualityRecords;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM quality_records`;
        yield* sql`DELETE FROM quality_operations`;
        const initial = yield* quality.update(
          { threadId, operationId: "one", expectedRevision: 0, todos: [todo] },
          authority,
        );
        assert.equal(initial.independentlyVerified, false);
        assert.equal(initial.todos[0]?.confidenceHistory.length, 1);
        const claimed = yield* quality.update(
          {
            threadId,
            operationId: "two",
            expectedRevision: 1,
            todos: [
              {
                ...todo,
                status: "completed",
                confidence: "verified",
                completionConfidence: "verified",
              },
            ],
          },
          authority,
        );
        assert.equal(claimed.independentlyVerified, false);
        assert.equal(claimed.todos[0]?.confidenceHistory.length, 2);
        const retry = yield* quality.update(
          {
            threadId,
            operationId: "two",
            expectedRevision: 1,
            todos: [
              {
                ...todo,
                status: "completed",
                confidence: "verified",
                completionConfidence: "verified",
              },
            ],
          },
          authority,
        );
        assert.equal(retry.revision, 2);
        assert.equal(retry.todos.length, 1);
        assert.equal(
          (yield* quality
            .update({ threadId, operationId: "two", expectedRevision: 1, todos: [] }, authority)
            .pipe(Effect.flip)).code,
          "conflict",
        );
        assert.equal(
          (yield* quality
            .update(
              { threadId, operationId: "one", expectedRevision: 0, todos: [todo] },
              { ...authority, source: "user-reported" },
            )
            .pipe(Effect.flip)).code,
          "conflict",
        );
        const reopened = yield* quality.update(
          {
            threadId,
            operationId: "three",
            expectedRevision: 2,
            todos: [{ ...todo, confidence: "verified" }],
          },
          authority,
        );
        assert.equal(reopened.todos[0]?.confidenceHistory.length, 2);
        assert.equal(reopened.todos[0]?.status, "pending");
      }),
  );
  it.effect("requires completion claims and rejects cross-thread writes and stale revisions", () =>
    Effect.gen(function* () {
      yield* migrate;
      const quality = yield* Quality.QualityRecords;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM quality_records`;
      yield* sql`DELETE FROM quality_operations`;
      assert.equal(
        (yield* quality
          .update(
            {
              threadId,
              operationId: "invalid",
              expectedRevision: 0,
              todos: [{ ...todo, status: "completed" }],
            },
            authority,
          )
          .pipe(Effect.flip)).code,
        "invalid",
      );
      assert.equal(
        (yield* quality.get({ threadId: ThreadId.make("foreign") }, authority).pipe(Effect.flip))
          .code,
        "forbidden",
      );
      yield* quality.update(
        { threadId, operationId: "one", expectedRevision: 0, todos: [todo] },
        authority,
      );
      assert.equal(
        (yield* quality
          .update({ threadId, operationId: "stale", expectedRevision: 0 }, authority)
          .pipe(Effect.flip)).code,
        "conflict",
      );
    }),
  );
  it.effect(
    "retains omitted goal groups, removes explicit groups and serializes competing quality updates",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const quality = yield* Quality.QualityRecords;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM quality_records`;
        yield* sql`DELETE FROM quality_operations`;
        const goal = {
          group: "tests",
          autonomy: "requested_only" as const,
          difficulty: "routine" as const,
          deliveryState: "change_made" as const,
          feedbackLoop: "Run focused SQLite tests",
          closedFeedbackLoop: "usable" as const,
          feedbackLoopCoverage: "main_paths" as const,
          feedbackLoopRelevance: "representative" as const,
          feedbackLoopTraceability: "complete" as const,
          iterationMaturity: "exploring" as const,
          stoppingEvidence: null,
        };
        yield* quality.update(
          {
            threadId,
            operationId: "goals",
            expectedRevision: 0,
            goals: [goal, { ...goal, group: "integration" }],
          },
          authority,
        );
        const partial = yield* quality.update(
          {
            threadId,
            operationId: "partial-goals",
            expectedRevision: 1,
            goals: [{ ...goal, iterationMaturity: "outcome_reached" }],
          },
          authority,
        );
        assert.equal(partial.goals.length, 2);
        const removed = yield* quality.update(
          {
            threadId,
            operationId: "remove-goal",
            expectedRevision: 2,
            removeGroups: ["integration"],
          },
          authority,
        );
        assert.equal(removed.goals.length, 1);
        const outcomes = yield* Effect.all(
          [
            quality
              .update(
                { threadId, operationId: "cas-a", expectedRevision: 3, todos: [todo] },
                authority,
              )
              .pipe(Effect.result),
            quality
              .update(
                { threadId, operationId: "cas-b", expectedRevision: 3, todos: [todo] },
                authority,
              )
              .pipe(Effect.result),
          ],
          { concurrency: 2 },
        );
        assert.equal(outcomes.filter((outcome) => outcome._tag === "Success").length, 1);
        const stored = yield* quality.get({ threadId }, authority);
        assert.equal(stored?.todos[0]?.confidenceHistory.length, 1);
        assert.equal(stored?.independentlyVerified, false);
      }),
  );
});
