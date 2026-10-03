import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as PubSub from "effect/PubSub";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ThreadId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/059_QualityRecords.ts";
import * as Quality from "./QualityRecords.ts";
const services = Quality.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
it.layer(services)("Quality changes", (it) => {
  it.effect("publishes committed metadata once, not retry or failed CAS notifications", () =>
    Effect.gen(function* () {
      yield* migrate;
      const quality = yield* Quality.QualityRecords;
      const threadId = ThreadId.make("changes");
      const authority = { threadId, source: "agent-reported" as const };
      const subscription = yield* quality.subscribeChanges;
      assert.equal(yield* quality.get({ threadId }, authority), null);
      const input = {
        threadId,
        operationId: "change",
        expectedRevision: 0,
        intention: "Reported intention",
      };
      yield* quality.update(input, authority);
      assert.deepEqual(yield* PubSub.takeUpTo(subscription, 10), [{ threadId, revision: 1 }]);
      assert.equal((yield* quality.get({ threadId }, authority))?.revision, 1);
      yield* quality.update(input, authority);
      assert.equal(
        (yield* quality
          .update({ ...input, intention: "Changed request" }, authority)
          .pipe(Effect.flip)).code,
        "conflict",
      );
      yield* quality.update({ ...input, operationId: "stale" }, authority).pipe(Effect.flip);
      assert.deepEqual(yield* PubSub.takeUpTo(subscription, 10), []);
    }).pipe(Effect.scoped),
  );
  it.effect(
    "bounds a slow subscription and keeps current state available for resynchronization",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const quality = yield* Quality.QualityRecords;
        const threadId = ThreadId.make("slow-changes");
        const authority = { threadId, source: "agent-reported" as const };
        const subscription = yield* quality.subscribeChanges;
        for (let revision = 0; revision < 140; revision++)
          yield* quality.update(
            { threadId, operationId: `revision-${revision}`, expectedRevision: revision },
            authority,
          );
        const events = yield* PubSub.takeUpTo(subscription, 200);
        assert.equal(events.length, 128);
        assert.equal(events[0]?.revision, 13);
        assert.equal(events.at(-1)?.revision, 140);
        assert.equal((yield* quality.get({ threadId }, authority))?.revision, 140);
      }).pipe(Effect.scoped),
  );
  it.effect(
    "authorized subscription emits current boundary and excludes foreign thread changes",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const quality = yield* Quality.QualityRecords;
        const threadId = ThreadId.make("authorized-stream");
        const authority = { threadId, source: "agent-reported" as const };
        const pull = yield* Stream.toPull(quality.subscribe({ threadId }, authority));
        assert.deepEqual(yield* pull, [{ threadId, revision: 0 }]);
        const foreign = ThreadId.make("foreign-stream");
        yield* quality.update(
          { threadId: foreign, operationId: "foreign", expectedRevision: 0 },
          { threadId: foreign, source: "agent-reported" },
        );
        yield* quality.update(
          { threadId, operationId: "authorized", expectedRevision: 0 },
          authority,
        );
        assert.deepEqual(yield* pull, [{ threadId, revision: 1 }]);
        const denied = yield* quality
          .subscribe({ threadId: foreign }, authority)
          .pipe(Stream.runDrain, Effect.flip);
        assert.equal(denied.code, "forbidden");
      }).pipe(Effect.scoped),
  );
});
