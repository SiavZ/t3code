import { requestFingerprint } from "../memory/requestFingerprint.ts";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import type * as Scope from "effect/Scope";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { ThreadId } from "@t3tools/contracts";
import * as Q from "../../../../packages/contracts/src/qualityRecords.ts";
export interface QualityAuthority {
  readonly threadId: ThreadId;
  readonly source: "agent-reported" | "user-reported";
}
export class QualityRecords extends Context.Service<
  QualityRecords,
  {
    /** Acquire before reading the snapshot. Revisions are invalidations, not retained record payloads. */
    readonly subscribeChanges: Effect.Effect<
      PubSub.Subscription<Q.QualityRecordChange>,
      never,
      Scope.Scope
    >;
    readonly subscribe: (
      input: Q.QualityReadInput,
      authority: QualityAuthority,
    ) => Stream.Stream<Q.QualityRecordChange, Q.QualityRecordsError>;
    readonly streamChanges: Stream.Stream<Q.QualityRecordChange>;
    readonly update: (
      input: Q.QualityUpdateInput,
      authority: QualityAuthority,
    ) => Effect.Effect<Q.QualityRecord, Q.QualityRecordsError>;
    readonly get: (
      input: Q.QualityReadInput,
      authority: QualityAuthority,
    ) => Effect.Effect<Q.QualityRecord | null, Q.QualityRecordsError>;
    readonly list: (
      input: Q.QualityReadInput,
      authority: QualityAuthority,
    ) => Effect.Effect<Q.QualityRecord | null, Q.QualityRecordsError>;
  }
>()("t3/orchestration/QualityRecords") {}
const isQualityError = Schema.is(Q.QualityRecordsError);
const qualityJson = Schema.fromJsonString(Q.QualityRecord);
const encodeQuality = Schema.encodeEffect(qualityJson);
const error = (code: Q.QualityRecordsError["code"], detail: string) =>
  new Q.QualityRecordsError({ code, detail });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const changes = yield* Effect.acquireRelease(
    PubSub.sliding<Q.QualityRecordChange>(128),
    PubSub.shutdown,
  );
  const guard = <A, E>(effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.mapError((e) =>
        isQualityError(e) ? e : error("storage", "Quality record storage operation failed."),
      ),
    );
  const decode = <A>(schema: Schema.Codec<A, unknown, never, never>, input: unknown) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError(() => error("invalid", "Invalid quality record input.")),
    );
  const get = (raw: Q.QualityReadInput, a: QualityAuthority) =>
    guard(
      Effect.gen(function* () {
        const input = yield* decode(Q.QualityReadInput, raw);
        if (input.threadId !== a.threadId)
          return yield* error(
            "forbidden",
            "Only the authorized thread's quality record is accessible.",
          );
        const rows = yield* sql<{
          json: string;
        }>`SELECT record_json AS json FROM quality_records WHERE thread_id=${input.threadId}`;
        return rows[0] ? yield* decode(qualityJson, rows[0].json) : null;
      }),
    );
  const update = (raw: Q.QualityUpdateInput, a: QualityAuthority) =>
    guard(
      sql
        .withTransaction(
          Effect.gen(function* () {
            const input = yield* decode(Q.QualityUpdateInput, raw);
            const previous = yield* get(input, a);
            const fingerprint = requestFingerprint(
              `${a.threadId.length}:${a.threadId}:${a.source}`,
              "update",
              input,
            );
            const receipt = yield* sql<{
              revision: number;
              fingerprint: string;
            }>`SELECT revision,request_fingerprint AS fingerprint FROM quality_operations WHERE thread_id=${input.threadId} AND operation_id=${input.operationId}`;
            if (receipt[0]) {
              if (receipt[0].fingerprint !== fingerprint)
                return yield* error("conflict", "Operation identity was already used.");
              if (!previous) return yield* error("notFound", "Quality record no longer exists.");
              return { record: previous, changed: false };
            }
            if ((previous?.revision ?? 0) !== input.expectedRevision)
              return yield* error("conflict", "Quality record revision changed.");
            const revision = input.expectedRevision + 1;
            const now = DateTime.formatIso(yield* DateTime.now);
            const ids = new Set<string>();
            for (const todo of input.todos ?? []) {
              if (ids.has(todo.id))
                return yield* error("invalid", "Todo identities must be unique.");
              ids.add(todo.id);
              if (todo.status === "completed" && !todo.completionConfidence)
                return yield* error(
                  "invalid",
                  "Completed todos require reported completion confidence.",
                );
            }
            const todos = input.todos
              ? input.todos.map((todo) => {
                  const old = previous?.todos.find((t) => t.id === todo.id);
                  const history = old?.confidenceHistory ?? [];
                  return {
                    ...todo,
                    confidenceHistory:
                      old?.confidence === todo.confidence
                        ? history
                        : [
                            ...history,
                            {
                              confidence: todo.confidence,
                              revision,
                              changedAt: now,
                              source: a.source,
                            },
                          ].slice(-64),
                  };
                })
              : (previous?.todos ?? []);
            const goals = new Map((previous?.goals ?? []).map((g) => [g.group, g]));
            for (const group of input.removeGroups ?? []) goals.delete(group);
            for (const goal of input.goals ?? []) goals.set(goal.group, goal);
            const record = yield* decode(Q.QualityRecord, {
              threadId: input.threadId,
              revision,
              intention: input.intention ?? previous?.intention ?? null,
              understanding: input.understanding ?? previous?.understanding ?? null,
              todos,
              goals: [...goals.values()],
              source: a.source,
              independentlyVerified: false,
              updatedAt: now,
            });
            const json = yield* encodeQuality(record);
            yield* sql`INSERT INTO quality_records VALUES(${input.threadId},${revision},${json}) ON CONFLICT(thread_id) DO UPDATE SET revision=excluded.revision,record_json=excluded.record_json`;
            yield* sql`INSERT INTO quality_operations VALUES(${input.threadId},${input.operationId},${revision},${fingerprint})`;
            return { record, changed: true };
          }),
        )
        .pipe(
          Effect.flatMap(({ record, changed }) =>
            changed
              ? PubSub.publish(changes, {
                  threadId: record.threadId,
                  revision: record.revision,
                }).pipe(Effect.as(record))
              : Effect.succeed(record),
          ),
          Effect.uninterruptible,
        ),
    );
  const subscribe = (raw: Q.QualityReadInput, a: QualityAuthority) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const input = yield* decode(Q.QualityReadInput, raw);
        if (input.threadId !== a.threadId)
          return yield* error(
            "forbidden",
            "Only the authorized thread's quality record is accessible.",
          );
        const subscription = yield* PubSub.subscribe(changes);
        const snapshot = yield* get(input, a);
        let revision = snapshot?.revision ?? 0;
        const initial = { threadId: input.threadId, revision };
        return Stream.concat(
          Stream.make(initial),
          Stream.fromSubscription(subscription).pipe(
            Stream.filter((change) => {
              if (change.threadId !== input.threadId || change.revision <= revision) return false;
              revision = change.revision;
              return true;
            }),
          ),
        );
      }),
    );
  return QualityRecords.of({
    subscribe,
    get,
    list: get,
    update,
    streamChanges: Stream.fromPubSub(changes),
    subscribeChanges: PubSub.subscribe(changes),
  });
});
export const layer = Layer.effect(QualityRecords, make);
