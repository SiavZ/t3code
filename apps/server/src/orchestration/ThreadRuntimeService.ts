import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as NodeCrypto from "node:crypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { CommandId, ThreadId, RuntimeThreadMetadata } from "@t3tools/contracts";
import * as R from "../../../../packages/contracts/src/runtimeOperations.ts";
import * as Engine from "./Services/OrchestrationEngine.ts";
import * as Snapshots from "./Services/ProjectionSnapshotQuery.ts";
import { buildRuntimeTranscriptSeed } from "./runtimeHandoffDecision.ts";
export class ThreadRuntimeService extends Context.Service<
  ThreadRuntimeService,
  {
    readonly handoff: (
      input: R.RuntimeHandoffInput,
    ) => Effect.Effect<R.RuntimeOperationReceipt, R.RuntimeOperationError>;
    readonly fork: (
      input: R.RuntimeForkInput,
    ) => Effect.Effect<R.RuntimeOperationReceipt, R.RuntimeOperationError>;
    readonly metadata: (
      threadId: ThreadId,
    ) => Effect.Effect<typeof RuntimeThreadMetadata.Type, R.RuntimeOperationError>;
    readonly recordHandoffStatus: (
      threadId: ThreadId,
      operationId: string,
      status: "completed" | "failed",
      detail: string,
    ) => Effect.Effect<void, R.RuntimeOperationError>;
    readonly prepareContext: (input: {
      threadId: ThreadId;
      turnKey: string;
      messageText: string;
    }) => Effect.Effect<{ messageText: string; epochId: string | null }, R.RuntimeOperationError>;
    readonly acknowledgeSeed: (input: {
      threadId: ThreadId;
      turnKey: string;
      epochId: string;
    }) => Effect.Effect<void, R.RuntimeOperationError>;
    readonly recoverPending: () => Effect.Effect<void, R.RuntimeOperationError>;
  }
>()("t3/orchestration/ThreadRuntimeService") {}
const decodeReceipt = Schema.decodeUnknownEffect(Schema.fromJsonString(R.RuntimeOperationReceipt));
const decodeMetadata = Schema.decodeUnknownEffect(Schema.fromJsonString(RuntimeThreadMetadata));
const isRuntimeError = Schema.is(R.RuntimeOperationError);
const decodeHandoffInput = Schema.decodeUnknownEffect(R.RuntimeHandoffInput);
const decodeForkInput = Schema.decodeUnknownEffect(R.RuntimeForkInput);
const operationTimestamp = (expectedUpdatedAt: string) =>
  DateTime.now.pipe(
    Effect.map((now) =>
      new Date(
        Math.max(Date.parse(expectedUpdatedAt) + 1, DateTime.toEpochMillis(now)),
      ).toISOString(),
    ),
  );
const make = Effect.gen(function* () {
  const engine = yield* Engine.OrchestrationEngineService;
  const snapshots = yield* Snapshots.ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  const previous = (operationId: string, threadId: string, input: unknown) =>
    Effect.gen(function* () {
      const rows = yield* sql<{
        input_json: string;
        receipt_json: string;
      }>`SELECT input_json,receipt_json FROM runtime_operations WHERE operation_id=${`${threadId}:${operationId}`}`;
      if (!rows[0]) return null;
      if (rows[0].input_json !== JSON.stringify(input))
        return yield* new R.RuntimeOperationError({
          code: "conflict",
          detail: "Runtime operation identity was reused with different input.",
        });
      return yield* decodeReceipt(rows[0].receipt_json);
    });
  const save = (input: unknown, receipt: R.RuntimeOperationReceipt, keyThreadId: string) =>
    sql`INSERT OR IGNORE INTO runtime_operations VALUES(${`${keyThreadId}:${receipt.operationId}`},${receipt.threadId},${receipt.kind},${JSON.stringify(input)},${JSON.stringify(receipt)})`;
  const guard = <A, E>(effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.tapError((e) => Effect.logDebug("Runtime operation admission failed", { cause: e })),
      Effect.mapError((e) =>
        isRuntimeError(e)
          ? e
          : new R.RuntimeOperationError({
              code: "conflict",
              detail: "Runtime operation could not be admitted.",
            }),
      ),
    );
  const handoff = (raw: R.RuntimeHandoffInput) =>
    guard(
      Effect.gen(function* () {
        const input = yield* decodeHandoffInput(raw);
        const existing = yield* previous(input.operationId, input.threadId, input);
        if (existing) return existing;
        const thread = yield* snapshots.getThreadDetailById(input.threadId);
        if (Option.isNone(thread))
          return yield* new R.RuntimeOperationError({
            code: "notFound",
            detail: "Thread was not found.",
          });
        const createdAt = yield* operationTimestamp(input.expectedUpdatedAt);
        const epochId = `handoff:${NodeCrypto.createHash("sha256")
          .update(JSON.stringify([input.threadId, input.operationId]))
          .digest("hex")}`;
        const receipt: R.RuntimeOperationReceipt = {
          operationId: input.operationId,
          threadId: input.threadId,
          kind: "handoff",
          status: "accepted",
          epochId,
          detail:
            "Explicit visible transcript handoff accepted. Native hidden state is not transferred.",
          createdAt,
        };
        yield* save(input, receipt, input.threadId);
        yield* engine
          .dispatch({
            type: "thread.runtime.handoff",
            commandId: CommandId.make(epochId),
            ...input,
            epochId,
            seed: buildRuntimeTranscriptSeed(thread.value.messages),
            createdAt,
          })
          .pipe(
            Effect.tapError(
              () =>
                sql`DELETE FROM runtime_operations WHERE operation_id=${`${input.threadId}:${input.operationId}`} AND receipt_json=${JSON.stringify(receipt)}`,
            ),
          );
        return receipt;
      }),
    );
  const fork = (raw: R.RuntimeForkInput) =>
    guard(
      Effect.gen(function* () {
        const input = yield* decodeForkInput(raw);
        const existing = yield* previous(input.operationId, input.sourceThreadId, input);
        if (existing) return existing;
        const source = yield* snapshots.getThreadDetailById(input.sourceThreadId);
        if (Option.isNone(source))
          return yield* new R.RuntimeOperationError({
            code: "notFound",
            detail: "Fork source was not found.",
          });
        const createdAt = yield* operationTimestamp(input.expectedUpdatedAt);
        const threadId = ThreadId.make(
          `fork:${NodeCrypto.createHash("sha256")
            .update(JSON.stringify([input.sourceThreadId, input.operationId]))
            .digest("hex")}`,
        );
        yield* engine.dispatch({
          type: "thread.runtime.fork",
          commandId: CommandId.make(`fork:${input.sourceThreadId}:${input.operationId}`),
          sourceThreadId: input.sourceThreadId,
          expectedUpdatedAt: input.expectedUpdatedAt,
          operationId: input.operationId,
          threadId,
          throughMessageId: input.throughMessageId,
          title: input.title ?? `${source.value.title} (fork)`,
          modelSelection: input.targetModelSelection ?? source.value.modelSelection,
          createdAt,
        });
        const receipt: R.RuntimeOperationReceipt = {
          operationId: input.operationId,
          threadId,
          kind: "fork",
          status: "completed",
          epochId: null,
          detail:
            "Independent visible conversation fork created. Approvals, workers and native cursors were not copied.",
          createdAt,
        };
        yield* save(input, receipt, input.sourceThreadId);
        return receipt;
      }),
    );
  const metadata = (threadId: ThreadId) =>
    guard(
      Effect.gen(function* () {
        const rows = yield* sql<{
          metadata_json: string;
        }>`SELECT metadata_json FROM projection_runtime_metadata WHERE thread_id=${threadId}`;
        return rows[0] ? yield* decodeMetadata(rows[0].metadata_json) : {};
      }),
    );
  const recordHandoffStatus = (
    threadId: ThreadId,
    operationId: string,
    status: "completed" | "failed",
    detail: string,
  ) =>
    guard(
      Effect.gen(function* () {
        const rows = yield* sql<{
          receipt_json: string;
        }>`SELECT receipt_json FROM runtime_operations WHERE operation_id=${`${threadId}:${operationId}`}`;
        if (!rows[0]) return;
        const receipt = yield* decodeReceipt(rows[0].receipt_json);
        yield* sql`UPDATE runtime_operations SET receipt_json=${JSON.stringify({ ...receipt, status, detail })} WHERE operation_id=${`${threadId}:${operationId}`}`;
      }),
    );
  const prepareContext = (input: { threadId: ThreadId; turnKey: string; messageText: string }) =>
    guard(
      Effect.gen(function* () {
        const state = yield* metadata(input.threadId);
        const epochId = state.runtimeEpochId;
        if (!epochId || (!state.forkProvenance && state.runtimeHandoff?.status !== "committed"))
          return { messageText: input.messageText, epochId: null };
        const delivered =
          yield* sql`SELECT 1 FROM runtime_seed_deliveries WHERE thread_id=${input.threadId} AND epoch_id=${epochId}`;
        if (delivered.length) return { messageText: input.messageText, epochId: null };
        yield* sql`INSERT INTO runtime_seed_reservations VALUES(${input.threadId},${epochId},${input.turnKey}) ON CONFLICT(thread_id,epoch_id) DO UPDATE SET turn_key=excluded.turn_key`;
        const forkThread =
          state.runtimeHandoff?.status !== "committed"
            ? yield* snapshots.getThreadDetailById(input.threadId)
            : Option.none();
        const seed =
          state.runtimeHandoff?.status === "committed"
            ? state.runtimeHandoff.seed
            : Option.isSome(forkThread)
              ? buildRuntimeTranscriptSeed(
                  forkThread.value.messages.filter((message) =>
                    message.id.startsWith("import:fork:"),
                  ),
                )
              : null;
        if (!seed)
          return yield* new R.RuntimeOperationError({
            code: "notFound",
            detail: "Fork conversation could not be read for its fresh runtime.",
          });
        return {
          messageText: `${seed.text}\nCurrent user request:\n${input.messageText}`,
          epochId,
        };
      }),
    );
  const acknowledgeSeed = (input: { threadId: ThreadId; turnKey: string; epochId: string }) =>
    guard(
      sql.withTransaction(
        Effect.gen(function* () {
          const state = yield* metadata(input.threadId);
          if (state.runtimeEpochId !== input.epochId)
            return yield* new R.RuntimeOperationError({
              code: "conflict",
              detail: "Runtime epoch changed before transcript seed acknowledgement.",
            });
          const deliveries = yield* sql<{
            turn_key: string;
          }>`SELECT turn_key FROM runtime_seed_deliveries WHERE thread_id=${input.threadId} AND epoch_id=${input.epochId}`;
          if (deliveries[0]?.turn_key === input.turnKey) return;
          if (deliveries.length)
            return yield* new R.RuntimeOperationError({
              code: "conflict",
              detail: "Transcript seed was already acknowledged by a different native turn.",
            });
          const reservations = yield* sql<{
            turn_key: string;
          }>`SELECT turn_key FROM runtime_seed_reservations WHERE thread_id=${input.threadId} AND epoch_id=${input.epochId}`;
          if (reservations[0]?.turn_key !== input.turnKey)
            return yield* new R.RuntimeOperationError({
              code: "conflict",
              detail: "Transcript seed reservation changed before native send acknowledgement.",
            });
          yield* sql`INSERT OR IGNORE INTO runtime_seed_deliveries VALUES(${input.threadId},${input.epochId},${input.turnKey})`;
          yield* sql`DELETE FROM runtime_seed_reservations WHERE thread_id=${input.threadId} AND epoch_id=${input.epochId}`;
        }),
      ),
    );
  const recoverPending = () =>
    guard(
      Effect.gen(function* () {
        const rows = yield* sql<{
          thread_id: string;
          metadata_json: string;
        }>`SELECT thread_id,metadata_json FROM projection_runtime_metadata WHERE json_extract(metadata_json,'$.runtimeHandoff.status')='pending'`;
        for (const row of rows) {
          const state = yield* decodeMetadata(row.metadata_json);
          if (!state.runtimeHandoff) continue;
          const threadId = ThreadId.make(row.thread_id);
          const handoff = state.runtimeHandoff;
          yield* engine.dispatch({
            type: "thread.runtime.handoff.fail",
            commandId: CommandId.make(`runtime-recovery:${handoff.epochId}`),
            threadId,
            expectedEpochId: handoff.epochId,
            createdAt: handoff.requestedAt,
          });
          yield* recordHandoffStatus(
            threadId,
            handoff.operationId,
            "failed",
            "Server restarted before native stop acknowledgement. No external process was taken over. Retry explicitly with a new operation.",
          );
        }
        const interrupted = yield* sql<{
          operation_id: string;
          receipt_json: string;
        }>`SELECT operation_id,receipt_json FROM runtime_operations WHERE kind='handoff' AND json_extract(receipt_json,'$.status')='accepted'`;
        for (const row of interrupted) {
          const receipt = yield* decodeReceipt(row.receipt_json);
          yield* sql`UPDATE runtime_operations SET receipt_json=${JSON.stringify({ ...receipt, status: "failed", detail: "Server restarted before the runtime operation completed. No external process was taken over. Retry explicitly with a new operation." })} WHERE operation_id=${row.operation_id} AND receipt_json=${row.receipt_json}`;
        }
      }),
    );
  return ThreadRuntimeService.of({
    handoff,
    fork,
    metadata,
    recordHandoffStatus,
    prepareContext,
    acknowledgeSeed,
    recoverPending,
  });
});
export const layer = Layer.effect(ThreadRuntimeService, make);
