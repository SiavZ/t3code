import { CommandId, CoordinationError, CoordinationPlan, type ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Engine from "./Services/OrchestrationEngine.ts";
import * as Query from "./Services/ProjectionSnapshotQuery.ts";
import { latestAttempt, readyNodes } from "./coordinationGraph.ts";

export class CoordinationReactor extends Context.Service<
  CoordinationReactor,
  {
    readonly drain: (
      rootThreadId?: ThreadId,
      planId?: string,
    ) => Effect.Effect<void, CoordinationError>;
    readonly recover: Effect.Effect<void, CoordinationError>;
    readonly start: Effect.Effect<void, never, Scope.Scope>;
  }
>()("t3/orchestration/CoordinationReactor") {}

const decodePlan = Schema.decodeUnknownEffect(CoordinationPlan);
const failure = () =>
  new CoordinationError({ code: "invalid", detail: "Unable to drain coordination plans." });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* Engine.OrchestrationEngineService;
  const query = yield* Query.ProjectionSnapshotQuery;
  const mutex = yield* Semaphore.make(1);
  let drainCursor = "";
  const drainUnlocked = Effect.fn("CoordinationReactor.drain")(function* (
    rootThreadId?: ThreadId,
    planId?: string,
  ) {
    // No background polling. Each wake is bounded and fair across plan IDs.
    const rows = yield* (
      planId !== undefined
        ? sql<{
            document_json: string;
          }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id = ${planId}`
        : rootThreadId === undefined
          ? sql<{
              document_json: string;
            }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id > ${drainCursor} ORDER BY plan_id LIMIT 128`
          : sql<{
              document_json: string;
            }>`SELECT document_json FROM projection_coordination_plans WHERE root_thread_id = ${rootThreadId} ORDER BY plan_id LIMIT 128`
    ).pipe(Effect.mapError(failure));
    for (const row of rows) {
      const raw = yield* Effect.try({ try: () => JSON.parse(row.document_json), catch: failure });
      let plan = yield* decodePlan(raw).pipe(Effect.mapError(failure));
      if (rootThreadId === undefined) drainCursor = plan.id;
      for (const node of plan.nodes) {
        const attempt = latestAttempt(node);
        if (attempt?.status !== "accepted") continue;
        const state = yield* query
          .getWorkerState(attempt.workerThreadId)
          .pipe(Effect.mapError(failure));
        const orphan =
          Option.isNone(state) ||
          (state.value.pendingMessageId === null &&
            state.value.thread.session?.activeTurnId == null &&
            state.value.thread.session?.status !== "starting" &&
            (state.value.thread.session?.status === "stopped" ||
              state.value.thread.session?.status === "error" ||
              state.value.thread.session?.status === "interrupted" ||
              !state.value.thread.latestTurn));
        if (orphan) {
          const createdAt = DateTime.formatIso(yield* DateTime.now);
          const abandoned = yield* engine
            .dispatch({
              type: "coordination.plan.abandon",
              commandId: CommandId.make(
                `plan-abandon:${plan.id}:${node.id}:${attempt.number}:${plan.revision}`,
              ),
              threadId: plan.rootThreadId,
              planId: plan.id,
              expectedRevision: plan.revision,
              nodeId: node.id,
              expectedMessageId: attempt.dispatchMessageId,
              createdAt,
            })
            .pipe(Effect.result);
          if (abandoned._tag === "Success") {
            const updated = yield* sql<{
              document_json: string;
            }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id = ${plan.id}`.pipe(
              Effect.mapError(failure),
            );
            plan = yield* decodePlan(JSON.parse(updated[0]!.document_json)).pipe(
              Effect.mapError(failure),
            );
          }
          continue;
        }
        if (Option.isNone(state)) continue;
        const turn = state.value.thread.latestTurn;
        if (
          !turn ||
          turn.state === "running" ||
          state.value.pendingMessageId !== null ||
          state.value.thread.backgroundLiveness != null ||
          state.value.thread.hasPendingApprovals ||
          state.value.thread.hasPendingUserInput
        )
          continue;
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const outcome =
          turn.state === "completed"
            ? "completed"
            : turn.state === "interrupted"
              ? "interrupted"
              : "failed";
        const result = yield* engine
          .dispatch({
            type: "coordination.plan.settle",
            commandId: CommandId.make(
              `plan-settle:${plan.id}:${node.id}:${attempt.number}:${plan.revision}`,
            ),
            threadId: plan.rootThreadId,
            planId: plan.id,
            expectedRevision: plan.revision,
            nodeId: node.id,
            turnId: turn.turnId,
            outcome,
            createdAt,
          })
          .pipe(Effect.result);
        if (result._tag === "Success") {
          const updated = yield* sql<{
            document_json: string;
          }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id = ${plan.id}`.pipe(
            Effect.mapError(failure),
          );
          plan = yield* decodePlan(JSON.parse(updated[0]!.document_json)).pipe(
            Effect.mapError(failure),
          );
        }
      }
      for (const node of readyNodes(plan)) {
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        const result = yield* engine
          .dispatch({
            type: "coordination.plan.dispatch",
            commandId: CommandId.make(
              `plan-dispatch:${plan.id}:${node.id}:${node.attempts.length + 1}:${plan.revision}`,
            ),
            threadId: plan.rootThreadId,
            planId: plan.id,
            expectedRevision: plan.revision,
            nodeId: node.id,
            createdAt,
          })
          .pipe(Effect.result);
        if (result._tag === "Failure") break;
        const updated = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id = ${plan.id}`.pipe(
          Effect.mapError(failure),
        );
        plan = yield* decodePlan(JSON.parse(updated[0]!.document_json)).pipe(
          Effect.mapError(failure),
        );
      }
    }
    if (rootThreadId === undefined && rows.length < 128) drainCursor = "";
  });
  const drain = (rootThreadId?: ThreadId, planId?: string) =>
    drainUnlocked(rootThreadId, planId).pipe(mutex.withPermits(1));
  const recover = Effect.gen(function* () {
    let cursor = "";
    while (true) {
      const rows = yield* sql<{
        document_json: string;
      }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id > ${cursor} ORDER BY plan_id LIMIT 128`.pipe(
        Effect.mapError(failure),
      );
      for (const row of rows) {
        const plan = yield* decodePlan(JSON.parse(row.document_json)).pipe(
          Effect.mapError(failure),
        );
        cursor = plan.id;
        const root = yield* query.getWorkerState(plan.rootThreadId).pipe(Effect.mapError(failure));
        if (Option.isNone(root)) continue;
        const createdAt = DateTime.formatIso(yield* DateTime.now);
        yield* engine
          .dispatch({
            type: "coordination.plan.recover",
            commandId: CommandId.make(`plan-recover:${plan.id}:${plan.revision}`),
            threadId: plan.rootThreadId,
            planId: plan.id,
            expectedRevision: plan.revision,
            createdAt,
          })
          .pipe(Effect.mapError(failure));
      }
      if (rows.length < 128) break;
    }
  }).pipe(mutex.withPermits(1));
  const start = Effect.gen(function* () {
    const capacityWakes = yield* Queue.unbounded<string>();
    let scanQueued = false;
    yield* Effect.gen(function* () {
      while (true) {
        const cursor = yield* Queue.take(capacityWakes);
        const rows = yield* sql<{
          plan_id: string;
        }>`SELECT plan_id FROM projection_coordination_plans WHERE plan_id > ${cursor} AND json_extract(document_json, '$.paused') = 0 ORDER BY plan_id LIMIT 128`.pipe(
          Effect.mapError(failure),
        );
        for (const row of rows) yield* drain(undefined, row.plan_id);
        if (rows.length === 128) yield* Queue.offer(capacityWakes, rows.at(-1)!.plan_id);
        else scanQueued = false;
      }
    }).pipe(
      Effect.catch((cause) => Effect.logWarning("coordination capacity wake failed", { cause })),
      Effect.forkScoped,
    );
    yield* engine.streamDomainEvents.pipe(
      Stream.filter(
        (event) =>
          event.type === "coordination.plan.updated" ||
          event.type === "thread.session-set" ||
          event.type === "thread.turn-diff-completed" ||
          event.type === "thread.deleted" ||
          event.type === "thread.activity-appended",
      ),
      Stream.runForEach((event) =>
        Effect.gen(function* () {
          if (event.type === "coordination.plan.updated") {
            yield* drain(event.payload.plan.rootThreadId, event.payload.plan.id);
            return;
          }
          const affected = yield* sql<{
            plan_id: string;
          }>`SELECT DISTINCT p.plan_id FROM projection_coordination_plans p, json_each(p.document_json, '$.nodes') n, json_each(n.value, '$.attempts') a WHERE json_extract(a.value, '$.workerThreadId') = ${event.aggregateId} AND json_extract(a.value, '$.status') = 'accepted' LIMIT 128`.pipe(
            Effect.mapError(failure),
          );
          for (const row of affected) yield* drain(undefined, row.plan_id);
          // Capacity can be released by an ordinary worker, including one owned by
          // another root. Continue scans in bounded queue pages rather than waiting
          // for an unrelated assignment event or materializing every plan.
          if (!scanQueued) {
            scanQueued = true;
            yield* Queue.offer(capacityWakes, "");
          }
        }).pipe(Effect.catch((cause) => Effect.logWarning("coordination drain failed", { cause }))),
      ),
      Effect.forkScoped,
      Effect.asVoid,
    );
  });
  return CoordinationReactor.of({ drain, recover, start });
});
export const layer = Layer.effect(CoordinationReactor, make);
