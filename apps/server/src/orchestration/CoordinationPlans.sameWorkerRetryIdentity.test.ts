import {
  CheckpointRef,
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type CoordinationArtifact,
  type CoordinationPlan,
  type ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";
import * as Plans from "./CoordinationPlans.ts";
import * as Store from "./CoordinationPlanStore.ts";
import * as Reactor from "./CoordinationReactor.ts";
import * as Grants from "./UnattendedGrants.ts";

const NOW = "2026-10-03T00:00:00.000Z";
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const core = Layer.mergeAll(
  OrchestrationEngineLive.pipe(
    Layer.provide(OrchestrationProjectionSnapshotQueryLive),
    Layer.provide(OrchestrationProjectionPipelineLive),
  ),
  OrchestrationProjectionSnapshotQueryLive,
).pipe(
  Layer.provideMerge(ThreadBackgroundLiveness.layer),
  Layer.provide(ThreadPlanProgress.layer),
  Layer.provide(OrchestrationEventStoreLive),
  Layer.provide(OrchestrationCommandReceiptRepositoryLive),
  Layer.provide(
    Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
      resolve: () => Effect.succeed(null),
    }),
  ),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-same-worker-retry-" })),
  Layer.provideMerge(NodeServices.layer),
);
const testLayer = Layer.mergeAll(
  Plans.layer.pipe(Layer.provide(Store.layer)),
  Reactor.layer,
  Grants.layer,
).pipe(
  Layer.provideMerge(core),
  Layer.provideMerge(
    Layer.mock(ServerSettingsService)({
      getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
    }),
  ),
);
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Retry completed",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Agent-reported result" },
  unchecked: [],
  confidence: "medium",
  outcome: "completed",
};
const latest = (plan: CoordinationPlan) => plan.nodes[0]!.attempts.at(-1)!;
const session = Effect.fn(function* (worker: ThreadId, turn: TurnId | null, label: string) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(label),
    threadId: worker,
    createdAt: NOW,
    session: {
      threadId: worker,
      status: turn ? "running" : "ready",
      activeTurnId: turn,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
});
const finish = Effect.fn(function* (worker: ThreadId, turn: TurnId, label: string, count: number) {
  const engine = yield* OrchestrationEngineService;
  yield* session(worker, null, `${label}:idle`);
  yield* engine.dispatch({
    type: "thread.turn.diff.complete",
    commandId: CommandId.make(label),
    threadId: worker,
    turnId: turn,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${label}`),
    status: "ready",
    files: [],
    checkpointTurnCount: count,
    createdAt: NOW,
  });
});

// Only repository discovery and settings are stubbed. Plans, receipts, projections,
// grants and reactor are real. SQLite is read-only below, no provider reactor is installed.
it.layer(testLayer)("same-worker settled-failure retry identity", (it) => {
  for (const ending of ["report", "cancel"] as const) {
    it.effect(
      `reuses the failed worker, rejects stale identities, and accepts current ${ending}`,
      () =>
        Effect.gen(function* () {
          const label = `same-worker-retry-${ending}`;
          const root = ThreadId.make(`${label}:root`);
          const projectId = ProjectId.make(`${label}:project`);
          const target = { callerThreadId: root, rootThreadId: root, planId: `${label}:plan` };
          const engine = yield* OrchestrationEngineService;
          const plans = yield* Plans.CoordinationPlans;
          const reactor = yield* Reactor.CoordinationReactor;
          const grants = yield* Grants.UnattendedGrants;
          const query = yield* ProjectionSnapshotQuery;
          const sql = yield* SqlClient.SqlClient;
          yield* engine.dispatch({
            type: "project.create",
            commandId: CommandId.make(`${label}:project`),
            projectId,
            title: label,
            workspaceRoot: `/workspace/${label}`,
            createdAt: NOW,
          });
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`${label}:root`),
            threadId: root,
            projectId,
            title: label,
            modelSelection: MODEL,
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: "main",
            worktreePath: null,
            createdAt: NOW,
          });
          const grant = yield* grants.create(
            {
              id: `${label}:grant`,
              callerThreadId: root,
              ceiling: { runtimeMode: "approval-required", mcpCapabilities: ["workers"] },
              hostJobs: false,
            },
            { source: "client" },
          );
          const authority: ThreadUnattendedAuthority = {
            grantId: grant.id,
            grantRevision: grant.revision,
            ownerThreadId: root,
            runtimeModeCeiling: grant.ceiling.runtimeMode,
            mcpCapabilityCeiling: grant.ceiling.mcpCapabilities,
          };
          let plan = yield* plans.write(
            {
              ...target,
              commandId: CommandId.make(`${label}:create`),
              expectedRevision: 0,
              operation: "create",
              policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
              nodes: [
                {
                  id: "task",
                  kind: "work",
                  prompt: "Implement isolated retry proof",
                  dependsOn: [],
                  gateScope: [],
                  attemptLimit: 3,
                  modelSelection: MODEL,
                },
              ],
            },
            authority,
          );
          plan = yield* plans.write(
            {
              ...target,
              commandId: CommandId.make(`${label}:run`),
              expectedRevision: plan.revision,
              operation: "run",
            },
            authority,
          );
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          const first = latest(plan);
          const worker = first.workerThreadId;
          const oldTurn = TurnId.make(`${label}:old-turn`);
          yield* session(worker, oldTurn, `${label}:old-start`);
          // A blocked typed report plus a real terminal engine receipt settles failure
          // without requesting stop, leaving the original worker eligible for retry.
          const blocked = {
            ...artifact,
            summary: "First attempt blocked",
            outcome: "blocked" as const,
          };
          plan = yield* plans.write({
            ...target,
            callerThreadId: worker,
            commandId: CommandId.make(`${label}:old-report`),
            expectedRevision: plan.revision,
            operation: "complete",
            nodeId: "task",
            attemptNumber: first.number,
            workerThreadId: worker,
            turnId: oldTurn,
            artifact: blocked,
          });
          yield* finish(worker, oldTurn, `${label}:old-finish`, 1);
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          expect(latest(plan)).toMatchObject({
            status: "failed",
            turnId: oldTurn,
            artifact: blocked,
          });
          expect(
            Option.getOrThrow(yield* query.getWorkerState(worker)).thread.worker?.stopRequestedAt,
          ).toBeNull();
          plan = yield* plans.write(
            {
              ...target,
              commandId: CommandId.make(`${label}:retry`),
              expectedRevision: plan.revision,
              operation: "retry",
              nodeId: "task",
            },
            authority,
          );
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          const current = latest(plan);
          expect(current).toMatchObject({ number: 2, status: "accepted", workerThreadId: worker });
          expect(current.dispatchMessageId).not.toBe(first.dispatchMessageId);
          expect(plan.nodes[0]!.attempts[0]!.status).toBe("superseded");
          const turn = TurnId.make(`${label}:fresh-turn`);
          yield* session(worker, turn, `${label}:fresh-start`);
          expect(
            yield* sql`SELECT turn_id, pending_message_id FROM projection_turns WHERE thread_id = ${worker} ORDER BY turn_id`,
          ).toEqual([
            { turn_id: turn, pending_message_id: current.dispatchMessageId },
            { turn_id: oldTurn, pending_message_id: first.dispatchMessageId },
          ]);
          const beforeWorker = yield* query.getWorkerState(worker);
          const activation =
            yield* sql`SELECT message_id, event_sequence, authority_json FROM projection_thread_activation_authorities WHERE thread_id = ${worker}`;
          expect(activation[0]?.message_id).toBe(current.dispatchMessageId);
          expect(yield* query.getThreadActivationAuthority(worker)).toEqual(Option.some(authority));
          const beforeSequence = yield* engine.latestSequence;
          for (const stale of [
            "report",
            "turn-report",
            "abandon",
            "stop",
            "interrupt",
            "settle",
          ] as const) {
            const commandId = CommandId.make(`${label}:stale-${stale}`);
            const dispatch = {
              commandId,
              threadId: root,
              planId: plan.id,
              expectedRevision: plan.revision,
              nodeId: "task",
              createdAt: NOW,
            };
            if (stale === "report" || stale === "turn-report") {
              expect(
                yield* plans
                  .write({
                    ...target,
                    callerThreadId: worker,
                    commandId,
                    expectedRevision: plan.revision,
                    operation: "complete",
                    nodeId: "task",
                    workerThreadId: worker,
                    attemptNumber: stale === "report" ? first.number : current.number,
                    turnId: stale === "report" ? turn : oldTurn,
                    artifact,
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "forbidden" });
            } else if (stale === "abandon") {
              expect(
                yield* engine
                  .dispatch({
                    ...dispatch,
                    type: "coordination.plan.abandon",
                    expectedMessageId: first.dispatchMessageId,
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "conflict" });
            } else if (stale === "stop" || stale === "interrupt") {
              expect(
                yield* engine
                  .dispatch({
                    type: stale === "stop" ? "thread.session.stop" : "thread.turn.interrupt",
                    commandId,
                    threadId: worker,
                    createdAt: NOW,
                    expectedMessageId:
                      stale === "stop" ? first.dispatchMessageId : current.dispatchMessageId,
                    expectedTurnId: stale === "stop" ? turn : oldTurn,
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "conflict" });
            } else {
              expect(
                yield* engine
                  .dispatch({
                    ...dispatch,
                    type: "coordination.plan.settle",
                    turnId: oldTurn,
                    outcome: "completed",
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "conflict" });
            }
            expect(yield* plans.read(target)).toEqual(plan);
            expect(yield* query.getWorkerState(worker)).toEqual(beforeWorker);
            expect(
              yield* sql`SELECT message_id, event_sequence, authority_json FROM projection_thread_activation_authorities WHERE thread_id = ${worker}`,
            ).toEqual(activation);
            expect(yield* grants.get({ id: grant.id, callerThreadId: root })).toEqual(grant);
          }
          expect(yield* engine.latestSequence).toBe(beforeSequence);
          expect(yield* Stream.runCollect(engine.readEvents(beforeSequence))).toEqual([]);
          if (ending === "report") {
            plan = yield* plans.write({
              ...target,
              callerThreadId: worker,
              commandId: CommandId.make(`${label}:fresh-report`),
              expectedRevision: plan.revision,
              operation: "complete",
              nodeId: "task",
              attemptNumber: current.number,
              workerThreadId: worker,
              turnId: turn,
              artifact,
            });
            yield* finish(worker, turn, `${label}:fresh-finish`, 2);
            yield* reactor.drain(root, plan.id);
            plan = yield* plans.read(target);
            expect(latest(plan)).toMatchObject({
              status: "succeeded",
              workerThreadId: worker,
              turnId: turn,
              dispatchMessageId: current.dispatchMessageId,
              artifact,
            });
          } else {
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make(`${label}:fresh-cancel`),
              expectedRevision: plan.revision,
              operation: "cancel",
            });
            expect(plan.cancelled).toBe(true);
            const events = yield* Stream.runCollect(engine.readEvents(beforeSequence));
            expect(events).toContainEqual(
              expect.objectContaining({
                type: "thread.session-stop-requested",
                aggregateId: worker,
              }),
            );
            expect(
              Option.getOrThrow(yield* query.getWorkerState(worker)).thread.worker?.stopRequestedAt,
            ).not.toBeNull();
          }
          expect(plan.executionAuthority).toEqual(authority);
          expect(yield* grants.get({ id: grant.id, callerThreadId: root })).toEqual(grant);
        }),
    );
  }
});
