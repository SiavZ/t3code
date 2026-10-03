import {
  CheckpointRef,
  CommandId,
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
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
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

const NOW = "2026-10-03T00:00:00.000Z";
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
// Real SQLite, event store, engine and reactor. Only repository discovery is
// stubbed. Provider receipts are simulated through engine commands, never processes.
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
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-salvage-identity-" })),
  Layer.provideMerge(NodeServices.layer),
);
const testLayer = Layer.mergeAll(Plans.layer.pipe(Layer.provide(Store.layer)), Reactor.layer).pipe(
  Layer.provideMerge(core),
);
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Root accepted salvage evidence",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Agent report" },
  unchecked: [],
  confidence: "medium",
  outcome: "completed",
};
const node = (id: string, dependsOn: string[] = []) => ({
  id,
  kind: "work" as const,
  prompt: `Implement ${id}`,
  dependsOn,
  gateScope: [],
  attemptLimit: 3,
  modelSelection: MODEL,
});
const targetOf = (plan: CoordinationPlan) => ({
  callerThreadId: plan.rootThreadId,
  rootThreadId: plan.rootThreadId,
  planId: plan.id,
});
const session = Effect.fn(function* (
  worker: ThreadId,
  turnId: TurnId | null,
  label: string,
  status: "running" | "ready" | "stopped",
) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(label),
    threadId: worker,
    createdAt: NOW,
    session: {
      threadId: worker,
      status,
      activeTurnId: turnId,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
});
const finish = Effect.fn(function* (worker: ThreadId, turnId: TurnId, label: string) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.turn.diff.complete",
    commandId: CommandId.make(`${label}:finish`),
    threadId: worker,
    turnId,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${label}`),
    status: "ready",
    files: [],
    checkpointTurnCount: 1,
    createdAt: NOW,
  });
});

it.layer(testLayer)("Salvage successor identity", (it) => {
  for (const outcome of ["failed", "interrupted"] as const) {
    it.effect(
      `fences stale ${outcome} assignment controls after salvage dispatches its dependent`,
      () =>
        Effect.gen(function* () {
          const plans = yield* Plans.CoordinationPlans;
          const reactor = yield* Reactor.CoordinationReactor;
          const engine = yield* OrchestrationEngineService;
          const query = yield* ProjectionSnapshotQuery;
          const sql = yield* SqlClient.SqlClient;
          const label = `salvage-${outcome}`;
          const root = ThreadId.make(`${label}:root`);
          const projectId = ProjectId.make(`${label}:project`);
          yield* engine.dispatch({
            type: "project.create",
            commandId: CommandId.make(`${label}:project`),
            projectId,
            title: "Salvage",
            workspaceRoot: `/workspace/${label}`,
            createdAt: NOW,
          });
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(`${label}:root`),
            threadId: root,
            projectId,
            title: "Root",
            modelSelection: MODEL,
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: "main",
            worktreePath: null,
            createdAt: NOW,
          });
          const grant: ThreadUnattendedAuthority = {
            grantId: `${label}:grant`,
            grantRevision: 1,
            ownerThreadId: root,
            runtimeModeCeiling: "approval-required",
            mcpCapabilityCeiling: ["workers"],
          };
          // Only the external authority ledger is seeded with SQL. Every plan,
          // assignment, report, receipt and control uses the actual service/engine.
          yield* sql`INSERT INTO unattended_grants (grant_id, owner_thread_id, project_id, revision, revoked, ceiling_json, created_at) VALUES (${grant.grantId}, ${root}, ${projectId}, 1, 0, ${JSON.stringify({ runtimeMode: grant.runtimeModeCeiling, mcpCapabilities: grant.mcpCapabilityCeiling })}, ${NOW})`;
          const target = { callerThreadId: root, rootThreadId: root, planId: label };
          let plan = yield* plans.write(
            {
              ...target,
              commandId: CommandId.make(`${label}:create`),
              expectedRevision: 0,
              operation: "create",
              policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
              nodes: [node("task"), node("next", ["task"])],
            },
            grant,
          );
          plan = yield* plans.write(
            {
              ...target,
              commandId: CommandId.make(`${label}:run`),
              expectedRevision: plan.revision,
              operation: "run",
            },
            grant,
          );
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          const first = plan.nodes[0]!.attempts[0]!;
          const oldTurn = TurnId.make(`${label}:old-turn`);
          const original = {
            ...artifact,
            summary: "Original blocked worker evidence",
            outcome: "blocked" as const,
          };
          yield* session(first.workerThreadId, oldTurn, `${label}:old-start`, "running");
          plan = yield* plans.write({
            ...target,
            callerThreadId: first.workerThreadId,
            commandId: CommandId.make(`${label}:old-report`),
            expectedRevision: plan.revision,
            operation: "complete",
            nodeId: "task",
            attemptNumber: first.number,
            workerThreadId: first.workerThreadId,
            turnId: oldTurn,
            artifact: original,
          });
          if (outcome === "interrupted") {
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make(`${label}:old-cancel`),
              expectedRevision: plan.revision,
              operation: "cancel",
            });
          }
          yield* session(
            first.workerThreadId,
            null,
            `${label}:old-idle`,
            outcome === "interrupted" ? "stopped" : "ready",
          );
          yield* finish(first.workerThreadId, oldTurn, `${label}:old`);
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          const old = plan.nodes[0]!.attempts[0]!;
          expect(old).toMatchObject({
            status: outcome,
            turnId: oldTurn,
            artifact: original,
            pendingArtifact: null,
          });
          expect(plan.nodes[1]!.attempts).toHaveLength(0);
          plan = yield* plans.write(
            {
              ...target,
              commandId: CommandId.make(`${label}:salvage`),
              expectedRevision: plan.revision,
              operation: "salvage",
              nodeId: "task",
              artifact,
            },
            grant,
          );
          if (plan.paused) {
            plan = yield* plans.write(
              {
                ...target,
                commandId: CommandId.make(`${label}:resume`),
                expectedRevision: plan.revision,
                operation: "run",
              },
              grant,
            );
          }
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          const salvaged = plan.nodes[0]!.attempts[0]!;
          expect(salvaged).toEqual({
            ...old,
            status: "succeeded",
            failureCode: null,
            salvage: { actorThreadId: root, artifact },
          });
          const current = plan.nodes[1]!.attempts[0]!;
          expect(current).toMatchObject({
            status: "accepted",
            dependencyVersions: [{ nodeId: "task", attemptNumber: old.number }],
          });
          if (outcome === "failed") {
            expect(current.workerThreadId).toBe(old.workerThreadId);
          } else {
            // A cancelled worker is stop-requested and not reusable. Failed work
            // above explicitly exercises the reachable same-worker stale identity path.
            expect(current.workerThreadId).not.toBe(old.workerThreadId);
          }
          expect(current.dispatchMessageId).not.toBe(old.dispatchMessageId);
          expect(plan.executionAuthority).toEqual(grant);
          const turnId = TurnId.make(`${label}:current-turn`);
          yield* session(current.workerThreadId, turnId, `${label}:current-start`, "running");
          const before = yield* query.getWorkerState(current.workerThreadId);
          expect(before).toMatchObject({
            _tag: "Some",
            value: { thread: { session: { activeTurnId: turnId } } },
          });
          const authorityBefore = yield* query.getThreadActivationAuthority(current.workerThreadId);
          expect(authorityBefore).toMatchObject({ _tag: "Some", value: grant });
          const dispatch = {
            threadId: root,
            planId: plan.id,
            expectedRevision: plan.revision,
            createdAt: NOW,
          };
          const report = {
            ...target,
            callerThreadId: current.workerThreadId,
            expectedRevision: plan.revision,
            operation: "complete" as const,
            attemptNumber: current.number,
            workerThreadId: current.workerThreadId,
            artifact,
          };
          const rejected: Array<Effect.Effect<unknown, unknown>> = [
            plans.write({
              ...report,
              commandId: CommandId.make(`${label}:old-task-report`),
              nodeId: "task",
              turnId: oldTurn,
            }),
            plans.write({
              ...report,
              commandId: CommandId.make(`${label}:old-task-current-turn-report`),
              nodeId: "task",
              turnId,
            }),
            plans.write({
              ...report,
              commandId: CommandId.make(`${label}:old-turn-report`),
              nodeId: "next",
              turnId: oldTurn,
            }),
            engine.dispatch({
              ...dispatch,
              type: "coordination.plan.settle",
              commandId: CommandId.make(`${label}:old-task-settle`),
              nodeId: "task",
              turnId: oldTurn,
              outcome: "interrupted",
            }),
            engine.dispatch({
              ...dispatch,
              type: "coordination.plan.settle",
              commandId: CommandId.make(`${label}:old-turn-settle`),
              nodeId: "next",
              turnId: oldTurn,
              outcome: "interrupted",
            }),
            engine.dispatch({
              ...dispatch,
              type: "coordination.plan.abandon",
              commandId: CommandId.make(`${label}:old-task-abandon`),
              nodeId: "task",
              expectedMessageId: old.dispatchMessageId,
            }),
            engine.dispatch({
              ...dispatch,
              type: "coordination.plan.abandon",
              commandId: CommandId.make(`${label}:old-message-abandon`),
              nodeId: "next",
              expectedMessageId: old.dispatchMessageId,
            }),
            engine.dispatch({
              type: "thread.session.stop",
              commandId: CommandId.make(`${label}:old-message-stop`),
              threadId: current.workerThreadId,
              expectedMessageId: old.dispatchMessageId,
              expectedTurnId: turnId,
              createdAt: NOW,
            }),
            engine.dispatch({
              type: "thread.session.stop",
              commandId: CommandId.make(`${label}:old-turn-stop`),
              threadId: current.workerThreadId,
              expectedMessageId: current.dispatchMessageId,
              expectedTurnId: oldTurn,
              createdAt: NOW,
            }),
            engine.dispatch({
              type: "thread.turn.interrupt",
              commandId: CommandId.make(`${label}:old-message-interrupt`),
              threadId: current.workerThreadId,
              expectedMessageId: old.dispatchMessageId,
              expectedTurnId: turnId,
              createdAt: NOW,
            }),
            engine.dispatch({
              type: "thread.turn.interrupt",
              commandId: CommandId.make(`${label}:old-turn-interrupt`),
              threadId: current.workerThreadId,
              expectedMessageId: current.dispatchMessageId,
              expectedTurnId: oldTurn,
              createdAt: NOW,
            }),
          ];
          for (const operation of rejected) {
            expect((yield* operation.pipe(Effect.result))._tag).toBe("Failure");
            expect(yield* plans.read(target)).toEqual(plan);
            expect(yield* query.getWorkerState(current.workerThreadId)).toEqual(before);
            expect(yield* query.getThreadActivationAuthority(current.workerThreadId)).toEqual(
              authorityBefore,
            );
          }
          plan = yield* plans.write({
            ...report,
            commandId: CommandId.make(`${label}:current-report`),
            nodeId: "next",
            turnId,
          });
          expect(plan.nodes[1]!.attempts[0]!.pendingArtifact).toEqual(artifact);
          plan = yield* plans.write({
            ...targetOf(plan),
            commandId: CommandId.make(`${label}:current-cancel`),
            expectedRevision: plan.revision,
            operation: "cancel",
          });
          expect(plan).toMatchObject({ paused: true, cancelled: true, executionAuthority: grant });
          const stopped = yield* query.getWorkerState(current.workerThreadId);
          expect(stopped).toMatchObject({ _tag: "Some", value: { pendingMessageId: null } });
          expect(yield* query.getThreadActivationAuthority(current.workerThreadId)).toEqual(
            authorityBefore,
          );
          expect(
            stopped._tag === "Some" && stopped.value.thread.worker?.stopRequestedAt,
          ).toBeTruthy();
          yield* session(current.workerThreadId, null, `${label}:current-idle`, "stopped");
          yield* finish(current.workerThreadId, turnId, `${label}:current`);
          yield* reactor.drain(root, plan.id);
          plan = yield* plans.read(target);
          expect(plan.nodes[1]!.attempts[0]).toMatchObject({
            status: "interrupted",
            turnId,
            dispatchMessageId: current.dispatchMessageId,
            artifact,
            pendingArtifact: null,
          });
          expect(plan.nodes[0]!.attempts).toEqual([salvaged]);
          expect(plan.nodes[0]!.attempts[0]!.artifact).toEqual(original);
          expect(plan.nodes[0]!.attempts[0]!.salvage).toEqual({ actorThreadId: root, artifact });
          expect(plan.executionAuthority).toEqual(grant);
        }),
    );
  }
});
