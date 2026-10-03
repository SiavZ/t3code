import {
  CheckpointRef,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type CoordinationArtifact,
  type ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
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
const ROOT = ThreadId.make("reopen-root");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "reopen-plan" };
const grant: ThreadUnattendedAuthority = {
  grantId: "reopen-G",
  grantRevision: 1,
  ownerThreadId: ROOT,
  runtimeModeCeiling: "approval-required",
  mcpCapabilityCeiling: ["workers"],
};
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Original failed report",
  findings: ["Partial implementation"],
  evidence: [],
  validation: { status: "notRun", detail: "Agent report only" },
  unchecked: ["Remaining work"],
  confidence: "medium",
  outcome: "blocked",
};
const salvage: CoordinationArtifact = {
  ...artifact,
  summary: "Owner accepted partial work",
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
const session = Effect.fn(function* (
  threadId: ThreadId,
  turnId: TurnId | null,
  label: string,
  status: "running" | "ready" | "interrupted",
) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(label),
    threadId,
    createdAt: NOW,
    session: {
      threadId,
      status,
      activeTurnId: turnId,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
});

// Real disk SQLite, engine, event store and projections. Closing Effect scopes
// disposes the first engine before reopening. No native process survives here.
it.effect(
  "disk reopen preserves history and grant, then isolates old cancellation from an explicit retry",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-reopen-identity-" });
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
          Layer.provideMerge(makeSqlitePersistenceLive(`${directory}/state.sqlite`)),
          Layer.provideMerge(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-reopen-identity-config-" }),
          ),
          Layer.provideMerge(NodeServices.layer),
        );
        const diskLayer = Layer.mergeAll(
          Plans.layer.pipe(Layer.provide(Store.layer)),
          Reactor.layer,
        ).pipe(Layer.provideMerge(core));
        const before = yield* Effect.scoped(
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            const sql = yield* SqlClient.SqlClient;
            yield* engine.dispatch({
              type: "project.create",
              commandId: CommandId.make("reopen-project"),
              projectId: ProjectId.make("reopen-project"),
              title: "Reopen proof",
              workspaceRoot: "/workspace/reopen-proof",
              createdAt: NOW,
            });
            yield* engine.dispatch({
              type: "thread.create",
              commandId: CommandId.make("reopen-root"),
              threadId: ROOT,
              projectId: ProjectId.make("reopen-project"),
              title: "Root",
              modelSelection: MODEL,
              runtimeMode: "approval-required",
              interactionMode: "default",
              branch: "main",
              worktreePath: null,
              createdAt: NOW,
            });
            yield* sql`INSERT INTO unattended_grants (grant_id, owner_thread_id, project_id, revision, revoked, ceiling_json, created_at) VALUES (${grant.grantId}, ${ROOT}, ${"reopen-project"}, 1, 0, ${JSON.stringify({ runtimeMode: grant.runtimeModeCeiling, mcpCapabilities: grant.mcpCapabilityCeiling })}, ${NOW})`;
            let plan = yield* plans.write(
              {
                ...target,
                commandId: CommandId.make("reopen-create"),
                expectedRevision: 0,
                operation: "create",
                policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
                nodes: [node("history"), node("task", ["history"])],
              },
              grant,
            );
            plan = yield* plans.write(
              {
                ...target,
                commandId: CommandId.make("reopen-run"),
                expectedRevision: plan.revision,
                operation: "run",
              },
              grant,
            );
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            const history = plan.nodes[0]!.attempts[0]!;
            const historyTurn = TurnId.make("history-turn");
            yield* session(history.workerThreadId, historyTurn, "history-start", "running");
            plan = yield* plans.write({
              ...target,
              callerThreadId: history.workerThreadId,
              commandId: CommandId.make("history-report"),
              expectedRevision: plan.revision,
              operation: "complete",
              nodeId: "history",
              attemptNumber: history.number,
              workerThreadId: history.workerThreadId,
              turnId: historyTurn,
              artifact,
            });
            yield* session(history.workerThreadId, null, "history-idle", "ready");
            yield* engine.dispatch({
              type: "thread.turn.diff.complete",
              commandId: CommandId.make("history-checkpoint"),
              threadId: history.workerThreadId,
              turnId: historyTurn,
              completedAt: NOW,
              checkpointRef: CheckpointRef.make("refs/t3/checkpoints/history"),
              status: "ready",
              files: [],
              checkpointTurnCount: 1,
              createdAt: NOW,
            });
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            expect(plan.nodes[0]!.attempts[0]!.status).toBe("failed");
            plan = yield* plans.write(
              {
                ...target,
                commandId: CommandId.make("history-salvage"),
                expectedRevision: plan.revision,
                operation: "salvage",
                nodeId: "history",
                artifact: salvage,
              },
              grant,
            );
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            const old = plan.nodes[1]!.attempts[0]!;
            const oldTurn = TurnId.make("old-task-turn");
            yield* session(old.workerThreadId, oldTurn, "old-task-start", "running");
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("old-task-cancel"),
              expectedRevision: plan.revision,
              operation: "cancel",
            });
            expect(plan.nodes[1]!.attempts[0]!.status).toBe("accepted");
            return {
              plan,
              oldTurn,
              worker: yield* (yield* ProjectionSnapshotQuery).getWorkerState(old.workerThreadId),
            };
          }).pipe(Effect.provide(Layer.fresh(diskLayer))),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            const engine = yield* OrchestrationEngineService;
            const query = yield* ProjectionSnapshotQuery;
            let plan = yield* plans.read(target);
            // Full structural equality covers every persisted plan and attempt field,
            // including artifact/salvage provenance and cancellation message identity.
            expect(plan).toEqual(before.plan);
            const old = before.plan.nodes[1]!.attempts[0]!;
            expect(yield* query.getWorkerState(old.workerThreadId)).toEqual(before.worker);
            expect(plan.executionAuthority).toEqual(grant);
            expect(plan.nodes[0]!.attempts[0]!.artifact).toEqual(artifact);
            expect(plan.nodes[0]!.attempts[0]!.salvage).toEqual({
              actorThreadId: ROOT,
              artifact: salvage,
            });
            // Model startup reconciliation explicitly, not native crash continuity.
            yield* session(old.workerThreadId, null, "orphan-reconciled", "interrupted");
            yield* reactor.recover;
            plan = yield* plans.read(target);
            expect(plan.paused).toBe(true);
            expect(plan.nodes[1]!.attempts[0]!.status).toBe("interrupted");
            yield* reactor.drain(ROOT, plan.id);
            expect((yield* plans.read(target)).nodes[1]!.attempts).toHaveLength(1);
            const history = plan.nodes[0];
            const frozen = plan;
            expect(
              yield* plans
                .write(
                  {
                    ...target,
                    commandId: CommandId.make("reopen-wrong-grant"),
                    expectedRevision: plan.revision,
                    operation: "retry",
                    nodeId: "task",
                  },
                  { ...grant, grantId: "different-grant" },
                )
                .pipe(Effect.flip),
            ).toMatchObject({ code: "forbidden" });
            expect(yield* plans.read(target)).toEqual(frozen);
            plan = yield* plans.write(
              {
                ...target,
                commandId: CommandId.make("reopen-retry"),
                expectedRevision: plan.revision,
                operation: "retry",
                nodeId: "task",
              },
              grant,
            );
            expect(plan.paused).toBe(true);
            plan = yield* plans.write(
              {
                ...target,
                commandId: CommandId.make("reopen-resume"),
                expectedRevision: plan.revision,
                operation: "run",
              },
              grant,
            );
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            const fresh = plan.nodes[1]!.attempts[1]!;
            expect(fresh.number).toBe(old.number + 1);
            expect(fresh.workerThreadId).not.toBe(old.workerThreadId);
            expect(yield* query.getThreadActivationAuthority(fresh.workerThreadId)).toMatchObject({
              _tag: "Some",
              value: grant,
            });
            expect(fresh.dispatchMessageId).not.toBe(old.dispatchMessageId);
            expect(plan.nodes[1]!.attempts[0]!.status).toBe("superseded");
            expect(plan.nodes[0]).toEqual(history);
            expect(plan.executionAuthority).toEqual(grant);
            const freshTurn = TurnId.make("fresh-task-turn");
            yield* session(fresh.workerThreadId, freshTurn, "fresh-task-start", "running");
            const beforeWorker = yield* query.getWorkerState(fresh.workerThreadId);
            expect(beforeWorker).toMatchObject({
              _tag: "Some",
              value: { thread: { session: { status: "running", activeTurnId: freshTurn } } },
            });
            expect(
              yield* engine
                .dispatch({
                  type: "thread.session.stop",
                  commandId: CommandId.make("stale-stop"),
                  threadId: fresh.workerThreadId,
                  expectedMessageId: old.dispatchMessageId,
                  expectedTurnId: before.oldTurn,
                  createdAt: NOW,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            // Exact pre-reopen cancellation replay must not emit another stop.
            yield* plans.write({
              ...target,
              commandId: CommandId.make("old-task-cancel"),
              expectedRevision: before.plan.revision - 1,
              operation: "cancel",
            });
            // A late acknowledgement addressed to the actual pre-reopen worker
            // may settle that worker, but cannot settle the new assignment.
            yield* engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("old-worker-stop-ack"),
              threadId: old.workerThreadId,
              expectedMessageId: old.dispatchMessageId,
              createdAt: NOW,
              session: {
                threadId: old.workerThreadId,
                status: "stopped",
                activeTurnId: null,
                providerName: "codex",
                runtimeMode: "approval-required",
                lastError: null,
                updatedAt: NOW,
              },
            });
            expect(yield* query.getWorkerState(fresh.workerThreadId)).toEqual(beforeWorker);
            expect(yield* plans.read(target)).toEqual(plan);
            expect(
              yield* engine
                .dispatch({
                  type: "thread.session.set",
                  commandId: CommandId.make("stale-stop-ack"),
                  threadId: fresh.workerThreadId,
                  expectedMessageId: old.dispatchMessageId,
                  createdAt: NOW,
                  session: {
                    threadId: fresh.workerThreadId,
                    status: "stopped",
                    activeTurnId: null,
                    providerName: "codex",
                    runtimeMode: "approval-required",
                    lastError: null,
                    updatedAt: NOW,
                  },
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            expect(
              yield* plans
                .write({
                  ...target,
                  callerThreadId: fresh.workerThreadId,
                  commandId: CommandId.make("stale-report"),
                  expectedRevision: plan.revision,
                  operation: "complete",
                  nodeId: "task",
                  attemptNumber: old.number,
                  workerThreadId: fresh.workerThreadId,
                  turnId: before.oldTurn,
                  artifact: salvage,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "forbidden" });
            expect(yield* query.getWorkerState(fresh.workerThreadId)).toEqual(beforeWorker);
            expect(yield* plans.read(target)).toEqual(plan);
            yield* reactor.drain(ROOT, plan.id);
            expect(yield* plans.read(target)).toEqual(plan);
          }).pipe(Effect.provide(Layer.fresh(diskLayer))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
