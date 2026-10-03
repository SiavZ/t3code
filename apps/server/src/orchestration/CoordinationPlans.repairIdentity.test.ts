import {
  CheckpointRef,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type CoordinationArtifact,
  type CoordinationPlan,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
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
// Real SQLite, event store, decider and projection. Only repository discovery is stubbed.
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
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-repair-identity-" })),
  Layer.provideMerge(NodeServices.layer),
);
const testLayer = Layer.mergeAll(Plans.layer.pipe(Layer.provide(Store.layer)), Reactor.layer).pipe(
  Layer.provideMerge(core),
);
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Repair identity proof",
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
const session = Effect.fn(function* (worker: ThreadId, turnId: TurnId | null, label: string) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(label),
    threadId: worker,
    createdAt: NOW,
    session: {
      threadId: worker,
      status: turnId === null ? "ready" : "running",
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
  yield* session(worker, null, `${label}:idle`);
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
const execute = Effect.fn(function* (
  plan: CoordinationPlan,
  nodeId: string,
  report: CoordinationArtifact,
) {
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  const attempt = plan.nodes.find((entry) => entry.id === nodeId)!.attempts.at(-1)!;
  const label = `${plan.id}:${nodeId}:${attempt.number}`;
  const turnId = TurnId.make(label);
  yield* session(attempt.workerThreadId, turnId, `${label}:start`);
  plan = yield* plans.write({
    ...targetOf(plan),
    callerThreadId: attempt.workerThreadId,
    commandId: CommandId.make(`${label}:report`),
    expectedRevision: plan.revision,
    operation: "complete",
    nodeId,
    attemptNumber: attempt.number,
    workerThreadId: attempt.workerThreadId,
    turnId,
    artifact: report,
  });
  yield* finish(attempt.workerThreadId, turnId, label);
  yield* reactor.drain(plan.rootThreadId, plan.id);
  return yield* plans.read(targetOf(plan));
});
const failedGate = Effect.fn(function* (label: string) {
  const engine = yield* OrchestrationEngineService;
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  const root = ThreadId.make(`${label}:root`);
  const projectId = ProjectId.make(`${label}:project`);
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make(`${label}:project`),
    projectId,
    title: "Repair",
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
  const target = { callerThreadId: root, rootThreadId: root, planId: label };
  let plan = yield* plans.write({
    ...target,
    commandId: CommandId.make(`${label}:create`),
    expectedRevision: 0,
    operation: "create",
    policy: { mode: "deep", maxConcurrent: 1, retainWorkers: true },
    nodes: [node("task"), { ...node("gate", ["task"]), kind: "verify", gateScope: ["task"] }],
  });
  plan = yield* plans.write({
    ...target,
    commandId: CommandId.make(`${label}:run`),
    expectedRevision: plan.revision,
    operation: "run",
  });
  yield* reactor.drain(root, plan.id);
  plan = yield* plans.read(target);
  plan = yield* execute(plan, "task", artifact);
  return yield* execute(plan, "gate", { ...artifact, verdict: "repair" });
});
const repair = Effect.fn(function* (plan: CoordinationPlan) {
  const plans = yield* Plans.CoordinationPlans;
  return yield* plans.write({
    ...targetOf(plan),
    commandId: CommandId.make(`${plan.id}:repair`),
    expectedRevision: plan.revision,
    operation: "repair",
    nodeId: "gate",
    successorGateId: "successor",
    repairs: [node("fix", ["task"])],
  });
});

it.layer(testLayer)("Deep repair successor identity", (it) => {
  it.effect(
    "fences retired gate, turn and message controls from a same-worker active successor",
    () =>
      Effect.gen(function* () {
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const engine = yield* OrchestrationEngineService;
        const query = yield* ProjectionSnapshotQuery;
        let plan = yield* failedGate("repair-successor");
        const old = plan.nodes[1]!.attempts[0]!;
        const retiredFrontierRevision = plan.revision;
        expect(old.status).toBe("failed");
        plan = yield* repair(plan);
        yield* reactor.drain(plan.rootThreadId, plan.id);
        plan = yield* plans.read(targetOf(plan));
        expect(plan.nodes[1]).toMatchObject({ retired: true, attempts: [old] });
        const fix = plan.nodes[2]!.attempts[0]!;
        expect(fix.workerThreadId).toBe(old.workerThreadId);
        expect(fix.dispatchMessageId).not.toBe(old.dispatchMessageId);
        plan = yield* execute(plan, "fix", artifact);
        expect(plan.nodes[2]!.attempts[0]!.turnId).not.toBe(old.turnId);
        const current = plan.nodes[3]!.attempts[0]!;
        expect(current.workerThreadId).toBe(old.workerThreadId);
        expect(current.dispatchMessageId).not.toBe(old.dispatchMessageId);
        expect(current.dispatchMessageId).not.toBe(fix.dispatchMessageId);
        const turnId = TurnId.make("repair-successor:live-turn");
        expect(turnId).not.toBe(old.turnId);
        yield* session(current.workerThreadId, turnId, "repair-successor:live-start");
        const before = yield* query.getWorkerState(current.workerThreadId);
        const dispatch = {
          threadId: plan.rootThreadId,
          planId: plan.id,
          expectedRevision: plan.revision,
          createdAt: NOW,
        };
        const report = {
          ...targetOf(plan),
          callerThreadId: current.workerThreadId,
          expectedRevision: plan.revision,
          operation: "complete" as const,
          attemptNumber: current.number,
          workerThreadId: current.workerThreadId,
          artifact: { ...artifact, verdict: "pass" as const },
        };
        const rejected = [
          plans
            .write({
              ...targetOf(plan),
              commandId: CommandId.make("retired-frontier-cancel"),
              expectedRevision: retiredFrontierRevision,
              operation: "cancel",
            })
            .pipe(Effect.asVoid),
          plans
            .write({
              ...report,
              commandId: CommandId.make("old-gate-report"),
              nodeId: "gate",
              turnId: old.turnId!,
            })
            .pipe(Effect.asVoid),
          plans
            .write({
              ...report,
              commandId: CommandId.make("retired-gate-current-turn-report"),
              nodeId: "gate",
              turnId,
            })
            .pipe(Effect.asVoid),
          plans
            .write({
              ...report,
              commandId: CommandId.make("old-turn-report"),
              nodeId: "successor",
              turnId: old.turnId!,
            })
            .pipe(Effect.asVoid),
          engine
            .dispatch({
              ...dispatch,
              type: "coordination.plan.settle",
              commandId: CommandId.make("old-gate-settle"),
              nodeId: "gate",
              turnId: old.turnId!,
              outcome: "completed",
            })
            .pipe(Effect.asVoid),
          engine
            .dispatch({
              ...dispatch,
              type: "coordination.plan.settle",
              commandId: CommandId.make("old-turn-settle"),
              nodeId: "successor",
              turnId: old.turnId!,
              outcome: "completed",
            })
            .pipe(Effect.asVoid),
          engine
            .dispatch({
              ...dispatch,
              type: "coordination.plan.abandon",
              commandId: CommandId.make("old-gate-abandon"),
              nodeId: "gate",
              expectedMessageId: old.dispatchMessageId,
            })
            .pipe(Effect.asVoid),
          engine
            .dispatch({
              ...dispatch,
              type: "coordination.plan.abandon",
              commandId: CommandId.make("old-message-abandon"),
              nodeId: "successor",
              expectedMessageId: old.dispatchMessageId,
            })
            .pipe(Effect.asVoid),
          engine
            .dispatch({
              type: "thread.session.stop",
              commandId: CommandId.make("old-message-stop"),
              threadId: current.workerThreadId,
              expectedMessageId: old.dispatchMessageId,
              expectedTurnId: turnId,
              createdAt: NOW,
            })
            .pipe(Effect.asVoid),
          engine
            .dispatch({
              type: "thread.turn.interrupt",
              commandId: CommandId.make("old-turn-interrupt"),
              threadId: current.workerThreadId,
              expectedMessageId: current.dispatchMessageId,
              expectedTurnId: old.turnId!,
              createdAt: NOW,
            })
            .pipe(Effect.asVoid),
        ];
        for (const operation of rejected) {
          expect((yield* operation.pipe(Effect.result))._tag).toBe("Failure");
          expect(yield* plans.read(targetOf(plan))).toEqual(plan);
          expect(yield* query.getWorkerState(current.workerThreadId)).toEqual(before);
        }
        plan = yield* plans.write({
          ...report,
          commandId: CommandId.make("current-report"),
          nodeId: "successor",
          turnId,
        });
        expect(plan.nodes[3]!.attempts[0]!.pendingArtifact).toEqual(report.artifact);
        // Cancel is plan-wide, not a gate-addressed operation. The root's fresh revision
        // must still stop the current successor after all stale controls were rejected.
        plan = yield* plans.write({
          ...targetOf(plan),
          commandId: CommandId.make("current-cancel"),
          expectedRevision: plan.revision,
          operation: "cancel",
        });
        expect(plan).toMatchObject({ paused: true, cancelled: true });
        const stopped = yield* query.getWorkerState(current.workerThreadId);
        expect(stopped).toMatchObject({ _tag: "Some", value: { pendingMessageId: null } });
        expect(
          stopped._tag === "Some" && stopped.value.thread.worker?.stopRequestedAt,
        ).toBeTruthy();
        // The simulated native receipt is completed, so its already accepted pass
        // artifact settles successfully. Cancellation still stops the live worker.
        yield* finish(current.workerThreadId, turnId, "current-cancel");
        yield* reactor.drain(plan.rootThreadId, plan.id);
        plan = yield* plans.read(targetOf(plan));
        expect(plan.nodes[1]).toMatchObject({ retired: true, attempts: [old] });
        expect(plan.nodes[1]!.attempts[0]).toEqual(old);
        expect(plan.nodes[3]!.attempts[0]).toMatchObject({
          status: "succeeded",
          turnId,
          dispatchMessageId: current.dispatchMessageId,
          artifact: report.artifact,
          pendingArtifact: null,
        });
      }),
  );
  for (const operation of ["pause", "cancel"] as const) {
    it.effect(`repair preserves ${operation} without implicitly dispatching its new frontier`, () =>
      Effect.gen(function* () {
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        let plan = yield* failedGate(`repair-${operation}`);
        const old = plan.nodes[1]!.attempts[0]!;
        plan = yield* plans.write({
          ...targetOf(plan),
          commandId: CommandId.make(`${plan.id}:${operation}`),
          expectedRevision: plan.revision,
          operation,
        });
        plan = yield* repair(plan);
        yield* reactor.drain(plan.rootThreadId, plan.id);
        plan = yield* plans.read(targetOf(plan));
        expect(plan.paused).toBe(true);
        expect(plan.cancelled).toBe(operation === "cancel");
        expect(plan.nodes[1]!.attempts[0]).toEqual(old);
        expect(plan.nodes[2]!.attempts).toHaveLength(0);
        expect(plan.nodes[3]!.attempts).toHaveLength(0);
        if (operation === "cancel") {
          expect(
            (yield* plans
              .write({
                ...targetOf(plan),
                commandId: CommandId.make(`${plan.id}:implicit-run`),
                expectedRevision: plan.revision,
                operation: "run",
              })
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
          expect(yield* plans.read(targetOf(plan))).toEqual(plan);
        } else {
          plan = yield* plans.write({
            ...targetOf(plan),
            commandId: CommandId.make(`${plan.id}:explicit-run`),
            expectedRevision: plan.revision,
            operation: "run",
          });
          yield* reactor.drain(plan.rootThreadId, plan.id);
          plan = yield* plans.read(targetOf(plan));
          expect(plan.nodes[2]!.attempts).toHaveLength(1);
        }
      }),
    );
  }
});
