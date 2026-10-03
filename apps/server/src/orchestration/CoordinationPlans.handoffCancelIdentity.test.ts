import {
  CheckpointRef,
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type CoordinationArtifact,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
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
// Real SQLite, command receipts, engine, projections and coordination reactor.
// Only repository discovery is stubbed. No provider process is started.
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
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-handoff-cancel-" })),
  Layer.provideMerge(NodeServices.layer),
);
const testLayer = Layer.mergeAll(Plans.layer.pipe(Layer.provide(Store.layer)), Reactor.layer).pipe(
  Layer.provideMerge(core),
);
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Report-only continuation",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Agent-reported" },
  unchecked: [],
  confidence: "medium",
  outcome: "completed",
};
const session = Effect.fn(function* (
  threadId: ThreadId,
  turnId: TurnId | null,
  label: string,
  status: "running" | "ready" | "stopped",
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
const finish = Effect.fn(function* (threadId: ThreadId, turnId: TurnId, label: string) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.turn.diff.complete",
    commandId: CommandId.make(label),
    threadId,
    turnId,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${label}`),
    status: "ready",
    files: [],
    checkpointTurnCount: 1,
    createdAt: NOW,
  });
});
const setup = Effect.fn(function* (label: string, running: boolean) {
  const ROOT = ThreadId.make(`${label}:root`);
  const engine = yield* OrchestrationEngineService;
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make(`${label}:project`),
    projectId: ProjectId.make(label),
    title: "Handoff",
    workspaceRoot: `/workspace/${label}`,
    createdAt: NOW,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`${label}:root`),
    threadId: ROOT,
    projectId: ProjectId.make(label),
    title: "Root",
    modelSelection: MODEL,
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    createdAt: NOW,
  });
  const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: label };
  const node = (id: string) => ({
    id,
    kind: "work" as const,
    prompt: id,
    dependsOn: [],
    gateScope: [],
    attemptLimit: 3,
    modelSelection: MODEL,
  });
  let plan = yield* plans.write({
    ...target,
    commandId: CommandId.make(`${label}:create`),
    expectedRevision: 0,
    operation: "create",
    policy: { mode: "deep", maxConcurrent: 1, retainWorkers: true },
    nodes: [
      node("task"),
      { ...node("verify"), kind: "verify", dependsOn: ["task"], gateScope: ["task"] },
    ],
  });
  plan = yield* plans.write({
    ...target,
    commandId: CommandId.make(`${label}:run`),
    expectedRevision: plan.revision,
    operation: "run",
  });
  yield* reactor.drain(target.rootThreadId, plan.id);
  plan = yield* plans.read(target);
  const initial = plan.nodes[0]!.attempts[0]!;
  const oldTurn = TurnId.make(`${label}:initial-turn`);
  yield* session(initial.workerThreadId, oldTurn, `${label}:initial-start`, "running");
  yield* session(initial.workerThreadId, null, `${label}:initial-idle`, "ready");
  yield* finish(initial.workerThreadId, oldTurn, `${label}:initial-finish`);
  yield* reactor.drain(target.rootThreadId, plan.id);
  plan = yield* plans.read(target);
  const current = plan.nodes[0]!.attempts[0]!;
  expect(plan.nodes[0]!.attempts).toHaveLength(1);
  expect(current).toMatchObject({
    number: initial.number,
    workerThreadId: initial.workerThreadId,
    handoffRequested: true,
    initialDispatchMessageId: initial.dispatchMessageId,
    turnId: null,
    status: "accepted",
  });
  expect(current.dispatchMessageId).not.toBe(initial.dispatchMessageId);
  const turnId = TurnId.make(`${label}:handoff-turn`);
  if (running) yield* session(current.workerThreadId, turnId, `${label}:handoff-start`, "running");
  return { target, plan, initial, current, oldTurn, turnId };
});

it.layer(testLayer)("Report-only handoff cancellation identity", (it) => {
  for (const running of [false, true]) {
    for (const stale of ["stop", "ack", "report", "settlement"] as const) {
      it.effect(
        `${running ? "running" : "pending"} handoff rejects old initial ${stale}, cancels current message once`,
        () =>
          Effect.gen(function* () {
            const label = `${running ? "running" : "pending"}-${stale}`;
            const { target, initial, current, oldTurn, turnId, plan } = yield* setup(
              label,
              running,
            );
            const engine = yield* OrchestrationEngineService;
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            const query = yield* ProjectionSnapshotQuery;
            const beforeWorker = yield* query.getWorkerState(current.workerThreadId);
            const beforeHead = yield* engine.latestSequence;
            const commandId = CommandId.make(`${label}:stale`);
            if (stale === "stop") {
              expect(
                yield* engine
                  .dispatch({
                    type: "thread.session.stop",
                    commandId,
                    threadId: current.workerThreadId,
                    expectedMessageId: initial.dispatchMessageId,
                    createdAt: NOW,
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "conflict" });
            } else if (stale === "ack") {
              expect(
                yield* engine
                  .dispatch({
                    type: "thread.session.set",
                    commandId,
                    threadId: current.workerThreadId,
                    expectedMessageId: initial.dispatchMessageId,
                    createdAt: NOW,
                    session: {
                      threadId: current.workerThreadId,
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
            } else if (stale === "report") {
              expect(
                yield* plans
                  .write({
                    ...target,
                    callerThreadId: current.workerThreadId,
                    commandId,
                    expectedRevision: plan.revision,
                    operation: "complete",
                    nodeId: "task",
                    attemptNumber: current.number,
                    workerThreadId: current.workerThreadId,
                    turnId: oldTurn,
                    artifact,
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "forbidden" });
            } else {
              expect(
                yield* engine
                  .dispatch({
                    type: "coordination.plan.settle",
                    commandId,
                    threadId: target.rootThreadId,
                    planId: plan.id,
                    expectedRevision: plan.revision,
                    nodeId: "task",
                    turnId: oldTurn,
                    outcome: "completed",
                    createdAt: NOW,
                  })
                  .pipe(Effect.flip),
              ).toMatchObject({ code: "conflict" });
            }
            expect(yield* engine.latestSequence).toBe(beforeHead);
            expect(yield* plans.read(target)).toEqual(plan);
            expect(yield* query.getWorkerState(current.workerThreadId)).toEqual(beforeWorker);
            yield* reactor.drain(target.rootThreadId, plan.id);
            expect(yield* plans.read(target)).toEqual(plan);
            // Current activation remains stoppable. Engine dispatch awaits the durable
            // receipt and exact command replay cannot emit a second cancellation.
            const stop = {
              type: "thread.session.stop" as const,
              commandId: CommandId.make(`${label}:current-stop`),
              threadId: current.workerThreadId,
              expectedMessageId: current.dispatchMessageId,
              ...(running ? { expectedTurnId: turnId } : {}),
              createdAt: NOW,
            };
            const receipt = yield* engine.dispatch(stop);
            const afterStop = yield* query.getWorkerState(current.workerThreadId);
            const acceptedHead = yield* engine.latestSequence;
            expect(yield* engine.dispatch(stop)).toEqual(receipt);
            expect(yield* engine.latestSequence).toBe(acceptedHead);
            expect(yield* query.getWorkerState(current.workerThreadId)).toEqual(afterStop);
            const stops = Array.from(
              yield* Stream.runCollect(engine.readEvents(beforeHead, 100)),
            ).filter(
              (event) =>
                event.commandId === stop.commandId &&
                event.type === "thread.session-stop-requested",
            );
            expect(stops).toHaveLength(1);
            expect(stops[0]!.payload).toMatchObject({
              expectedMessageId: current.dispatchMessageId,
            });
            yield* engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make(`${label}:current-ack`),
              threadId: current.workerThreadId,
              expectedMessageId: current.dispatchMessageId,
              createdAt: NOW,
              session: {
                threadId: current.workerThreadId,
                status: "stopped",
                activeTurnId: null,
                providerName: "codex",
                runtimeMode: "approval-required",
                lastError: null,
                updatedAt: NOW,
              },
            });
            if (running) yield* finish(current.workerThreadId, turnId, `${label}:handoff-finish`);
            yield* reactor.drain(target.rootThreadId, plan.id);
            const settled = yield* plans.read(target);
            expect(settled.nodes[0]!.attempts).toHaveLength(1);
            expect(settled.nodes[0]!.attempts[0]).toMatchObject({
              number: current.number,
              workerThreadId: current.workerThreadId,
              dispatchMessageId: current.dispatchMessageId,
              initialDispatchMessageId: initial.dispatchMessageId,
              handoffRequested: true,
              status: "interrupted",
            });
            expect(settled.nodes[1]!.attempts).toHaveLength(0);
            yield* reactor.drain(target.rootThreadId, plan.id);
            expect(yield* plans.read(target)).toEqual(settled);
          }),
      );
    }
    it.effect(
      `${running ? "running" : "pending"} handoff graph cancellation targets current message exactly once`,
      () =>
        Effect.gen(function* () {
          const label = `${running ? "running" : "pending"}-graph-cancel`;
          const { target, initial, current, oldTurn, turnId, plan } = yield* setup(label, running);
          const plans = yield* Plans.CoordinationPlans;
          const engine = yield* OrchestrationEngineService;
          const reactor = yield* Reactor.CoordinationReactor;
          const input = {
            ...target,
            commandId: CommandId.make(`${label}:cancel`),
            expectedRevision: plan.revision,
            operation: "cancel" as const,
          };
          const beforeHead = yield* engine.latestSequence;
          const cancelled = yield* plans.write(input);
          expect(cancelled).toMatchObject({
            paused: true,
            cancelled: true,
            revision: plan.revision + 1,
          });
          const acceptedHead = yield* engine.latestSequence;
          expect(yield* plans.write(input)).toEqual(cancelled);
          expect(yield* engine.latestSequence).toBe(acceptedHead);
          const cancelEvents = Array.from(
            yield* Stream.runCollect(engine.readEvents(beforeHead, 100)),
          ).filter((event) => event.commandId === input.commandId);
          const stops = cancelEvents.filter(
            (event) => event.type === "thread.session-stop-requested",
          );
          expect(stops).toHaveLength(1);
          expect(stops[0]!.payload).toMatchObject({ expectedMessageId: current.dispatchMessageId });
          expect(
            cancelEvents.filter((event) => event.type === "coordination.plan.updated"),
          ).toHaveLength(1);
          // Cancellation closes reporting even for the exact current assignment.
          expect(
            yield* plans
              .write({
                ...target,
                callerThreadId: current.workerThreadId,
                commandId: CommandId.make(`${label}:late-report`),
                expectedRevision: cancelled.revision,
                operation: "complete",
                nodeId: "task",
                attemptNumber: current.number,
                workerThreadId: current.workerThreadId,
                turnId: running ? turnId : oldTurn,
                artifact,
              })
              .pipe(Effect.flip),
          ).toMatchObject({ code: "forbidden" });
          yield* engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make(`${label}:ack`),
            threadId: current.workerThreadId,
            expectedMessageId: current.dispatchMessageId,
            createdAt: NOW,
            session: {
              threadId: current.workerThreadId,
              status: "stopped",
              activeTurnId: null,
              providerName: "codex",
              runtimeMode: "approval-required",
              lastError: null,
              updatedAt: NOW,
            },
          });
          if (running) yield* finish(current.workerThreadId, turnId, `${label}:late-finish`);
          yield* reactor.drain(target.rootThreadId, plan.id);
          const settled = yield* plans.read(target);
          expect(settled).toMatchObject({ paused: true, cancelled: true });
          expect(settled.nodes[0]!.attempts).toHaveLength(1);
          expect(settled.nodes[0]!.attempts[0]).toMatchObject({
            number: current.number,
            workerThreadId: current.workerThreadId,
            dispatchMessageId: current.dispatchMessageId,
            initialDispatchMessageId: initial.dispatchMessageId,
            handoffRequested: true,
            status: "interrupted",
          });
          expect(settled.nodes[1]!.attempts).toHaveLength(0);
          const settledHead = yield* engine.latestSequence;
          yield* reactor.drain(target.rootThreadId, plan.id);
          expect(yield* plans.read(target)).toEqual(settled);
          expect(yield* engine.latestSequence).toBe(settledHead);
        }),
    );
  }
});
