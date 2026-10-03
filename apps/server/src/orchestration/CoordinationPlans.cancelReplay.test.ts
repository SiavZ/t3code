import {
  CommandId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type CoordinationWriteInput,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { ServerConfig } from "../config.ts";
import { makeSqlitePersistenceLive } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";
import * as Plans from "./CoordinationPlans.ts";
import * as Store from "./CoordinationPlanStore.ts";
import * as Reactor from "./CoordinationReactor.ts";
import * as ProjectionQuery from "./Services/ProjectionSnapshotQuery.ts";

const ROOT = ThreadId.make("cancel-replay-root");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const NOW = "2026-10-03T00:00:00.000Z";
const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "cancel-replay-plan" };

// Same real engine/projection harness as CoordinationPlans.test.ts, with a scoped
// disk database so receipt replay is also exercised after the engine is closed.
const diskLayer = (path: string) => {
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
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(
      Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
        resolve: () => Effect.succeed(null),
      }),
    ),
    Layer.provideMerge(makeSqlitePersistenceLive(path)),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cancel-replay-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  return Layer.mergeAll(Plans.layer.pipe(Layer.provide(Store.layer)), Reactor.layer).pipe(
    Layer.provideMerge(core),
  );
};

const events = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  return Array.from(yield* Stream.runCollect(engine.readEvents(0, 1_000)));
});

it.effect(
  "replays accepted cancellation receipts after restart without cancelling a new assignment",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-cancel-replay-db-" });
        const layer = diskLayer(`${directory}/state.sqlite`);
        const before = yield* Effect.scoped(
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            const receipts = yield* OrchestrationCommandReceiptRepository;
            yield* engine.dispatch({
              type: "project.create",
              commandId: CommandId.make("cancel-replay-project"),
              projectId: ProjectId.make("cancel-replay-project"),
              title: "Cancellation replay",
              workspaceRoot: directory,
              createdAt: NOW,
            });
            yield* engine.dispatch({
              type: "thread.create",
              commandId: CommandId.make("cancel-replay-root"),
              threadId: ROOT,
              projectId: ProjectId.make("cancel-replay-project"),
              title: "Root",
              modelSelection: MODEL,
              runtimeMode: "approval-required",
              interactionMode: "default",
              branch: "main",
              worktreePath: null,
              createdAt: NOW,
            });
            const node = {
              id: "task",
              kind: "work" as const,
              prompt: "Test cancellation replay without a native provider",
              dependsOn: [],
              gateScope: [],
              attemptLimit: 3,
              modelSelection: MODEL,
            };
            let plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("cancel-replay-create"),
              expectedRevision: 0,
              operation: "create",
              policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
              nodes: [node],
            });
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("cancel-replay-run"),
              expectedRevision: plan.revision,
              operation: "run",
            });
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            const original = plan.nodes[0]!.attempts[0]!;
            expect(original.status).toBe("accepted");
            const input = {
              ...target,
              commandId: CommandId.make("cancel-replay-cancel"),
              expectedRevision: plan.revision,
              operation: "cancel" as const,
            };
            const cancelled = yield* plans.write(input);
            expect(cancelled).toMatchObject({
              paused: true,
              cancelled: true,
              revision: plan.revision + 1,
            });
            const receipt = Option.getOrThrow(
              yield* receipts.getByCommandId({ commandId: input.commandId }),
            );
            expect(receipt).toMatchObject({
              status: "accepted",
              aggregateKind: "thread",
              aggregateId: ROOT,
              error: null,
            });
            const cancellationEvents = (yield* events).filter(
              (event) => event.commandId === input.commandId,
            );
            expect(
              cancellationEvents.filter((event) => event.type === "coordination.plan.updated"),
            ).toHaveLength(1);
            const stops = cancellationEvents.filter(
              (event) => event.type === "thread.session-stop-requested",
            );
            expect(stops).toHaveLength(1);
            expect(stops[0]!.aggregateId).toBe(original.workerThreadId);
            expect(stops[0]!.payload).toMatchObject({
              expectedMessageId: original.dispatchMessageId,
            });
            expect(cancellationEvents.at(-1)!.sequence).toBe(receipt.resultSequence);
            const acceptedHead = yield* engine.latestSequence;
            expect(yield* plans.write(input)).toEqual(cancelled);
            expect(yield* engine.latestSequence).toBe(acceptedHead);
            expect(yield* receipts.getByCommandId({ commandId: input.commandId })).toEqual(
              Option.some(receipt),
            );

            // A deterministic provider stop acknowledgement settles the old attempt.
            // No provider runtime, sleeps, polling, or live server is involved.
            yield* engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cancel-replay-stop-ack"),
              threadId: original.workerThreadId,
              createdAt: NOW,
              session: {
                threadId: original.workerThreadId,
                status: "stopped",
                activeTurnId: null,
                providerName: "codex",
                runtimeMode: "approval-required",
                lastError: null,
                updatedAt: NOW,
              },
            });
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            expect(plan.nodes[0]!.attempts[0]!.status).toBe("interrupted");
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("cancel-replay-retry"),
              expectedRevision: plan.revision,
              operation: "retry",
              nodeId: "task",
            });
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("cancel-replay-resume"),
              expectedRevision: plan.revision,
              operation: "run",
            });
            yield* reactor.drain(ROOT, plan.id);
            plan = yield* plans.read(target);
            expect(plan.cancelled).toBe(false);
            expect(plan.nodes[0]!.attempts).toHaveLength(2);
            expect(plan.nodes[0]!.attempts[1]!.status).toBe("accepted");
            const snapshot = yield* events;
            expect(yield* plans.write(input)).toEqual(plan);
            yield* reactor.drain(ROOT, plan.id);
            expect(yield* events).toEqual(snapshot);
            return { input, receipt, plan, snapshot };
          }).pipe(Effect.provide(Layer.fresh(layer))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const engine = yield* OrchestrationEngineService;
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            const receipts = yield* OrchestrationCommandReceiptRepository;
            const query = yield* ProjectionQuery.ProjectionSnapshotQuery;
            expect(yield* plans.read(target)).toEqual(before.plan);
            expect(yield* receipts.getByCommandId({ commandId: before.input.commandId })).toEqual(
              Option.some(before.receipt),
            );
            const worker = before.plan.nodes[0]!.attempts[1]!.workerThreadId;
            const workerBefore = yield* query.getWorkerState(worker);
            expect(Option.getOrThrow(workerBefore).pendingMessageId).toBe(
              before.plan.nodes[0]!.attempts[1]!.dispatchMessageId,
            );
            const command = {
              type: "coordination.plan.write" as const,
              commandId: before.input.commandId,
              threadId: ROOT,
              input: before.input,
              createdAt: "2026-10-03T01:00:00.000Z",
            };
            // createdAt changes across service calls but is intentionally not payload identity.
            expect(yield* engine.dispatch(command)).toEqual({
              sequence: before.receipt.resultSequence,
            });
            expect(yield* plans.write(before.input)).toEqual(before.plan);
            yield* reactor.drain(ROOT, target.planId);
            expect(yield* events).toEqual(before.snapshot);

            // Cancel has no node, reason, artifact, or policy field in the contract.
            // These are real decoded mutations, not ignored extra properties.
            const changed: CoordinationWriteInput[] = [
              { ...before.input, expectedRevision: before.plan.revision },
              { ...before.input, planId: "other-plan" },
              { ...before.input, operation: "pause" },
            ];
            for (const input of changed) {
              expect(yield* plans.write(input).pipe(Effect.flip)).toMatchObject({
                code: "conflict",
                detail: "Coordination command ID was accepted with different inputs.",
              });
              yield* reactor.drain(ROOT, target.planId);
              expect(yield* plans.read(target)).toEqual(before.plan);
              expect(yield* query.getWorkerState(worker)).toEqual(workerBefore);
              expect(yield* events).toEqual(before.snapshot);
              expect(yield* receipts.getByCommandId({ commandId: before.input.commandId })).toEqual(
                Option.some(before.receipt),
              );
            }
            // A rejected changed-payload replay must not poison the accepted receipt.
            expect(yield* engine.dispatch(command)).toEqual({
              sequence: before.receipt.resultSequence,
            });
            expect(yield* engine.latestSequence).toBe(before.snapshot.at(-1)!.sequence);
            expect(yield* query.getWorkerState(worker)).toEqual(workerBefore);
            expect(yield* events).toEqual(before.snapshot);
          }).pipe(Effect.provide(Layer.fresh(layer))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);
