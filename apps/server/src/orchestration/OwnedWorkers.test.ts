import * as NodePath from "node:path";

import {
  CheckpointRef,
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ProviderDriverKind,
  ThreadId,
  TurnId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import { ServerConfig } from "../config.ts";
import { ProviderService } from "../provider/Services/ProviderService.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { reconcileWorkerPendingStarts } from "../serverRuntimeStartup.ts";
import * as ServerSettings from "../serverSettings.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import * as OwnedWorkers from "./OwnedWorkers.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";

const NOW = "2026-10-02T00:00:00.000Z";
const ROOT = ThreadId.make("root");
const PROJECT = ProjectId.make("project");
const MODEL: ModelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };

function testLayer(databasePath?: string) {
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
    Layer.provide(databasePath ? makeSqlitePersistenceLive(databasePath) : SqlitePersistenceMemory),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-owned-workers-test-" })),
    Layer.provideMerge(NodeServices.layer),
  );
  return OwnedWorkers.layer.pipe(
    Layer.provideMerge(core),
    Layer.provideMerge(
      ServerSettings.layerTest({ enableAgentBrowserAccess: true, enableAgentDeviceAccess: false }),
    ),
  );
}

const setup = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("project"),
    projectId: PROJECT,
    title: "Workers",
    workspaceRoot: "/workspace/workers",
    createdAt: NOW,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make("root"),
    threadId: ROOT,
    projectId: PROJECT,
    title: "Root",
    modelSelection: MODEL,
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    createdAt: NOW,
  });
});
const spawn = (command = "spawn", callerThreadId = ROOT) =>
  Effect.gen(function* () {
    const workers = yield* OwnedWorkers.OwnedWorkers;
    return yield* workers.spawn({
      commandId: CommandId.make(command),
      callerThreadId,
      label: command,
      prompt: "Implement the task",
      modelSelection: MODEL,
    });
  });
const session = (
  threadId: ThreadId,
  status: "running" | "ready" | "error" | "stopped",
  activeTurnId: TurnId | null,
  commandId: string,
) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    return yield* engine.dispatch({
      type: "thread.session.set",
      commandId: CommandId.make(commandId),
      threadId,
      createdAt: NOW,
      session: {
        threadId,
        status,
        activeTurnId,
        providerName: "codex",
        runtimeMode: "approval-required",
        lastError: status === "error" ? "Startup failed" : null,
        updatedAt: NOW,
      },
    });
  });
const complete = (threadId: ThreadId, name = "turn", checkpointTurnCount = 1) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const turnId = TurnId.make(name);
    yield* session(threadId, "running", turnId, `${name}:running`);
    yield* engine.dispatch({
      type: "thread.message.assistant.delta",
      commandId: CommandId.make(`${name}:delta`),
      threadId,
      messageId: MessageId.make(`${name}:answer`),
      turnId,
      delta: "Done",
      createdAt: NOW,
    });
    yield* engine.dispatch({
      type: "thread.message.assistant.complete",
      commandId: CommandId.make(`${name}:answer`),
      threadId,
      messageId: MessageId.make(`${name}:answer`),
      turnId,
      createdAt: NOW,
    });
    yield* session(threadId, "ready", null, `${name}:ready`);
    yield* engine.dispatch({
      type: "thread.turn.diff.complete",
      commandId: CommandId.make(`${name}:complete`),
      threadId,
      turnId,
      completedAt: NOW,
      checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${threadId}/${name}`),
      status: "ready",
      files: [],
      assistantMessageId: MessageId.make(`${name}:answer`),
      checkpointTurnCount,
      createdAt: NOW,
    });
  });

// Instrument the actual engine's subscription acquisition, without replacing any
// command, persistence, projection, status, or event-stream behavior.
const observedWorkers = (
  subscribed: Deferred.Deferred<void>,
  closed: Ref.Ref<number>,
  reads?: Ref.Ref<number>,
) =>
  Layer.effect(
    OwnedWorkers.OwnedWorkers,
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const snapshots = yield* ProjectionSnapshotQuery;
      const context = yield* Layer.build(Layer.fresh(OwnedWorkers.layer)).pipe(
        Effect.provideService(ProjectionSnapshotQuery, {
          ...snapshots,
          getWorkerState: (id) =>
            snapshots
              .getWorkerState(id)
              .pipe(Effect.tap(() => (reads ? Ref.update(reads, (n) => n + 1) : Effect.void))),
        }),
        Effect.provideService(OrchestrationEngineService, {
          ...engine,
          subscribeDomainEvents: engine.subscribeDomainEvents.pipe(
            Effect.tap(() => Effect.addFinalizer(() => Ref.update(closed, (n) => n + 1))),
            Effect.tap(() => Deferred.succeed(subscribed, undefined)),
          ),
        }),
      );
      return Context.get(context, OwnedWorkers.OwnedWorkers);
    }),
  );

describe("OwnedWorkers", () => {
  it.effect("streaming deltas do not requery wait status", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      const turnId = TurnId.make("stream-turn");
      yield* session(workerThreadId, "running", turnId, "stream-running");
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const reads = yield* Ref.make(0);
      const workers = Context.get(
        yield* Layer.build(observedWorkers(subscribed, closed, reads)),
        OwnedWorkers.OwnedWorkers,
      );
      const waiter = yield* workers
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 1000,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(subscribed);
      const engine = yield* OrchestrationEngineService;
      for (let index = 0; index < 40; index++) {
        yield* engine.dispatch({
          type: "thread.message.assistant.delta",
          commandId: CommandId.make(`stream-${index}`),
          threadId: workerThreadId,
          messageId: MessageId.make("stream-answer"),
          turnId,
          delta: "a",
          createdAt: NOW,
        });
      }
      yield* complete(workerThreadId, "stream-done");
      expect((yield* Fiber.join(waiter)).timedOut).toBe(false);
      // Initial lineage and lifecycle events are bounded independently of forty deltas.
      expect(yield* Ref.get(reads)).toBeLessThan(25);
      expect(yield* Ref.get(closed)).toBe(1);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect(
    "spawns ordinary durable threads with inherited checkout/runtime and bounded results",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const workers = yield* OwnedWorkers.OwnedWorkers;
        const { workerThreadId } = yield* spawn();
        const listed = yield* workers.list({ callerThreadId: ROOT });
        expect(listed.workers).toHaveLength(1);
        expect(listed.workers[0]).toMatchObject({
          threadId: workerThreadId,
          ownerThreadId: ROOT,
          rootThreadId: ROOT,
          depth: 1,
          status: "pending",
          runtimeMode: "approval-required",
          modelSelection: MODEL,
        });
        const snapshots = yield* ProjectionSnapshotQuery;
        const state = Option.getOrThrow(yield* snapshots.getWorkerState(workerThreadId));
        expect(state.thread.branch).toBe("main");
        expect(state.thread.worker?.mcpCapabilityCeiling).toEqual([
          "preview",
          "pull-requests",
          "workers",
        ]);
        yield* complete(workerThreadId);
        const result = yield* workers.get({ callerThreadId: ROOT, workerThreadId });
        expect(result.worker.status).toBe("completed");
        expect(result.worker.result?.assistantMessageId).toBe("turn:answer");
        expect(result.detail.page).toBeDefined();
        expect(result.detail.thread.messages.some((message) => message.text === "Done")).toBe(true);
      }).pipe(Effect.provide(testLayer())),
  );

  it.effect("intersects trusted MCP grants and rejects a caller without workers capability", () =>
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const result = yield* workers.spawn(
        {
          commandId: CommandId.make("narrow"),
          callerThreadId: ROOT,
          label: "Narrow",
          prompt: "Task",
          modelSelection: MODEL,
        },
        { mcpCapabilityCeiling: ["workers", "device"] },
      );
      const snapshots = yield* ProjectionSnapshotQuery;
      expect(
        Option.getOrThrow(yield* snapshots.getWorkerState(result.workerThreadId)).thread.worker
          ?.mcpCapabilityCeiling,
      ).toEqual(["workers"]);
      const denied = yield* workers
        .spawn(
          {
            commandId: CommandId.make("denied"),
            callerThreadId: ROOT,
            label: "Denied",
            prompt: "Task",
            modelSelection: MODEL,
          },
          { mcpCapabilityCeiling: ["preview"] },
        )
        .pipe(Effect.flip);
      expect(denied.code).toBe("forbidden");
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("limits admission atomically and rejects busy follow-ups", () =>
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const first = yield* spawn("one");
      expect(
        (yield* workers
          .send({
            commandId: CommandId.make("busy"),
            callerThreadId: ROOT,
            workerThreadId: first.workerThreadId,
            text: "Follow up",
          })
          .pipe(Effect.flip)).code,
      ).toBe("busy");
      yield* Effect.forEach(["two", "three", "four"], (id) => spawn(id), {
        concurrency: "unbounded",
      });
      expect((yield* spawn("five").pipe(Effect.flip)).code).toBe("limit");
      expect((yield* workers.list({ callerThreadId: ROOT })).workers).toHaveLength(4);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("scopes reads and controls to descendants and revokes orphan access", () =>
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const one = yield* spawn("one");
      const two = yield* spawn("two");
      const nested = yield* spawn("nested", one.workerThreadId);
      expect(
        (yield* workers.list({ callerThreadId: one.workerThreadId })).workers.map(
          (worker) => worker.threadId,
        ),
      ).toEqual([nested.workerThreadId]);
      for (const method of ["get", "stop", "send"] as const) {
        const input = {
          commandId: CommandId.make(`sibling:${method}`),
          callerThreadId: one.workerThreadId,
          workerThreadId: two.workerThreadId,
          text: "Forbidden",
        };
        const denied =
          method === "get"
            ? yield* workers.get(input).pipe(Effect.flip)
            : method === "stop"
              ? yield* workers.stop(input).pipe(Effect.flip)
              : yield* workers.send(input).pipe(Effect.flip);
        expect(denied.code).toBe("forbidden");
      }
      const engine = yield* OrchestrationEngineService;
      yield* engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-parent"),
        threadId: one.workerThreadId,
      });
      const snapshots = yield* ProjectionSnapshotQuery;
      const retained = Option.getOrThrow(yield* snapshots.getWorkerState(nested.workerThreadId));
      expect(retained.thread.worker?.stopRequestedAt).not.toBeNull();
      expect(retained.pendingMessageId).toBeNull();
      expect(
        (yield* workers
          .get({ callerThreadId: ROOT, workerThreadId: nested.workerThreadId })
          .pipe(Effect.flip)).code,
      ).toBe("not-found");
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("stops pending startup without permanently closing the worker", () =>
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const { workerThreadId } = yield* spawn();
      yield* workers.stop({
        commandId: CommandId.make("stop"),
        callerThreadId: ROOT,
        workerThreadId,
      });
      const stopped = yield* workers.get({ callerThreadId: ROOT, workerThreadId });
      expect(stopped.worker.status).toBe("stopped");
      yield* workers.send({
        commandId: CommandId.make("resume"),
        callerThreadId: ROOT,
        workerThreadId,
        text: "Continue",
      });
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "pending",
      );
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("wait subscribes before checking and completes through real engine events", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const workers = yield* Layer.build(observedWorkers(subscribed, closed)).pipe(
        Effect.map((context) => Context.get(context, OwnedWorkers.OwnedWorkers)),
      );
      const waiting = yield* workers
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 10_000,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(subscribed);
      yield* complete(workerThreadId);
      const result = yield* Fiber.join(waiting);
      expect(result.timedOut).toBe(false);
      expect(result.workers[0]?.status).toBe("completed");
      expect(yield* Ref.get(closed)).toBe(1);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("timeout and abort release subscriptions without polling", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const workers = yield* Layer.build(observedWorkers(subscribed, closed)).pipe(
        Effect.map((context) => Context.get(context, OwnedWorkers.OwnedWorkers)),
      );
      const waiting = yield* workers
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 100,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(subscribed);
      yield* TestClock.adjust("100 millis");
      expect((yield* Fiber.join(waiting)).timedOut).toBe(true);
      expect(yield* Ref.get(closed)).toBe(1);
      const abortedSubscribed = yield* Deferred.make<void>();
      const abortable = yield* Layer.build(observedWorkers(abortedSubscribed, closed)).pipe(
        Effect.map((context) => Context.get(context, OwnedWorkers.OwnedWorkers)),
      );
      const aborted = yield* abortable
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 10_000,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(abortedSubscribed);
      yield* Fiber.interrupt(aborted);
      expect(yield* Ref.get(closed)).toBe(2);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("native background work prevents a completed turn from satisfying wait", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      yield* complete(workerThreadId);
      const background = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
      background.recordTaskLiveness({
        threadId: workerThreadId,
        taskId: "native",
        taskType: "agent",
        status: "running",
        kind: "started",
      });
      const workers = yield* OwnedWorkers.OwnedWorkers;
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "running",
      );
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const observed = yield* Layer.build(observedWorkers(subscribed, closed)).pipe(
        Effect.map((context) => Context.get(context, OwnedWorkers.OwnedWorkers)),
      );
      const waiting = yield* observed
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 10_000,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(subscribed);
      background.recordTaskLiveness({
        threadId: workerThreadId,
        taskId: "native",
        taskType: "agent",
        status: "completed",
        kind: "completed",
      });
      const engine = yield* OrchestrationEngineService;
      yield* engine.dispatch({
        type: "thread.activity.append",
        commandId: CommandId.make("native-completed"),
        threadId: workerThreadId,
        createdAt: NOW,
        activity: {
          id: EventId.make("native-completed"),
          kind: "task.completed",
          payload: { taskId: "native", status: "completed" },
          turnId: TurnId.make("turn"),
          summary: "Native task completed",
          tone: "info",
          createdAt: NOW,
        },
      });
      expect((yield* Fiber.join(waiting)).timedOut).toBe(false);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("distinguishes any/all waits and rejects deleted targets while waiting", () =>
    Effect.gen(function* () {
      yield* setup;
      const first = yield* spawn("one");
      const second = yield* spawn("two");
      yield* complete(first.workerThreadId, "first-turn");
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const ids = [first.workerThreadId, second.workerThreadId];
      expect(
        (yield* workers.wait({
          callerThreadId: ROOT,
          workerThreadIds: ids,
          mode: "any",
          timeoutMs: 100,
        })).timedOut,
      ).toBe(false);
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const observed = yield* Layer.build(observedWorkers(subscribed, closed)).pipe(
        Effect.map((context) => Context.get(context, OwnedWorkers.OwnedWorkers)),
      );
      const waiting = yield* observed
        .wait({ callerThreadId: ROOT, workerThreadIds: ids, mode: "all", timeoutMs: 10_000 })
        .pipe(Effect.flip, Effect.forkChild);
      yield* Deferred.await(subscribed);
      const engine = yield* OrchestrationEngineService;
      yield* engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("delete-target"),
        threadId: second.workerThreadId,
      });
      expect((yield* Fiber.join(waiting)).code).toBe("not-found");
      expect(yield* Ref.get(closed)).toBe(1);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("reports failed startup instead of a permanently pending worker", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      yield* session(workerThreadId, "error", null, "startup-error");
      const workers = yield* OwnedWorkers.OwnedWorkers;
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "failed",
      );
      expect(
        (yield* workers.wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 100,
        })).timedOut,
      ).toBe(false);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect.each(["approval", "user-input"] as const)(
    "keeps failed startup waiting until its durable %s request resolves",
    (requestKind) =>
      Effect.gen(function* () {
        yield* setup;
        const { workerThreadId } = yield* spawn(`failed-pending-${requestKind}`);
        const engine = yield* OrchestrationEngineService;
        const workers = yield* OwnedWorkers.OwnedWorkers;
        const snapshots = yield* ProjectionSnapshotQuery;
        const requestId = `failed-pending-${requestKind}:request`;
        const activity = (phase: "requested" | "resolved") =>
          engine.dispatch({
            type: "thread.activity.append",
            commandId: CommandId.make(`${requestId}:${phase}`),
            threadId: workerThreadId,
            createdAt: NOW,
            activity: {
              id: EventId.make(`${requestId}:${phase}`),
              kind: `${requestKind}.${phase}`,
              summary: phase === "requested" ? "Input needed" : "Input resolved",
              tone: requestKind === "approval" ? "approval" : "info",
              turnId: null,
              createdAt: NOW,
              payload: {
                requestId,
                requestKind: "command",
                ...(phase === "requested"
                  ? {
                      questions: [
                        { id: "question", header: "Question", question: "Proceed?", options: [] },
                      ],
                    }
                  : { decision: "approved" }),
              },
            },
          });
        yield* activity("requested");
        yield* session(workerThreadId, "error", null, `${requestId}:session-error`);
        expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
          "waiting",
        );
        expect(
          (yield* snapshots.getWorkerState(workerThreadId)).pipe(Option.getOrThrow)
            .pendingMessageId,
        ).toBeNull();
        const subscribed = yield* Deferred.make<void>();
        const checked = yield* Deferred.make<void>();
        const closed = yield* Ref.make(0);
        const observed = Context.get(
          yield* Layer.build(observedWorkers(subscribed, closed)).pipe(
            Effect.provideService(ProjectionSnapshotQuery, {
              ...snapshots,
              getWorkerState: (id) =>
                snapshots
                  .getWorkerState(id)
                  .pipe(
                    Effect.tap((state) =>
                      id === workerThreadId &&
                      Option.isSome(state) &&
                      state.value.thread.session?.status === "error"
                        ? Deferred.succeed(checked, undefined)
                        : Effect.void,
                    ),
                  ),
            }),
          ),
          OwnedWorkers.OwnedWorkers,
        );
        const settled = yield* Ref.make(false);
        const waiting = yield* observed
          .wait({
            callerThreadId: ROOT,
            workerThreadIds: [workerThreadId],
            mode: "all",
            timeoutMs: 10_000,
          })
          .pipe(
            Effect.tap(() => Ref.set(settled, true)),
            Effect.forkChild,
          );
        yield* Deferred.await(subscribed);
        yield* Deferred.await(checked);
        expect(yield* Ref.get(settled)).toBe(false);
        expect(yield* Ref.get(closed)).toBe(0);
        expect(
          (yield* workers
            .send({
              commandId: CommandId.make(`${requestId}:send`),
              callerThreadId: ROOT,
              workerThreadId,
              text: "Continue",
            })
            .pipe(Effect.flip)).code,
        ).toBe("busy");
        yield* activity("resolved");
        const result = yield* Fiber.join(waiting);
        expect(result.timedOut).toBe(false);
        expect(result.workers[0]?.status).toBe("failed");
        expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
          "failed",
        );
        expect(yield* Ref.get(closed)).toBe(1);
      }).pipe(Effect.provide(testLayer())),
  );

  it.effect("reports approval and input blocked work as waiting, not completed", () =>
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const engine = yield* OrchestrationEngineService;
      for (const kind of ["approval.requested", "user-input.requested"] as const) {
        const { workerThreadId } = yield* spawn(kind);
        yield* session(workerThreadId, "running", TurnId.make(kind), `${kind}:running`);
        yield* engine.dispatch({
          type: "thread.activity.append",
          commandId: CommandId.make(`${kind}:request`),
          threadId: workerThreadId,
          createdAt: NOW,
          activity: {
            id: EventId.make(`${kind}:request`),
            kind,
            summary: "Input needed",
            tone: kind === "approval.requested" ? "approval" : "info",
            turnId: TurnId.make(kind),
            createdAt: NOW,
            payload: {
              requestId: `${kind}:request`,
              requestKind: "command",
              questions: [
                { id: "question", header: "Question", question: "Proceed?", options: [] },
              ],
            },
          },
        });
        expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
          "waiting",
        );
      }
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("validates bounded inputs even for direct service callers", () =>
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      expect(
        (yield* workers
          .wait({ callerThreadId: ROOT, workerThreadIds: [], mode: "all", timeoutMs: 1 })
          .pipe(Effect.flip)).code,
      ).toBe("invalid-input");
      expect(
        (yield* workers
          .get({ callerThreadId: ROOT, workerThreadId: ROOT, turnLimit: 21 })
          .pipe(Effect.flip)).code,
      ).toBe("invalid-input");
      expect(
        (yield* workers
          .spawn({
            commandId: CommandId.make("empty"),
            callerThreadId: ROOT,
            label: "Worker",
            prompt: "",
            modelSelection: MODEL,
          })
          .pipe(Effect.flip)).code,
      ).toBe("invalid-input");
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("rejects unresolved scratch owners and inherits resolved scratch folders", () =>
    Effect.gen(function* () {
      const config = yield* ServerConfig;
      const engine = yield* OrchestrationEngineService;
      const scratch = NodePath.resolve(config.baseDir, "scratch");
      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("scratch-project"),
        projectId: PROJECT,
        title: "Scratch",
        workspaceRoot: scratch,
        createdAt: NOW,
      });
      yield* engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("scratch-owner"),
        threadId: ROOT,
        projectId: PROJECT,
        title: "Owner",
        modelSelection: MODEL,
        runtimeMode: "approval-required",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        createdAt: NOW,
      });
      expect((yield* spawn("unresolved").pipe(Effect.flip)).code).toBe("invalid-input");
      yield* engine.dispatch({
        type: "thread.meta.update",
        commandId: CommandId.make("resolve-scratch"),
        threadId: ROOT,
        worktreePath: NodePath.join(scratch, "owner"),
      });
      const result = yield* spawn("resolved");
      const snapshots = yield* ProjectionSnapshotQuery;
      expect(
        Option.getOrThrow(yield* snapshots.getWorkerState(result.workerThreadId)).thread
          .worktreePath,
      ).toBe(NodePath.join(scratch, "owner"));
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("keeps exact spawn retries idempotent when server capabilities narrow", () =>
    Effect.gen(function* () {
      yield* setup;
      const original = yield* spawn();
      const settings = yield* ServerSettings.ServerSettingsService;
      yield* settings.updateSettings({ enableAgentBrowserAccess: false });
      expect(yield* spawn()).toEqual(original);
      const workers = yield* OwnedWorkers.OwnedWorkers;
      expect((yield* workers.list({ callerThreadId: ROOT })).workers).toHaveLength(1);
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("fresh pending retries outrank stale startup error and reserve admission", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      yield* session(workerThreadId, "error", null, "failed-start");
      const workers = yield* OwnedWorkers.OwnedWorkers;
      yield* workers.send({
        commandId: CommandId.make("retry"),
        callerThreadId: ROOT,
        workerThreadId,
        text: "Retry",
      });
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "pending",
      );
      expect(
        (yield* workers
          .send({
            commandId: CommandId.make("busy-retry"),
            callerThreadId: ROOT,
            workerThreadId,
            text: "Cannot overlap",
          })
          .pipe(Effect.flip)).code,
      ).toBe("busy");
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const observed = Context.get(
        yield* Layer.build(observedWorkers(subscribed, closed)),
        OwnedWorkers.OwnedWorkers,
      );
      const waiting = yield* observed
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 100,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(subscribed);
      yield* TestClock.adjust("100 millis");
      const result = yield* Fiber.join(waiting);
      expect(result.timedOut).toBe(true);
      expect(result.workers[0]?.status).toBe("pending");
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("send retries reject changed text and send/stop command collisions", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      const workers = yield* OwnedWorkers.OwnedWorkers;
      yield* workers.stop({
        commandId: CommandId.make("initial-stop"),
        callerThreadId: ROOT,
        workerThreadId,
      });
      const input = {
        commandId: CommandId.make("followup"),
        callerThreadId: ROOT,
        workerThreadId,
        text: "First",
      };
      const sent = yield* workers.send(input);
      expect(yield* workers.send(input)).toEqual(sent);
      expect((yield* workers.send({ ...input, text: "Changed" }).pipe(Effect.flip)).code).toBe(
        "conflict",
      );
      expect((yield* workers.stop(input).pipe(Effect.flip)).code).toBe("conflict");
      expect(yield* workers.send(input)).toEqual(sent);
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "pending",
      );
      yield* workers.stop({ ...input, commandId: CommandId.make("actual-stop") });
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "stopped",
      );
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect("idle-session stop remains stopping until durable provider acknowledgement", () =>
    Effect.gen(function* () {
      yield* setup;
      const { workerThreadId } = yield* spawn();
      yield* complete(workerThreadId);
      const workers = yield* OwnedWorkers.OwnedWorkers;
      yield* workers.stop({
        commandId: CommandId.make("stop-idle"),
        callerThreadId: ROOT,
        workerThreadId,
      });
      const stopping = yield* workers.get({ callerThreadId: ROOT, workerThreadId });
      expect(stopping.worker.status).toBe("stopping");
      expect(stopping.worker.result).toBeNull();
      expect(
        (yield* workers
          .send({
            commandId: CommandId.make("before-ack"),
            callerThreadId: ROOT,
            workerThreadId,
            text: "Too soon",
          })
          .pipe(Effect.flip)).code,
      ).toBe("busy");
      const subscribed = yield* Deferred.make<void>();
      const closed = yield* Ref.make(0);
      const observed = Context.get(
        yield* Layer.build(observedWorkers(subscribed, closed)),
        OwnedWorkers.OwnedWorkers,
      );
      const waiting = yield* observed
        .wait({
          callerThreadId: ROOT,
          workerThreadIds: [workerThreadId],
          mode: "all",
          timeoutMs: 10000,
        })
        .pipe(Effect.forkChild);
      yield* Deferred.await(subscribed);
      yield* session(workerThreadId, "stopped", null, "provider-stop-ack");
      expect((yield* Fiber.join(waiting)).workers[0]?.status).toBe("stopped");
      yield* workers.send({
        commandId: CommandId.make("after-ack"),
        callerThreadId: ROOT,
        workerThreadId,
        text: "Continue",
      });
      expect((yield* workers.get({ callerThreadId: ROOT, workerThreadId })).worker.status).toBe(
        "pending",
      );
    }).pipe(Effect.provide(testLayer())),
  );

  it.effect(
    "worker detail excludes lifetime plans and checkpoints while bounding conversation",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const { workerThreadId } = yield* spawn();
        const workers = yield* OwnedWorkers.OwnedWorkers;
        const engine = yield* OrchestrationEngineService;
        for (let turn = 1; turn <= 3; turn += 1) {
          if (turn > 1)
            yield* workers.send({
              commandId: CommandId.make(`prompt:${turn}`),
              callerThreadId: ROOT,
              workerThreadId,
              text: `Prompt ${turn}`,
            });
          yield* complete(workerThreadId, `history:${turn}`, turn);
          yield* engine.dispatch({
            type: "thread.proposed-plan.upsert",
            commandId: CommandId.make(`plan:${turn}`),
            threadId: workerThreadId,
            createdAt: NOW,
            proposedPlan: {
              id: `plan:${turn}`,
              turnId: TurnId.make(`history:${turn}`),
              planMarkdown: `Plan ${turn}`,
              implementedAt: NOW,
              implementationThreadId: workerThreadId,
              createdAt: NOW,
              updatedAt: NOW,
            },
          });
        }
        const snapshots = yield* ProjectionSnapshotQuery;
        const ordinary = Option.getOrThrow(
          yield* snapshots.getThreadDetailSnapshot(workerThreadId, { turnLimit: 1 }),
        );
        expect(ordinary.thread.proposedPlans).toHaveLength(3);
        expect(ordinary.thread.checkpoints).toHaveLength(3);
        const bounded = yield* workers.get({ callerThreadId: ROOT, workerThreadId, turnLimit: 1 });
        expect(bounded.detail.thread.proposedPlans).toEqual([]);
        expect(bounded.detail.thread.checkpoints).toEqual([]);
        expect(
          bounded.detail.thread.messages.filter((message) => message.role === "user"),
        ).toHaveLength(1);
        expect(bounded.worker.result?.assistantMessageId).toBe("history:3:answer");
      }).pipe(Effect.provide(testLayer())),
  );

  it.effect("replays spawn receipts across restart and rejects changed retries", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-owned-workers-" });
        const path = NodePath.join(directory, "state.sqlite");
        const original = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* setup;
            const original = yield* spawn();
            yield* complete(original.workerThreadId);
            const workers = yield* OwnedWorkers.OwnedWorkers;
            yield* workers.send({
              commandId: CommandId.make("stranded-followup"),
              callerThreadId: ROOT,
              workerThreadId: original.workerThreadId,
              text: "Follow up before restart",
            });
            expect(
              (yield* workers.get({
                callerThreadId: ROOT,
                workerThreadId: original.workerThreadId,
              })).worker.result,
            ).toBeNull();
            const pendingSpawn = yield* spawn("never-started");
            const stopping = yield* spawn("ready-stop");
            yield* complete(stopping.workerThreadId, "ready-stop-turn");
            const stopReceipt = yield* workers.stop({
              commandId: CommandId.make("ready-stop-command"),
              callerThreadId: ROOT,
              workerThreadId: stopping.workerThreadId,
            });
            expect(
              (yield* workers.get({
                callerThreadId: ROOT,
                workerThreadId: stopping.workerThreadId,
              })).worker.status,
            ).toBe("stopping");
            return {
              ...original,
              pendingSpawnId: pendingSpawn.workerThreadId,
              stoppingId: stopping.workerThreadId,
              stopReceipt,
            };
          }).pipe(Effect.provide(testLayer(path))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const provider = ProviderService.of({
              startSession: () => Effect.die("Unexpected provider start"),
              sendTurn: () => Effect.die("Unexpected provider turn"),
              compactThread: () => Effect.die("Unexpected provider compact"),
              interruptTurn: () => Effect.die("Unexpected provider interrupt"),
              respondToRequest: () => Effect.die("Unexpected provider approval"),
              respondToUserInput: () => Effect.die("Unexpected provider input"),
              stopSession: () => Effect.die("Unexpected provider stop"),
              listSessions: () => Effect.succeed([]),
              getCapabilities: () => Effect.die("Unexpected provider capabilities"),
              getInstanceInfo: () => Effect.die("Unexpected provider routing"),
              assertConversationRollbackSupported: () => Effect.die("Unexpected provider rollback"),
              rollbackConversation: () => Effect.die("Unexpected provider rollback"),
              uploadFeedback: () => Effect.die("Unexpected provider upload"),
              streamEvents: Stream.empty,
            });
            const beforeRecovery = yield* OwnedWorkers.OwnedWorkers;
            const listFailure = yield* reconcileWorkerPendingStarts.pipe(
              Effect.provideService(ProviderService, {
                ...provider,
                listSessions: () => Effect.die("Native listing failed"),
              }),
              Effect.exit,
            );
            expect(Exit.isFailure(listFailure)).toBe(true);
            expect(
              (yield* beforeRecovery.get({
                callerThreadId: ROOT,
                workerThreadId: original.stoppingId,
              })).worker.status,
            ).toBe("stopping");
            const stopFailure = yield* reconcileWorkerPendingStarts.pipe(
              Effect.provideService(ProviderService, {
                ...provider,
                listSessions: () =>
                  Effect.succeed([
                    {
                      provider: ProviderDriverKind.make("codex"),
                      status: "ready",
                      runtimeMode: "approval-required",
                      threadId: original.stoppingId,
                      createdAt: NOW,
                      updatedAt: NOW,
                    },
                  ]),
              }),
              Effect.exit,
            );
            expect(Exit.isFailure(stopFailure)).toBe(true);
            expect(
              (yield* beforeRecovery.get({
                callerThreadId: ROOT,
                workerThreadId: original.stoppingId,
              })).worker.status,
            ).toBe("stopping");
            yield* reconcileWorkerPendingStarts.pipe(
              Effect.provideService(ProviderService, provider),
            );
            expect(yield* spawn()).toEqual({
              workerThreadId: original.workerThreadId,
              sequence: original.sequence,
            });
            const workers = yield* OwnedWorkers.OwnedWorkers;
            const recovered = yield* workers.get({
              callerThreadId: ROOT,
              workerThreadId: original.workerThreadId,
            });
            expect(recovered.worker.status).toBe("interrupted");
            expect(recovered.worker.pendingMessageId).toBeNull();
            expect(recovered.worker.result).toBeNull();
            expect(
              (yield* workers.get({
                callerThreadId: ROOT,
                workerThreadId: original.pendingSpawnId,
              })).worker.status,
            ).toBe("interrupted");
            const changed = yield* workers
              .spawn({
                commandId: CommandId.make("spawn"),
                callerThreadId: ROOT,
                label: "spawn",
                prompt: "Changed task",
                modelSelection: MODEL,
              })
              .pipe(Effect.flip);
            expect(changed.code).toBe("conflict");
            expect(yield* spawn()).toEqual({
              workerThreadId: original.workerThreadId,
              sequence: original.sequence,
            });
            expect((yield* workers.list({ callerThreadId: ROOT })).workers).toHaveLength(3);
            expect(
              (yield* workers.get({ callerThreadId: ROOT, workerThreadId: original.stoppingId }))
                .worker.status,
            ).toBe("stopped");
            expect(
              yield* workers.stop({
                commandId: CommandId.make("ready-stop-command"),
                callerThreadId: ROOT,
                workerThreadId: original.stoppingId,
              }),
            ).toEqual(original.stopReceipt);
            yield* workers.send({
              commandId: CommandId.make("ready-stop-reuse"),
              callerThreadId: ROOT,
              workerThreadId: original.stoppingId,
              text: "Resume recovered stop",
            });
            expect(
              (yield* workers.get({ callerThreadId: ROOT, workerThreadId: original.stoppingId }))
                .worker.status,
            ).toBe("pending");
            yield* workers.send({
              commandId: CommandId.make("after-restart"),
              callerThreadId: ROOT,
              workerThreadId: original.workerThreadId,
              text: "Continue after recovery",
            });
            expect(
              (yield* workers.get({
                callerThreadId: ROOT,
                workerThreadId: original.workerThreadId,
              })).worker.status,
            ).toBe("pending");
          }).pipe(Effect.provide(testLayer(path))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  );
});
