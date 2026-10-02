import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as ServerConfig from "./config.ts";
import { OrchestrationEngineLive } from "./orchestration/Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./orchestration/Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./orchestration/Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "./orchestration/ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./orchestration/ThreadPlanProgress.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "./persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "./persistence/Layers/OrchestrationEventStore.ts";
import { SqlitePersistenceMemory } from "./persistence/Layers/Sqlite.ts";
import { RepositoryIdentityResolver } from "./project/RepositoryIdentityResolver.ts";
import { ProviderService } from "./provider/Services/ProviderService.ts";
import { reconcileWorkerPendingStarts } from "./serverRuntimeStartup.ts";

const NOW = "2026-10-02T00:00:00.000Z";
const ROOT = ThreadId.make("root");
const WORKER = ThreadId.make("worker");
const PROJECT = ProjectId.make("project");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const testLayer = Layer.mergeAll(
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
  Layer.provide(Layer.succeed(RepositoryIdentityResolver, { resolve: () => Effect.succeed(null) })),
  Layer.provide(SqlitePersistenceMemory),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-worker-stop-startup-" })),
  Layer.provideMerge(NodeServices.layer),
);

it.effect(
  "startup waits for native idle-worker cancellation before acknowledging a durable stop",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const engine = yield* OrchestrationEngineService;
        const query = yield* ProjectionSnapshotQuery;
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
        yield* engine.dispatch({
          type: "thread.worker.spawn",
          commandId: CommandId.make("spawn"),
          threadId: WORKER,
          callerThreadId: ROOT,
          label: "worker",
          prompt: "Task",
          modelSelection: MODEL,
          mcpCapabilityCeiling: [],
          spawnFingerprint: "fingerprint",
          createdAt: NOW,
        });
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("ready"),
          threadId: WORKER,
          session: {
            threadId: WORKER,
            status: "ready",
            providerName: "codex",
            runtimeMode: "approval-required",
            activeTurnId: null,
            lastError: null,
            updatedAt: NOW,
          },
          createdAt: NOW,
        });
        yield* engine.dispatch({
          type: "thread.worker.stop",
          commandId: CommandId.make("stop"),
          callerThreadId: ROOT,
          threadId: WORKER,
          createdAt: NOW,
        });
        const stopping = yield* Deferred.make<void>();
        const stopped = yield* Deferred.make<void>();
        const native = {
          startSession: () => Effect.die("must not resume"),
          sendTurn: () => Effect.die("must not send"),
          compactThread: () => Effect.die("unused"),
          interruptTurn: () => Effect.die("unused"),
          respondToRequest: () => Effect.die("unused"),
          respondToUserInput: () => Effect.die("unused"),
          stopSession: ({ threadId }) =>
            Effect.gen(function* () {
              assert.equal(threadId, WORKER);
              yield* Deferred.succeed(stopping, undefined);
              yield* Deferred.await(stopped);
            }),
          listSessions: () =>
            Effect.succeed([
              {
                threadId: WORKER,
                provider: ProviderDriverKind.make("codex"),
                status: "ready",
                runtimeMode: "approval-required",
                createdAt: NOW,
                updatedAt: NOW,
              },
            ]),
          getCapabilities: () => Effect.die("unused"),
          getInstanceInfo: () => Effect.die("unused"),
          assertConversationRollbackSupported: () => Effect.die("unused"),
          rollbackConversation: () => Effect.die("unused"),
          uploadFeedback: () => Effect.die("unused"),
          streamEvents: Stream.empty,
        } satisfies ProviderService["Service"];
        const fiber = yield* reconcileWorkerPendingStarts.pipe(
          Effect.provideService(ProviderService, native),
          Effect.forkScoped,
        );
        yield* Deferred.await(stopping);
        assert.equal(
          Option.getOrThrow(yield* query.getWorkerState(WORKER)).thread.session?.status,
          "ready",
        );
        yield* Deferred.succeed(stopped, undefined);
        yield* Fiber.join(fiber);
        assert.equal(
          Option.getOrThrow(yield* query.getWorkerState(WORKER)).thread.session?.status,
          "stopped",
        );
      }),
    ).pipe(Effect.provide(testLayer)),
);
