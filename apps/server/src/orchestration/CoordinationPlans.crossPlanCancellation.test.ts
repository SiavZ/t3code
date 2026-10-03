import {
  CheckpointRef,
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
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
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "../mcp/McpSessionRegistry.ts";
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
import { NativeUnattendedActivation } from "./nativeUnattendedAuthority.ts";

const ROOT = ThreadId.make("cross-plan-root");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const NOW = "2026-10-03T00:00:00.000Z";
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
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cross-plan-test-" })),
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

const target = (planId: string) => ({ callerThreadId: ROOT, rootThreadId: ROOT, planId });
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Completed isolated task",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Provider execution is outside this fixture" },
  unchecked: [],
  confidence: "medium",
  outcome: "completed",
};
const runPlan = Effect.fn(function* (planId: string, authority: ThreadUnattendedAuthority) {
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  let plan = yield* plans.write(
    {
      ...target(planId),
      commandId: CommandId.make(`create:${planId}`),
      expectedRevision: 0,
      operation: "create",
      policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
      nodes: [
        {
          id: "task",
          kind: "work",
          prompt: `Implement ${planId}`,
          dependsOn: [],
          gateScope: [],
          attemptLimit: 3,
          modelSelection: MODEL,
        },
      ],
    },
    authority,
  );
  plan = yield* plans.write({
    ...target(planId),
    commandId: CommandId.make(`run:${planId}`),
    expectedRevision: plan.revision,
    operation: "run",
  });
  yield* reactor.drain(ROOT, planId);
  return yield* plans.read(target(planId));
});
const setSession = Effect.fn(function* (plan: CoordinationPlan, turnId: TurnId | null) {
  const engine = yield* OrchestrationEngineService;
  const threadId = plan.nodes[0]!.attempts[0]!.workerThreadId;
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(`${turnId ? "start" : "idle"}:${plan.id}`),
    threadId,
    createdAt: NOW,
    session: {
      threadId,
      status: turnId ? "running" : "ready",
      activeTurnId: turnId,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
});
const settle = Effect.fn(function* (plan: CoordinationPlan, turnId: TurnId) {
  const engine = yield* OrchestrationEngineService;
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  const attempt = plan.nodes[0]!.attempts[0]!;
  yield* plans.write({
    ...target(plan.id),
    callerThreadId: attempt.workerThreadId,
    commandId: CommandId.make(`report:${plan.id}`),
    expectedRevision: plan.revision,
    operation: "complete",
    nodeId: "task",
    attemptNumber: attempt.number,
    workerThreadId: attempt.workerThreadId,
    turnId,
    artifact: { ...artifact, summary: `Completed ${plan.id}` },
  });
  yield* setSession(plan, null);
  yield* engine.dispatch({
    type: "thread.turn.diff.complete",
    commandId: CommandId.make(`finish:${plan.id}`),
    threadId: attempt.workerThreadId,
    turnId,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${plan.id}`),
    status: "ready",
    files: [],
    checkpointTurnCount: plan.id === "A" ? 1 : 2,
    createdAt: NOW,
  });
  yield* reactor.drain(ROOT, plan.id);
  return yield* plans.read(target(plan.id));
});

// No provider reactor or listening HTTP server is installed. Native lifecycle acknowledgements
// enter through the real engine, while all tables/projections are the migrated SQLite ones.
it.layer(testLayer)("cross-plan retained-worker cancellation", (it) => {
  it.effect("cancels settled A without stopping or changing the retained worker running B", () =>
    Effect.gen(function* () {
      const engine = yield* OrchestrationEngineService;
      const plans = yield* Plans.CoordinationPlans;
      const reactor = yield* Reactor.CoordinationReactor;
      const query = yield* ProjectionSnapshotQuery;
      const sql = yield* SqlClient.SqlClient;
      const grants = yield* Grants.UnattendedGrants;
      yield* engine.dispatch({
        type: "project.create",
        commandId: CommandId.make("cross-project"),
        projectId: ProjectId.make("cross-project"),
        title: "Cross-plan acceptance",
        workspaceRoot: "/workspace/cross-plan",
        createdAt: NOW,
      });
      yield* engine.dispatch({
        type: "thread.create",
        commandId: CommandId.make("cross-root"),
        threadId: ROOT,
        projectId: ProjectId.make("cross-project"),
        title: "Root",
        modelSelection: MODEL,
        runtimeMode: "approval-required",
        interactionMode: "default",
        branch: "main",
        worktreePath: null,
        createdAt: NOW,
      });
      const grant = yield* grants.create(
        {
          id: "cross-plan-grant",
          callerThreadId: ROOT,
          ceiling: { runtimeMode: "approval-required", mcpCapabilities: ["workers"] },
          hostJobs: false,
        },
        { source: "client" },
      );
      const authority: ThreadUnattendedAuthority = {
        grantId: grant.id,
        grantRevision: grant.revision,
        ownerThreadId: ROOT,
        runtimeModeCeiling: grant.ceiling.runtimeMode,
        mcpCapabilityCeiling: grant.ceiling.mcpCapabilities,
      };
      let a = yield* runPlan("A", authority);
      const aAttempt = a.nodes[0]!.attempts[0]!;
      const aTurn = TurnId.make("native-A");
      yield* setSession(a, aTurn);
      a = yield* settle(a, aTurn);
      expect(a.nodes[0]!.attempts[0]!).toMatchObject({ status: "succeeded", turnId: aTurn });
      expect(
        Option.getOrThrow(yield* query.getWorkerState(aAttempt.workerThreadId)).thread.worker
          ?.stopRequestedAt,
      ).toBeNull();

      let b = yield* runPlan("B", authority);
      const bAttempt = b.nodes[0]!.attempts[0]!;
      expect(bAttempt.workerThreadId).toBe(aAttempt.workerThreadId);
      expect(bAttempt.dispatchMessageId).not.toBe(aAttempt.dispatchMessageId);
      const worker = bAttempt.workerThreadId;
      const bTurn = TurnId.make("native-B");
      yield* setSession(b, bTurn);
      const bindings = yield* sql<{
        turn_id: string;
        pending_message_id: string;
      }>`SELECT turn_id, pending_message_id FROM projection_turns WHERE thread_id = ${worker} ORDER BY turn_id`;
      expect(bindings).toEqual([
        { turn_id: aTurn, pending_message_id: aAttempt.dispatchMessageId },
        { turn_id: bTurn, pending_message_id: bAttempt.dispatchMessageId },
      ]);
      const activationBefore = yield* sql<{
        message_id: string;
        event_sequence: number;
        authority_json: string;
      }>`SELECT message_id, event_sequence, authority_json FROM projection_thread_activation_authorities WHERE thread_id = ${worker}`;
      expect(activationBefore).toHaveLength(1);
      expect(activationBefore[0]!.message_id).toBe(bAttempt.dispatchMessageId);
      expect(JSON.parse(activationBefore[0]!.authority_json)).toEqual(authority);
      const registry = yield* McpSessionRegistry.__testing.make({ now: () => 1_000 }).pipe(
        Effect.provideService(
          HttpServer.HttpServer,
          HttpServer.HttpServer.of({
            address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
            serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
          }),
        ),
        Effect.provideService(
          ServerEnvironment,
          ServerEnvironment.of({
            getEnvironmentId: Effect.succeed(EnvironmentId.make("cross-environment")),
            getDescriptor: Effect.die("unused"),
          }),
        ),
      );
      const credential = yield* registry
        .issue({
          threadId: worker,
          providerInstanceId: MODEL.instanceId,
          capabilities: new Set(["workers"]),
        })
        .pipe(
          Effect.provideService(NativeUnattendedActivation, {
            messageId: bAttempt.dispatchMessageId,
            sequence: activationBefore[0]!.event_sequence,
            authority,
          }),
        );
      const token = credential.config.authorizationHeader.slice("Bearer ".length);
      const credentialBefore = yield* registry.resolve(token);
      expect(credentialBefore).toMatchObject({ threadId: worker, unattendedAuthority: authority });
      expect([...credentialBefore!.capabilities]).toEqual(["workers"]);
      const stateBefore = yield* query.getWorkerState(worker);
      expect(Option.getOrThrow(stateBefore)).toMatchObject({
        pendingMessageId: null,
        thread: {
          latestTurn: { turnId: bTurn, state: "running" },
          session: { status: "running", activeTurnId: bTurn },
          worker: { stopRequestedAt: null },
        },
      });
      const beforeCancel = yield* engine.latestSequence;
      a = yield* plans.write({
        ...target("A"),
        commandId: CommandId.make("cancel:A"),
        expectedRevision: a.revision,
        operation: "cancel",
      });
      yield* reactor.drain(ROOT);
      expect(a.cancelled).toBe(true);
      expect(a.executionAuthority).toEqual(authority);
      const cancelEvents = yield* Stream.runCollect(engine.readEvents(beforeCancel));
      expect(
        cancelEvents.some(
          (event) =>
            event.type === "coordination.plan.updated" &&
            event.payload.plan.id === "A" &&
            event.payload.plan.cancelled,
        ),
      ).toBe(true);
      expect(cancelEvents.some((event) => event.type === "thread.session-stop-requested")).toBe(
        false,
      );
      expect(cancelEvents.some((event) => event.type === "thread.turn-interrupt-requested")).toBe(
        false,
      );
      expect(yield* query.getWorkerState(worker)).toEqual(stateBefore);
      expect(
        yield* sql`SELECT message_id, event_sequence, authority_json FROM projection_thread_activation_authorities WHERE thread_id = ${worker}`,
      ).toEqual(activationBefore);
      expect(yield* registry.resolve(token)).toEqual(credentialBefore);
      expect(yield* grants.get({ id: grant.id, callerThreadId: ROOT })).toEqual(grant);
      expect(yield* plans.read(target("B"))).toEqual(b);

      b = yield* settle(b, bTurn);
      expect(b.cancelled).toBe(false);
      expect(b.nodes[0]!.attempts[0]!).toMatchObject({
        status: "succeeded",
        workerThreadId: worker,
        dispatchMessageId: bAttempt.dispatchMessageId,
        turnId: bTurn,
        artifact: { summary: "Completed B" },
      });
      expect(b.nodes[0]!.attempts[0]!.turnId).not.toBe(aTurn);
      expect(b.executionAuthority).toEqual(authority);
    }),
  );
});
