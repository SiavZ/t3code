import * as NodeServices from "@effect/platform-node/NodeServices";
import { describe, expect, it } from "@effect/vitest";
import {
  CheckpointRef,
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ServerConfig } from "../config.ts";
import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import * as Invocation from "../mcp/McpInvocationContext.ts";
import * as Registry from "../mcp/McpSessionRegistry.ts";
import { WorkersToolkit } from "../mcp/toolkits/workers/tools.ts";
import { WorkersToolkitHandlersLive } from "../mcp/toolkits/workers/handlers.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationCommandReceiptRepository } from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as Settings from "../serverSettings.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { NativeUnattendedActivation } from "./nativeUnattendedAuthority.ts";
import * as Plans from "./CoordinationPlans.ts";
import * as Store from "./CoordinationPlanStore.ts";
import * as Reactor from "./CoordinationReactor.ts";
import * as Workers from "./OwnedWorkers.ts";
import * as Liveness from "./ThreadBackgroundLiveness.ts";
import * as Progress from "./ThreadPlanProgress.ts";

const ROOT = ThreadId.make("reuse-root");
const PROJECT = ProjectId.make("reuse-project");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const NOW = "2026-10-03T00:00:00.000Z";
const G: ThreadUnattendedAuthority = {
  grantId: "reuse-G",
  grantRevision: 1,
  ownerThreadId: ROOT,
  runtimeModeCeiling: "approval-required",
  mcpCapabilityCeiling: ["workers"],
};
const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "reuse-plan" };
const core = Layer.mergeAll(
  OrchestrationEngineLive.pipe(
    Layer.provide(OrchestrationProjectionSnapshotQueryLive),
    Layer.provide(OrchestrationProjectionPipelineLive),
  ),
  OrchestrationProjectionSnapshotQueryLive,
).pipe(
  Layer.provideMerge(Liveness.layer),
  Layer.provide(Progress.layer),
  Layer.provide(OrchestrationEventStoreLive),
  Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
  Layer.provide(
    Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
      resolve: () => Effect.succeed(null),
    }),
  ),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-public-reuse-" })),
  Layer.provideMerge(NodeServices.layer),
  Layer.provideMerge(Settings.layerTest({})),
);
const testLayer = Layer.mergeAll(
  Plans.layer.pipe(Layer.provide(Store.layer)),
  Reactor.layer,
  Workers.layer,
).pipe(Layer.provideMerge(core));

const makeRegistry = Registry.__testing.make({ now: () => 1_000 }).pipe(
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
      getEnvironmentId: Effect.succeed(EnvironmentId.make("reuse-environment")),
      getDescriptor: Effect.die("unused"),
    }),
  ),
);
const issue = (registry: Registry.McpSessionRegistry["Service"]) =>
  registry.issue({
    threadId: ROOT,
    providerInstanceId: MODEL.instanceId,
    capabilities: new Set(["workers"]),
  });

// Real persisted C9 settlement: revocation suppresses the report continuation and
// interrupts the graph assignment. No provider runtime or hand-edited graph state.
const prepare = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  const query = yield* ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("reuse-project"),
    projectId: PROJECT,
    title: "Reuse",
    workspaceRoot: "/workspace/reuse",
    createdAt: NOW,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make("reuse-root"),
    threadId: ROOT,
    projectId: PROJECT,
    title: "Root",
    modelSelection: MODEL,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    createdAt: NOW,
  });
  yield* sql`INSERT INTO unattended_grants (grant_id,owner_thread_id,project_id,revision,revoked,ceiling_json,created_at)
    VALUES (${G.grantId},${ROOT},${PROJECT},1,0,${JSON.stringify({ runtimeMode: G.runtimeModeCeiling, mcpCapabilities: G.mcpCapabilityCeiling })},${NOW})`;
  yield* engine.dispatch({
    type: "thread.turn.start",
    commandId: CommandId.make("root-G"),
    threadId: ROOT,
    message: { messageId: MessageId.make("root-G"), role: "user", text: "G", attachments: [] },
    modelSelection: MODEL,
    runtimeMode: "approval-required",
    interactionMode: "default",
    unattendedAuthority: G,
    createdAt: NOW,
  });
  const registry = yield* makeRegistry;
  const originRows = yield* sql<{
    message_id: string;
    event_sequence: number;
  }>`SELECT message_id,event_sequence FROM projection_thread_activation_authorities WHERE thread_id=${ROOT}`;
  const retained = yield* issue(registry).pipe(
    Effect.provideService(NativeUnattendedActivation, {
      messageId: MessageId.make(originRows[0]!.message_id),
      sequence: originRows[0]!.event_sequence,
      authority: G,
    }),
  );
  expect(
    (yield* registry.resolve(retained.config.authorizationHeader.slice(7)))?.unattendedAuthority,
  ).toEqual(G);
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
    operation: "create",
    commandId: CommandId.make("reuse-create"),
    expectedRevision: 0,
    policy: { mode: "deep", maxConcurrent: 1, retainWorkers: true },
    nodes: [
      node("task"),
      { ...node("verify"), kind: "verify", dependsOn: ["task"], gateScope: ["task"] },
    ],
  });
  plan = yield* plans.write({
    ...target,
    operation: "run",
    commandId: CommandId.make("reuse-run"),
    expectedRevision: plan.revision,
  });
  yield* reactor.drain(ROOT, plan.id);
  plan = yield* plans.read(target);
  const attempt = plan.nodes[0]!.attempts[0]!;
  const worker = attempt.workerThreadId;
  const turn = TurnId.make("reuse-original-turn");
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make("reuse-running"),
    threadId: worker,
    createdAt: NOW,
    session: {
      threadId: worker,
      status: "running",
      activeTurnId: turn,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
  yield* sql`UPDATE unattended_grants SET revoked = 1 WHERE grant_id = ${G.grantId}`;
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make("reuse-idle"),
    threadId: worker,
    createdAt: NOW,
    session: {
      threadId: worker,
      status: "ready",
      activeTurnId: null,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
  yield* engine.dispatch({
    type: "thread.turn.diff.complete",
    commandId: CommandId.make("reuse-completed"),
    threadId: worker,
    turnId: turn,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make("refs/t3/checkpoints/reuse"),
    status: "ready",
    files: [],
    checkpointTurnCount: 1,
    createdAt: NOW,
  });
  yield* reactor.drain(ROOT, plan.id);
  plan = yield* plans.read(target);
  expect(plan).toMatchObject({ paused: true, executionAuthority: G });
  expect(plan.nodes[0]!.attempts[0]).toMatchObject({
    status: "interrupted",
    failureCode: "executionAuthorityUnavailable",
  });
  expect(plan.nodes[0]!.attempts[0]!.handoffRequested).toBeUndefined();
  expect(plan.nodes[1]!.attempts).toHaveLength(0);
  expect(Option.getOrThrow(yield* query.getWorkerState(worker)).pendingMessageId).toBeNull();
  expect(yield* query.getThreadActivationAuthority(worker)).toEqual(Option.some(G));
  yield* engine.dispatch({
    type: "thread.turn.start",
    commandId: CommandId.make("root-B"),
    threadId: ROOT,
    message: {
      messageId: MessageId.make("root-B"),
      role: "user",
      text: "Foreground B",
      attachments: [],
    },
    modelSelection: MODEL,
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: NOW,
  });
  expect(yield* query.getThreadActivationAuthority(ROOT)).toEqual(Option.none());
  return { registry, retained, worker, plan, query, sql, engine };
});

const assertPersistedReuse = (
  worker: ThreadId,
  commandId: CommandId,
  plan: import("@t3tools/contracts").CoordinationPlan,
) =>
  Effect.gen(function* () {
    const query = yield* ProjectionSnapshotQuery;
    const sql = yield* SqlClient.SqlClient;
    const receipts = yield* OrchestrationCommandReceiptRepository;
    expect(yield* query.getThreadActivationAuthority(worker)).toEqual(Option.none());
    const rows = yield* sql<{
      message_id: string;
      authority_json: string | null;
    }>`SELECT message_id,authority_json FROM projection_thread_activation_authorities WHERE thread_id=${worker}`;
    expect(rows).toHaveLength(1);
    expect(rows[0]!.authority_json).toBeNull();
    expect(rows[0]!.message_id).toBe(`worker-message:${commandId}`);
    const state = Option.getOrThrow(yield* query.getWorkerState(worker));
    expect(state.pendingMessageId).toBe(rows[0]!.message_id);
    expect(state.thread.runtimeMode).toBe("approval-required");
    expect(state.thread.worker).toMatchObject({
      ownerThreadId: ROOT,
      rootThreadId: ROOT,
      mcpCapabilityCeiling: ["workers"],
    });
    expect(Option.getOrThrow(yield* receipts.getByCommandId({ commandId }))).toMatchObject({
      status: "accepted",
    });
    expect(yield* (yield* Plans.CoordinationPlans).read(target)).toEqual(plan);
    expect(
      (yield* sql<{
        revoked: number;
      }>`SELECT revoked FROM unattended_grants WHERE grant_id=${G.grantId}`)[0]!.revoked,
    ).toBe(1);
  });

describe("public revoked graph worker reuse", () => {
  it.effect(
    "OwnedWorkers.send denies retained caller G but resumes foreground B without target G",
    () =>
      Effect.gen(function* () {
        const { worker, query, plan, engine } = yield* prepare;
        const workers = yield* Workers.OwnedWorkers;
        const before = yield* query.getWorkerState(worker);
        const head = yield* engine.latestSequence;
        const denied = yield* workers
          .send(
            {
              callerThreadId: ROOT,
              workerThreadId: worker,
              commandId: CommandId.make("retained-G-send"),
              text: "Retained G",
            },
            { unattendedAuthority: G },
          )
          .pipe(Effect.flip);
        expect(denied).toMatchObject({ _tag: "WorkerOperationError", operation: "send" });
        expect(yield* engine.latestSequence).toBe(head);
        expect(yield* query.getWorkerState(worker)).toEqual(before);
        const commandId = CommandId.make("foreground-service-send");
        const result = yield* workers.send({
          callerThreadId: ROOT,
          workerThreadId: worker,
          commandId,
          text: "Foreground reuse",
        });
        expect(result.workerThreadId).toBe(worker);
        yield* assertPersistedReuse(worker, commandId, plan);
      }).pipe(Effect.provide(Layer.fresh(testLayer)), Effect.scoped),
  );

  it.effect(
    "registry-authenticated workers_send rejects revoked retained token and accepts fresh B token",
    () =>
      Effect.gen(function* () {
        const { registry, retained, worker, plan, engine, query } = yield* prepare;
        const before = yield* query.getWorkerState(worker);
        const head = yield* engine.latestSequence;
        // Authenticate using the actual registry. Never fabricate an invocation scope.
        expect(
          yield* registry.resolve(retained.config.authorizationHeader.slice(7)),
        ).toBeUndefined();
        expect(yield* query.getWorkerState(worker)).toEqual(before);
        expect(yield* engine.latestSequence).toBe(head);
        const fresh = yield* issue(registry);
        const scope = yield* registry.resolve(fresh.config.authorizationHeader.slice(7));
        expect(scope).toBeDefined();
        expect(scope?.unattendedAuthority).toBeUndefined();
        expect(scope?.capabilities.has("workers")).toBe(true);
        const toolkit = yield* WorkersToolkit.pipe(Effect.provide(WorkersToolkitHandlersLive));
        const commandId = CommandId.make("foreground-mcp-send");
        const results = yield* toolkit
          .handle("workers_send", {
            workerThreadId: worker,
            commandId,
            text: "Authenticated foreground reuse",
          })
          .pipe(
            Stream.unwrap,
            Stream.runCollect,
            Effect.provideService(Invocation.McpInvocationContext, scope!),
          );
        expect(results.at(-1)?.result).toMatchObject({ workerThreadId: worker });
        yield* assertPersistedReuse(worker, commandId, plan);
      }).pipe(Effect.provide(Layer.fresh(testLayer)), Effect.scoped),
  );
});
