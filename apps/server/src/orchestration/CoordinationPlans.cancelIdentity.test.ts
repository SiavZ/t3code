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
// Same real engine/projection stack as CoordinationPlans.test.ts. Only repository
// discovery is stubbed. No provider process or persisted user state is involved.
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
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-cancel-identity-" })),
  Layer.provideMerge(NodeServices.layer),
);
const testLayer = Layer.mergeAll(Plans.layer.pipe(Layer.provide(Store.layer)), Reactor.layer).pipe(
  Layer.provideMerge(core),
);
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Fresh retry completed",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Agent-reported result" },
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
const latest = (plan: CoordinationPlan) => plan.nodes[0]!.attempts.at(-1)!;
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
const finish = Effect.fn(function* (
  threadId: ThreadId,
  turnId: TurnId,
  label: string,
  status: "ready",
) {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "thread.turn.diff.complete",
    commandId: CommandId.make(label),
    threadId,
    turnId,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${label}`),
    status,
    files: [],
    checkpointTurnCount: 1,
    createdAt: NOW,
  });
});
const setupRetry = Effect.fn(function* (label: string) {
  const engine = yield* OrchestrationEngineService;
  const plans = yield* Plans.CoordinationPlans;
  const reactor = yield* Reactor.CoordinationReactor;
  const query = yield* ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  const root = ThreadId.make(`${label}:root`);
  const foreignRoot = ThreadId.make(`${label}:foreign`);
  const projectId = ProjectId.make(`${label}:project`);
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make(`${label}:project`),
    projectId,
    title: "Cancellation identity",
    workspaceRoot: `/workspace/${label}`,
    createdAt: NOW,
  });
  for (const threadId of [root, foreignRoot]) {
    yield* engine.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`${threadId}:create`),
      threadId,
      projectId,
      title: "Root",
      modelSelection: MODEL,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: "main",
      worktreePath: null,
      createdAt: NOW,
    });
  }
  const grant: ThreadUnattendedAuthority = {
    grantId: `${label}:G`,
    grantRevision: 1,
    ownerThreadId: root,
    runtimeModeCeiling: "approval-required",
    mcpCapabilityCeiling: ["workers"],
  };
  const higher: ThreadUnattendedAuthority = {
    ...grant,
    grantId: `${label}:H`,
    runtimeModeCeiling: "full-access",
  };
  // SQL seeds only the external authority ledger. Every assignment/turn/control
  // transition below goes through the real service or engine.
  for (const authority of [grant, higher]) {
    yield* sql`INSERT INTO unattended_grants (grant_id, owner_thread_id, project_id, revision, revoked, ceiling_json, created_at) VALUES (${authority.grantId}, ${root}, ${projectId}, 1, 0, ${JSON.stringify({ runtimeMode: authority.runtimeModeCeiling, mcpCapabilities: authority.mcpCapabilityCeiling })}, ${NOW})`;
  }
  const target = { callerThreadId: root, rootThreadId: root, planId: `${label}:plan` };
  let plan = yield* plans.write(
    {
      ...target,
      commandId: CommandId.make(`${label}:create`),
      expectedRevision: 0,
      operation: "create",
      policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
      nodes: [node("task"), node("after", ["task"])],
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
  const first = latest(plan);
  const oldTurn = TurnId.make(`${label}:old-turn`);
  yield* session(first.workerThreadId, oldTurn, `${label}:old-start`, "running");
  plan = yield* plans.write({
    ...target,
    commandId: CommandId.make(`${label}:cancel`),
    expectedRevision: plan.revision,
    operation: "cancel",
  });
  expect(plan.cancelled).toBe(true);
  // A simulated provider acknowledgement is an engine operation, not a direct
  // projection write. Its durable terminal receipt lets the reactor settle.
  yield* session(first.workerThreadId, null, `${label}:cancel-ack`, "stopped");
  yield* finish(first.workerThreadId, oldTurn, `${label}:old-finish`, "ready");
  yield* reactor.drain(root, plan.id);
  plan = yield* plans.read(target);
  expect(latest(plan).status).toBe("interrupted");
  const cancelled = plan;
  expect(
    yield* plans
      .write(
        {
          ...target,
          commandId: CommandId.make(`${label}:retry-H`),
          expectedRevision: plan.revision,
          operation: "retry",
          nodeId: "task",
        },
        higher,
      )
      .pipe(Effect.flip),
  ).toMatchObject({
    code: "forbidden",
    detail: "A later activation cannot replace the immutable plan execution grant.",
  });
  expect(yield* plans.read(target)).toEqual(cancelled);
  plan = yield* plans.write(
    {
      ...target,
      commandId: CommandId.make(`${label}:retry-G`),
      expectedRevision: plan.revision,
      operation: "retry",
      nodeId: "task",
    },
    grant,
  );
  plan = yield* plans.write(
    {
      ...target,
      commandId: CommandId.make(`${label}:resume-G`),
      expectedRevision: plan.revision,
      operation: "run",
    },
    grant,
  );
  yield* reactor.drain(root, plan.id);
  plan = yield* plans.read(target);
  const current = latest(plan);
  expect(plan.executionAuthority).toEqual(grant);
  expect(plan.cancelled).toBe(false);
  expect(plan.nodes[0]!.attempts[0]!.status).toBe("superseded");
  expect(current.number).toBe(2);
  expect(current.dispatchMessageId).not.toBe(first.dispatchMessageId);
  expect(yield* query.getThreadActivationAuthority(current.workerThreadId)).toMatchObject({
    _tag: "Some",
    value: grant,
  });
  expect(yield* query.getThreadShellById(current.workerThreadId)).toMatchObject({
    _tag: "Some",
    value: { runtimeMode: "approval-required" },
  });
  const turnId = TurnId.make(`${label}:fresh-turn`);
  yield* session(current.workerThreadId, turnId, `${label}:fresh-start`, "running");
  return { label, target, foreignRoot, grant, plan, first, current, oldTurn, turnId };
});

const invalidCases = [
  "wrong root cancel",
  "stale revision cancel",
  "wrong root dispatch",
  "wrong node dispatch",
  "stale revision dispatch",
  "stale attempt report",
  "stale message abandon",
  "stale turn report",
  "stale message stop",
  "stale turn interrupt",
  "wrong node settle",
  "stale turn settle",
] as const;

it.layer(testLayer)("Coordination cancellation identity and same-plan retry", (it) => {
  for (const invalidCase of invalidCases) {
    it.effect(`rejects ${invalidCase} without affecting the fresh retry assignment`, () =>
      Effect.gen(function* () {
        const label = invalidCase.replaceAll(" ", "-");
        const fixture = yield* setupRetry(label);
        const { target, first, current, oldTurn, turnId, foreignRoot, grant } = fixture;
        let plan = fixture.plan;
        const engine = yield* OrchestrationEngineService;
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const query = yield* ProjectionSnapshotQuery;
        const beforeWorker = yield* query.getWorkerState(current.workerThreadId);
        const commandId = CommandId.make(`${label}:invalid`);
        const dispatch = {
          commandId,
          threadId: target.rootThreadId,
          planId: plan.id,
          expectedRevision: plan.revision,
          nodeId: "task",
          createdAt: NOW,
        };
        switch (invalidCase) {
          case "wrong root cancel":
            expect(
              yield* plans
                .write({
                  ...target,
                  callerThreadId: foreignRoot,
                  rootThreadId: foreignRoot,
                  commandId,
                  expectedRevision: plan.revision,
                  operation: "cancel",
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "stale revision cancel":
            expect(
              yield* plans
                .write({
                  ...target,
                  commandId,
                  expectedRevision: plan.revision - 1,
                  operation: "cancel",
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "wrong root dispatch":
            expect(
              yield* engine
                .dispatch({
                  ...dispatch,
                  type: "coordination.plan.dispatch",
                  threadId: foreignRoot,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "forbidden" });
            break;
          case "wrong node dispatch":
            expect(
              yield* engine
                .dispatch({
                  ...dispatch,
                  type: "coordination.plan.dispatch",
                  nodeId: "missing",
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "busy" });
            break;
          case "stale revision dispatch":
            expect(
              yield* engine
                .dispatch({
                  ...dispatch,
                  type: "coordination.plan.dispatch",
                  expectedRevision: plan.revision - 1,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "stale attempt report":
            expect(
              yield* plans
                .write({
                  ...target,
                  callerThreadId: current.workerThreadId,
                  commandId,
                  expectedRevision: plan.revision,
                  operation: "complete",
                  nodeId: "task",
                  attemptNumber: first.number,
                  workerThreadId: current.workerThreadId,
                  turnId,
                  artifact,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "forbidden" });
            break;
          case "stale message abandon":
            expect(
              yield* engine
                .dispatch({
                  ...dispatch,
                  type: "coordination.plan.abandon",
                  expectedMessageId: first.dispatchMessageId,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "stale turn report":
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
            break;
          case "stale message stop":
            expect(
              yield* engine
                .dispatch({
                  type: "thread.session.stop",
                  commandId,
                  threadId: current.workerThreadId,
                  expectedMessageId: first.dispatchMessageId,
                  expectedTurnId: turnId,
                  createdAt: NOW,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "stale turn interrupt":
            expect(
              yield* engine
                .dispatch({
                  type: "thread.turn.interrupt",
                  commandId,
                  threadId: current.workerThreadId,
                  expectedMessageId: current.dispatchMessageId,
                  expectedTurnId: oldTurn,
                  createdAt: NOW,
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "wrong node settle":
            expect(
              yield* engine
                .dispatch({
                  ...dispatch,
                  type: "coordination.plan.settle",
                  nodeId: "missing",
                  turnId,
                  outcome: "completed",
                })
                .pipe(Effect.flip),
            ).toMatchObject({ code: "conflict" });
            break;
          case "stale turn settle":
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
            break;
        }
        expect(yield* plans.read(target)).toEqual(plan);
        expect(yield* query.getWorkerState(current.workerThreadId)).toEqual(beforeWorker);
        expect(yield* query.getThreadActivationAuthority(current.workerThreadId)).toMatchObject({
          _tag: "Some",
          value: grant,
        });
        // After every rejected identity, the exact fresh assignment still accepts
        // its report and terminal receipt, then dispatches its dependent under G.
        plan = yield* plans.write({
          ...target,
          callerThreadId: current.workerThreadId,
          commandId: CommandId.make(`${label}:fresh-report`),
          expectedRevision: plan.revision,
          operation: "complete",
          nodeId: "task",
          attemptNumber: current.number,
          workerThreadId: current.workerThreadId,
          turnId,
          artifact,
        });
        yield* session(current.workerThreadId, null, `${label}:fresh-idle`, "ready");
        yield* finish(current.workerThreadId, turnId, `${label}:fresh-finish`, "ready");
        yield* reactor.drain(target.rootThreadId, plan.id);
        const settled = yield* plans.read(target);
        expect(latest(settled)).toMatchObject({
          status: "succeeded",
          number: 2,
          turnId,
          dispatchMessageId: current.dispatchMessageId,
          artifact,
          pendingArtifact: null,
        });
        expect(settled.nodes[1]!.attempts).toHaveLength(1);
        expect(settled.executionAuthority).toEqual(grant);
        expect(
          yield* query.getThreadActivationAuthority(settled.nodes[1]!.attempts[0]!.workerThreadId),
        ).toMatchObject({ _tag: "Some", value: grant });
      }),
    );
  }
});
