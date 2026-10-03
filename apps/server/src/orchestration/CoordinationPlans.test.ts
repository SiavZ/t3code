import {
  CheckpointRef,
  CommandId,
  EventId,
  EnvironmentId,
  MessageId,
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
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Stream from "effect/Stream";
import { ServerConfig } from "../config.ts";
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
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
import * as McpInvocation from "../mcp/McpInvocationContext.ts";
import { CoordinationToolkit } from "../mcp/toolkits/coordination/tools.ts";
import { CoordinationToolkitHandlersLive } from "../mcp/toolkits/coordination/handlers.ts";

const ROOT = ThreadId.make("root");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" };
const NOW = "2026-10-03T00:00:00.000Z";
const makeCore = <E, R>(database: Layer.Layer<SqlClient.SqlClient, E, R>) =>
  Layer.mergeAll(
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
    Layer.provideMerge(database),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-coordination-test-" })),
    Layer.provideMerge(NodeServices.layer),
  );
const core = makeCore(SqlitePersistenceMemory);
const testLayer = Layer.mergeAll(Plans.layer.pipe(Layer.provide(Store.layer)), Reactor.layer).pipe(
  Layer.provideMerge(core),
);
const setup = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("project"),
    projectId: ProjectId.make("project"),
    title: "Plans",
    workspaceRoot: "/workspace/plans",
    createdAt: NOW,
  });
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make("root"),
    threadId: ROOT,
    projectId: ProjectId.make("project"),
    title: "Root",
    modelSelection: MODEL,
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: "main",
    worktreePath: null,
    createdAt: NOW,
  });
});

it.layer(testLayer)("Coordination capacity and cancellation receipts", (it) => {
  it.effect(
    "pauses and interrupts a quiescent deep assignment when its report continuation grant is revoked",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const engine = yield* OrchestrationEngineService;
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const sql = yield* SqlClient.SqlClient;
        const root = ThreadId.make("revoked-handoff-root");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("revoked-handoff-root"),
          threadId: root,
          projectId: ProjectId.make("project"),
          title: "Revoked handoff",
          modelSelection: MODEL,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdAt: NOW,
        });
        const grant: ThreadUnattendedAuthority = {
          grantId: "revoked-handoff-G",
          grantRevision: 1,
          ownerThreadId: root,
          runtimeModeCeiling: "approval-required",
          mcpCapabilityCeiling: ["workers"],
        };
        yield* sql`INSERT INTO unattended_grants (grant_id, owner_thread_id, project_id, revision, revoked, ceiling_json, created_at) VALUES (${grant.grantId}, ${root}, ${"project"}, 1, 0, ${JSON.stringify({ runtimeMode: "approval-required", mcpCapabilities: ["workers"] })}, ${NOW})`;
        const target = { callerThreadId: root, rootThreadId: root, planId: "revoked-handoff-plan" };
        let plan = yield* plans.write(
          {
            ...target,
            commandId: CommandId.make("revoked-handoff-create"),
            expectedRevision: 0,
            operation: "create",
            policy: { mode: "deep", maxConcurrent: 1, retainWorkers: true },
            nodes: [
              node("task"),
              { ...node("verify"), kind: "verify", dependsOn: ["task"], gateScope: ["task"] },
            ],
          },
          grant,
        );
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("revoked-handoff-run"),
          expectedRevision: plan.revision,
          operation: "run",
        });
        yield* reactor.drain(root, plan.id);
        plan = yield* plans.read(target);
        const initial = plan.nodes[0]!.attempts[0]!;
        yield* sql`UPDATE unattended_grants SET revoked = 1 WHERE grant_id = ${grant.grantId}`;
        plan = yield* executeAttempt(plan, "task");
        expect(plan.paused).toBe(true);
        expect(plan.executionAuthority).toEqual(grant);
        expect(plan.nodes[0]!.attempts[0]).toMatchObject({
          status: "interrupted",
          failureCode: "executionAuthorityUnavailable",
          dispatchMessageId: initial.dispatchMessageId,
        });
        expect(plan.nodes[0]!.attempts[0]!.handoffRequested).toBeUndefined();
        expect(plan.nodes[1]!.attempts).toHaveLength(0);
        expect(
          yield* plans
            .write({
              ...target,
              commandId: CommandId.make("revoked-handoff-retry"),
              expectedRevision: plan.revision,
              operation: "retry",
              nodeId: "task",
            })
            .pipe(Effect.flip),
        ).toMatchObject({ code: "forbidden" });
        yield* engine.dispatch({
          type: "thread.worker.send",
          commandId: CommandId.make("revoked-handoff-explicit-send"),
          callerThreadId: root,
          threadId: initial.workerThreadId,
          text: "Explicit owner work after graph assignment settled",
          createdAt: NOW,
        });
      }),
  );
  it.effect(
    "preserves immutable graph grant G across foreground B, rejects H upgrade and revoked dispatch",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const engine = yield* OrchestrationEngineService;
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const query = yield* ProjectionQuery.ProjectionSnapshotQuery;
        const sql = yield* SqlClient.SqlClient;
        const root = ThreadId.make("authority-root");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("authority-root"),
          threadId: root,
          projectId: ProjectId.make("project"),
          title: "Grant root",
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdAt: NOW,
        });
        const grant: ThreadUnattendedAuthority = {
          grantId: "graph-G",
          grantRevision: 1,
          ownerThreadId: root,
          runtimeModeCeiling: "approval-required",
          mcpCapabilityCeiling: ["workers"],
        };
        const higher: ThreadUnattendedAuthority = {
          ...grant,
          grantId: "graph-H",
          runtimeModeCeiling: "full-access",
        };
        for (const authority of [grant, higher])
          yield* sql`INSERT INTO unattended_grants (grant_id, owner_thread_id, project_id, revision, revoked, ceiling_json, created_at) VALUES (${authority.grantId}, ${root}, ${"project"}, 1, 0, ${JSON.stringify({ runtimeMode: authority.runtimeModeCeiling, mcpCapabilities: authority.mcpCapabilityCeiling })}, ${NOW})`;
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("authority-start-G"),
          threadId: root,
          message: {
            messageId: MessageId.make("authority-G"),
            role: "user",
            text: "Grant G",
            attachments: [],
          },
          modelSelection: MODEL,
          runtimeMode: "approval-required",
          interactionMode: "default",
          unattendedAuthority: grant,
          createdAt: NOW,
        });
        const target = { callerThreadId: root, rootThreadId: root, planId: "authority-plan" };
        let plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("authority-create"),
          expectedRevision: 0,
          operation: "create",
          policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
          nodes: [node("first"), { ...node("second"), dependsOn: ["first"] }],
        });
        expect(plan.executionAuthority).toEqual(grant);
        plan = yield* plans.write(
          {
            ...target,
            commandId: CommandId.make("authority-run"),
            expectedRevision: plan.revision,
            operation: "run",
          },
          grant,
        );
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("authority-foreground-B"),
          threadId: root,
          message: {
            messageId: MessageId.make("authority-B"),
            role: "user",
            text: "Foreground B",
            attachments: [],
          },
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt: NOW,
        });
        expect((yield* query.getThreadActivationAuthority(root))._tag).toBe("None");
        const toolkit = yield* CoordinationToolkit.pipe(
          Effect.provide(CoordinationToolkitHandlersLive),
        );
        yield* toolkit
          .handle("coordination_write", {
            input: {
              operation: "run",
              commandId: CommandId.make("authority-MCP-run"),
              rootThreadId: root,
              planId: plan.id,
              expectedRevision: plan.revision,
            },
          })
          .pipe(
            Stream.unwrap,
            Stream.runDrain,
            Effect.provideService(McpInvocation.McpInvocationContext, {
              environmentId: EnvironmentId.make("environment"),
              threadId: root,
              providerSessionId: "session-G",
              providerInstanceId: MODEL.instanceId,
              capabilities: new Set<McpInvocation.McpCapability>(["workers"]),
              issuedAt: 1,
              unattendedAuthority: grant,
            }),
          );
        plan = yield* plans.read(target);
        yield* reactor.drain(root, plan.id);
        plan = yield* plans.read(target);
        const worker = plan.nodes[0]!.attempts[0]!.workerThreadId;
        expect(yield* query.getThreadShellById(worker)).toMatchObject({
          _tag: "Some",
          value: { runtimeMode: "approval-required" },
        });
        expect(yield* query.getThreadActivationAuthority(worker)).toMatchObject({
          _tag: "Some",
          value: grant,
        });
        expect(
          yield* plans
            .write(
              {
                ...target,
                commandId: CommandId.make("authority-upgrade-H"),
                expectedRevision: plan.revision,
                operation: "run",
              },
              higher,
            )
            .pipe(Effect.flip),
        ).toMatchObject({ code: "forbidden" });
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("authority-foreground-H"),
          threadId: root,
          message: {
            messageId: MessageId.make("authority-H"),
            role: "user",
            text: "Foreground H",
            attachments: [],
          },
          modelSelection: MODEL,
          runtimeMode: "full-access",
          interactionMode: "default",
          unattendedAuthority: higher,
          createdAt: NOW,
        });
        expect(
          yield* plans
            .write({
              ...target,
              commandId: CommandId.make("authority-client-upgrade-H"),
              expectedRevision: plan.revision,
              operation: "run",
            })
            .pipe(Effect.flip),
        ).toMatchObject({ code: "forbidden" });
        yield* sql`UPDATE unattended_grants SET revoked = 1 WHERE grant_id = ${grant.grantId}`;
        plan = yield* executeAttempt(plan, "first", { ...artifact, summary: "G completed" });
        expect(
          yield* engine
            .dispatch({
              type: "coordination.plan.dispatch",
              commandId: CommandId.make("authority-revoked-dispatch"),
              threadId: root,
              planId: plan.id,
              expectedRevision: plan.revision,
              nodeId: "second",
              createdAt: NOW,
            })
            .pipe(Effect.flip),
        ).toMatchObject({ code: "forbidden" });
        plan = yield* plans.read(target);
        expect(plan.executionAuthority).toEqual(grant);
        expect(plan.nodes[0]!.attempts.at(-1)!.status).toBe("succeeded");
        expect(plan.nodes[1]!.attempts).toHaveLength(0);
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("authority-cancel"),
          expectedRevision: plan.revision,
          operation: "cancel",
        });
        expect(plan.cancelled).toBe(true);
        expect(plan.executionAuthority).toEqual(grant);
      }),
  );
  it.effect("wakes a low-ID plan after the bounded scan cursor has passed it", () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const engine = yield* OrchestrationEngineService;
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const root = ThreadId.make("cursor-root");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("cursor-root"),
          threadId: root,
          projectId: ProjectId.make("project"),
          title: "Cursor",
          modelSelection: MODEL,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdAt: NOW,
        });
        for (let index = 0; index < 129; index++)
          yield* plans.write({
            callerThreadId: root,
            rootThreadId: root,
            planId: `cursor-${String(index).padStart(3, "0")}`,
            commandId: CommandId.make(`cursor-create:${index}`),
            expectedRevision: 0,
            operation: "create",
            policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
            nodes: [node("task")],
          });
        yield* reactor.drain();
        const target = { callerThreadId: root, rootThreadId: root, planId: "cursor-000" };
        const accepted = yield* Stream.toPull(
          engine.streamDomainEvents.pipe(
            Stream.filter(
              (event) =>
                event.type === "coordination.plan.updated" &&
                event.payload.plan.id === target.planId &&
                event.payload.plan.nodes[0]!.attempts.length === 1,
            ),
            Stream.take(1),
          ),
        );
        yield* reactor.start;
        const plan = yield* plans.read(target);
        yield* plans.write({
          ...target,
          commandId: CommandId.make("cursor-run"),
          expectedRevision: plan.revision,
          operation: "run",
        });
        yield* accepted;
        expect((yield* plans.read(target)).nodes[0]!.attempts).toHaveLength(1);
      }),
    ),
  );
  it.effect(
    "settles on a terminal native background activity without another session or graph wake",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          yield* setup;
          const engine = yield* OrchestrationEngineService;
          const plans = yield* Plans.CoordinationPlans;
          const reactor = yield* Reactor.CoordinationReactor;
          const background = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
          const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "native-background" };
          let plan = yield* plans.write({
            ...target,
            commandId: CommandId.make("native-background-create"),
            expectedRevision: 0,
            operation: "create",
            policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
            nodes: [node("task")],
          });
          plan = yield* plans.write({
            ...target,
            commandId: CommandId.make("native-background-run"),
            expectedRevision: plan.revision,
            operation: "run",
          });
          yield* reactor.drain(ROOT, plan.id);
          plan = yield* plans.read(target);
          const threadId = plan.nodes[0]!.attempts[0]!.workerThreadId;
          background.recordTaskLiveness({
            threadId,
            taskId: "native-child",
            taskType: "agent",
            status: "running",
            kind: "started",
          });
          plan = yield* executeAttempt(plan, "task", artifact);
          expect(plan.nodes[0]!.attempts[0]!.status).toBe("accepted");
          const settled = yield* Stream.toPull(
            engine.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "coordination.plan.updated" &&
                  event.payload.plan.id === target.planId &&
                  event.payload.plan.nodes[0]!.attempts[0]?.status === "succeeded",
              ),
              Stream.take(1),
            ),
          );
          yield* reactor.start;
          background.recordTaskLiveness({
            threadId,
            taskId: "native-child",
            taskType: "agent",
            status: "completed",
            kind: "completed",
          });
          yield* engine.dispatch({
            type: "thread.activity.append",
            commandId: CommandId.make("native-background-terminal"),
            threadId,
            createdAt: NOW,
            activity: {
              id: EventId.make("native-background-terminal"),
              kind: "task.completed",
              tone: "info",
              summary: "Native child completed",
              payload: { taskId: "native-child" },
              turnId: null,
              createdAt: NOW,
            },
          });
          yield* settled;
          expect((yield* plans.read(target)).nodes[0]!.attempts[0]!.status).toBe("succeeded");
        }),
      ),
  );
  it.effect(
    "retries transient capacity with the same dispatch identity on an ordinary worker release",
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          yield* setup;
          const engine = yield* OrchestrationEngineService;
          const plans = yield* Plans.CoordinationPlans;
          const reactor = yield* Reactor.CoordinationReactor;
          const root = ThreadId.make("capacity-root");
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make("capacity-root"),
            threadId: root,
            projectId: ProjectId.make("project"),
            title: "Capacity",
            modelSelection: MODEL,
            runtimeMode: "approval-required",
            interactionMode: "default",
            branch: "main",
            worktreePath: null,
            createdAt: NOW,
          });
          for (let index = 0; index < 4; index++)
            yield* engine.dispatch({
              type: "thread.worker.spawn",
              commandId: CommandId.make(`ordinary:${index}`),
              callerThreadId: root,
              threadId: ThreadId.make(`ordinary:${index}`),
              label: `ordinary${index}`,
              prompt: "Benign task",
              modelSelection: MODEL,
              mcpCapabilityCeiling: ["workers"],
              spawnFingerprint: `ordinary${index}`,
              createdAt: NOW,
            });
          const target = { callerThreadId: root, rootThreadId: root, planId: "capacity-plan" };
          let plan = yield* plans.write({
            ...target,
            commandId: CommandId.make("capacity-create"),
            expectedRevision: 0,
            operation: "create",
            policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
            nodes: [node("waiting")],
          });
          plan = yield* plans.write({
            ...target,
            commandId: CommandId.make("capacity-run"),
            expectedRevision: plan.revision,
            operation: "run",
          });
          yield* reactor.drain(root, plan.id);
          yield* reactor.drain(root, plan.id);
          expect((yield* plans.read(target)).nodes[0]!.attempts).toHaveLength(0);
          const accepted = yield* Stream.toPull(
            engine.streamDomainEvents.pipe(
              Stream.filter(
                (event) =>
                  event.type === "coordination.plan.updated" &&
                  event.payload.plan.id === target.planId &&
                  event.payload.plan.nodes[0]!.attempts.length === 1,
              ),
              Stream.take(1),
            ),
          );
          yield* reactor.start;
          yield* engine.dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("ordinary-release"),
            threadId: ThreadId.make("ordinary:0"),
            createdAt: NOW,
            session: {
              threadId: ThreadId.make("ordinary:0"),
              status: "stopped",
              activeTurnId: null,
              providerName: "codex",
              runtimeMode: "approval-required",
              lastError: null,
              updatedAt: NOW,
            },
          });
          yield* accepted;
          plan = yield* plans.read(target);
          expect(plan.nodes[0]!.attempts).toHaveLength(1);
        }),
      ),
  );
  it.effect("settles cancelled and deleted assignments before their first native turn", () =>
    Effect.gen(function* () {
      yield* setup;
      const engine = yield* OrchestrationEngineService;
      const plans = yield* Plans.CoordinationPlans;
      const reactor = yield* Reactor.CoordinationReactor;
      const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "no-native-turn" };
      let plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("no-turn-create"),
        expectedRevision: 0,
        operation: "create",
        policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
        nodes: [node("task")],
      });
      plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("no-turn-run"),
        expectedRevision: plan.revision,
        operation: "run",
      });
      yield* reactor.drain(ROOT, plan.id);
      plan = yield* plans.read(target);
      const worker = plan.nodes[0]!.attempts[0]!.workerThreadId;
      plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("no-turn-cancel"),
        expectedRevision: plan.revision,
        operation: "cancel",
      });
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: CommandId.make("no-turn-stop-ack"),
        threadId: worker,
        createdAt: NOW,
        session: {
          threadId: worker,
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
        commandId: CommandId.make("no-turn-retry"),
        expectedRevision: plan.revision,
        operation: "retry",
        nodeId: "task",
      });
      plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("no-turn-resume"),
        expectedRevision: plan.revision,
        operation: "run",
      });
      yield* reactor.drain(ROOT, plan.id);
      plan = yield* plans.read(target);
      const recipient = plan.nodes[0]!.attempts[1]!.workerThreadId;
      const inbox = yield* plans.mailboxRead({ callerThreadId: ROOT, rootThreadId: ROOT });
      yield* plans.mailboxWrite({
        callerThreadId: ROOT,
        rootThreadId: ROOT,
        commandId: CommandId.make("deleted-recipient-message"),
        expectedRevision: inbox.revision,
        operation: "message",
        recipientThreadIds: [recipient],
        channelId: null,
        text: "Cancelled when recipient is deleted",
      });
      yield* engine.dispatch({
        type: "thread.delete",
        commandId: CommandId.make("no-turn-delete"),
        threadId: recipient,
      });
      expect(
        (yield* plans.mailboxRead({ callerThreadId: ROOT, rootThreadId: ROOT })).envelopes.find(
          (entry) => entry.recipientThreadId === recipient,
        )!.delivery,
      ).toBe("cancelled");
      yield* reactor.drain(ROOT, plan.id);
      expect((yield* plans.read(target)).nodes[0]!.attempts[1]!.status).toBe("interrupted");
    }),
  );
});
const node = (id: string, dependsOn: string[] = []) => ({
  id,
  kind: "work" as const,
  prompt: `Implement ${id}`,
  dependsOn,
  gateScope: [],
  attemptLimit: 3,
  modelSelection: MODEL,
});
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Implemented task",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Not independently tested" },
  unchecked: [],
  confidence: "medium",
  outcome: "completed",
};
const executeAttempt = Effect.fn(function* (
  plan: CoordinationPlan,
  nodeId: string,
  report?: CoordinationArtifact,
) {
  const plans = yield* Plans.CoordinationPlans;
  const engine = yield* OrchestrationEngineService;
  const reactor = yield* Reactor.CoordinationReactor;
  const attempt = plan.nodes.find((entry) => entry.id === nodeId)!.attempts.at(-1)!;
  const label = `${plan.id}:${nodeId}:${attempt.number}:${attempt.handoffRequested ? "handoff" : "task"}`;
  const turnId = TurnId.make(label);
  const threadId = attempt.workerThreadId;
  const target = {
    callerThreadId: plan.rootThreadId,
    rootThreadId: plan.rootThreadId,
    planId: plan.id,
  };
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(`start:${label}`),
    threadId,
    createdAt: NOW,
    session: {
      threadId,
      status: "running",
      activeTurnId: turnId,
      providerName: "codex",
      runtimeMode: "approval-required",
      lastError: null,
      updatedAt: NOW,
    },
  });
  if (report)
    plan = yield* plans.write({
      ...target,
      callerThreadId: threadId,
      commandId: CommandId.make(`report:${label}`),
      expectedRevision: plan.revision,
      operation: "complete",
      nodeId,
      attemptNumber: attempt.number,
      workerThreadId: threadId,
      turnId,
      artifact: report,
    });
  yield* engine.dispatch({
    type: "thread.session.set",
    commandId: CommandId.make(`idle:${label}`),
    threadId,
    createdAt: NOW,
    session: {
      threadId,
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
    commandId: CommandId.make(`finish:${label}`),
    threadId,
    turnId,
    completedAt: NOW,
    checkpointRef: CheckpointRef.make(`refs/t3/checkpoints/${label}`),
    status: "ready",
    files: [],
    checkpointTurnCount: 1,
    createdAt: NOW,
  });
  yield* reactor.drain(plan.rootThreadId, plan.id);
  return yield* plans.read(target);
});

it.effect(
  "reopens persisted engine state, pauses recovery and preserves durable inbox receipts",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-coordination-restart-" });
        const diskLayer = Layer.mergeAll(
          Plans.layer.pipe(Layer.provide(Store.layer)),
          Reactor.layer,
        ).pipe(
          Layer.provideMerge(makeCore(makeSqlitePersistenceLive(`${directory}/state.sqlite`))),
        );
        const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "cold-restart" };
        const before = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* setup;
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            let plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("cold-create"),
              expectedRevision: 0,
              operation: "create",
              policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
              nodes: [node("task")],
            });
            plan = yield* plans.write({
              ...target,
              commandId: CommandId.make("cold-run"),
              expectedRevision: plan.revision,
              operation: "run",
            });
            yield* reactor.drain(ROOT, plan.id);
            yield* plans.mailboxWrite({
              callerThreadId: ROOT,
              rootThreadId: ROOT,
              commandId: CommandId.make("cold-inbox"),
              expectedRevision: 0,
              operation: "message",
              recipientThreadIds: [ROOT],
              channelId: null,
              text: "Persist this receipt across restart",
            });
            return yield* plans.read(target);
          }).pipe(Effect.provide(Layer.fresh(diskLayer))),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const plans = yield* Plans.CoordinationPlans;
            const reactor = yield* Reactor.CoordinationReactor;
            const engine = yield* OrchestrationEngineService;
            expect(yield* plans.read(target)).toEqual(before);
            const inbox = yield* plans.mailboxRead({ callerThreadId: ROOT, rootThreadId: ROOT });
            expect(inbox.envelopes).toHaveLength(1);
            expect(inbox.envelopes[0]!.delivery).toBe("pending");
            const threadId = before.nodes[0]!.attempts[0]!.workerThreadId;
            // The actual startup reconciler publishes this state after discovering that
            // no native provider owns the persisted pending activation.
            yield* engine.dispatch({
              type: "thread.session.set",
              commandId: CommandId.make("cold-orphan-reconciled"),
              threadId,
              createdAt: NOW,
              session: {
                threadId,
                status: "interrupted",
                activeTurnId: null,
                providerName: "codex",
                runtimeMode: "approval-required",
                lastError: "Server restarted before native activation",
                updatedAt: NOW,
              },
            });
            yield* reactor.recover;
            const recovered = yield* plans.read(target);
            expect(recovered.paused).toBe(true);
            expect(recovered.nodes[0]!.attempts[0]!.status).toBe("interrupted");
            yield* reactor.drain(ROOT, recovered.id);
            expect((yield* plans.read(target)).nodes[0]!.attempts).toHaveLength(1);
            yield* plans.mailboxWrite({
              callerThreadId: ROOT,
              rootThreadId: ROOT,
              commandId: CommandId.make("cold-inbox"),
              expectedRevision: 0,
              operation: "message",
              recipientThreadIds: [ROOT],
              channelId: null,
              text: "Persist this receipt across restart",
            });
            expect(
              (yield* plans.mailboxRead({ callerThreadId: ROOT, rootThreadId: ROOT })).envelopes,
            ).toHaveLength(1);
          }).pipe(Effect.provide(Layer.fresh(diskLayer))),
        );
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
);

it.layer(testLayer)("CoordinationPlans real engine", (it) => {
  it.effect(
    "durably pauses recovery, interrupts orphan starts and retries with a fresh assignment",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const engine = yield* OrchestrationEngineService;
        const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "restart" };
        let plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("restart-create"),
          expectedRevision: 0,
          operation: "create",
          policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
          nodes: [node("task"), node("after", ["task"])],
        });
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("restart-run"),
          expectedRevision: plan.revision,
          operation: "run",
        });
        yield* reactor.drain(ROOT);
        plan = yield* plans.read(target);
        const first = plan.nodes[0]!.attempts[0]!;
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("startup-orphan"),
          threadId: first.workerThreadId,
          createdAt: NOW,
          session: {
            threadId: first.workerThreadId,
            status: "interrupted",
            activeTurnId: null,
            providerName: "codex",
            runtimeMode: "approval-required",
            lastError: "Server restarted",
            updatedAt: NOW,
          },
        });
        yield* reactor.recover;
        plan = yield* plans.read(target);
        expect(plan.paused).toBe(true);
        expect(plan.nodes[0]!.attempts[0]!.status).toBe("interrupted");
        expect(plan.nodes[0]!.attempts[0]!.failureCode).toBe("serverRestart");
        yield* reactor.drain(ROOT);
        expect((yield* plans.read(target)).nodes[1]!.attempts).toHaveLength(0);
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("explicit-retry"),
          expectedRevision: plan.revision,
          operation: "retry",
          nodeId: "task",
        });
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("explicit-resume"),
          expectedRevision: plan.revision,
          operation: "run",
        });
        yield* reactor.drain(ROOT);
        plan = yield* plans.read(target);
        expect(plan.nodes[0]!.attempts).toHaveLength(2);
        expect(plan.nodes[0]!.attempts[1]!.dispatchMessageId).not.toBe(first.dispatchMessageId);
        expect(plan.nodes[0]!.attempts[0]!.status).toBe("superseded");
        const stale = yield* plans
          .write({
            ...target,
            callerThreadId: first.workerThreadId,
            commandId: CommandId.make("restart-stale-report"),
            expectedRevision: plan.revision,
            operation: "complete",
            nodeId: "task",
            attemptNumber: 1,
            workerThreadId: first.workerThreadId,
            turnId: TurnId.make("old"),
            artifact,
          })
          .pipe(Effect.result);
        expect(stale._tag).toBe("Failure");
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("cancel-restarted"),
          expectedRevision: plan.revision,
          operation: "cancel",
        });
        expect(plan.cancelled).toBe(true);
        const cancelledWorker = plan.nodes[0]!.attempts[1]!.workerThreadId;
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("cancel-restarted-ack"),
          threadId: cancelledWorker,
          createdAt: NOW,
          session: {
            threadId: cancelledWorker,
            status: "stopped",
            activeTurnId: null,
            providerName: "codex",
            runtimeMode: "approval-required",
            lastError: null,
            updatedAt: NOW,
          },
        });
        yield* reactor.drain(ROOT);
        expect((yield* plans.read(target)).nodes[1]!.attempts).toHaveLength(0);
      }),
  );
  it.effect(
    "rejects a foreign root plan ID collision without overwriting its durable document",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const plans = yield* Plans.CoordinationPlans;
        const engine = yield* OrchestrationEngineService;
        const foreignRoot = ThreadId.make("foreign-root");
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("foreign-root-create"),
          threadId: foreignRoot,
          projectId: ProjectId.make("project"),
          title: "Foreign",
          modelSelection: MODEL,
          runtimeMode: "approval-required",
          interactionMode: "default",
          branch: "main",
          worktreePath: null,
          createdAt: NOW,
        });
        const input = {
          callerThreadId: ROOT,
          rootThreadId: ROOT,
          planId: "same-id",
          commandId: CommandId.make("same-id-create"),
          expectedRevision: 0,
          operation: "create" as const,
          policy: { mode: "light" as const, maxConcurrent: 1, retainWorkers: true },
          nodes: [node("original")],
        };
        const original = yield* plans.write(input);
        const collision = yield* plans
          .write({
            ...input,
            callerThreadId: foreignRoot,
            rootThreadId: foreignRoot,
            commandId: CommandId.make("collision"),
            nodes: [node("overwrite")],
          })
          .pipe(Effect.result);
        expect(collision._tag).toBe("Failure");
        expect(yield* plans.read(input)).toEqual(original);
      }),
  );
  it.effect(
    "persists active peer inbox without native steering, exact replay, channel reversals and context CAS",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const plans = yield* Plans.CoordinationPlans;
        const engine = yield* OrchestrationEngineService;
        const reactor = yield* Reactor.CoordinationReactor;
        const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "mail-workers" };
        let plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("mail-create"),
          expectedRevision: 0,
          operation: "create",
          policy: { mode: "light", maxConcurrent: 2, retainWorkers: true },
          nodes: [node("sender"), node("receiver")],
        });
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("mail-run"),
          expectedRevision: plan.revision,
          operation: "run",
        });
        yield* reactor.drain(ROOT);
        plan = yield* plans.read(target);
        const sender = plan.nodes[0]!.attempts[0]!.workerThreadId;
        const receiver = plan.nodes[1]!.attempts[0]!.workerThreadId;
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("receiver-active"),
          threadId: receiver,
          createdAt: NOW,
          session: {
            threadId: receiver,
            status: "running",
            activeTurnId: TurnId.make("active-receiver"),
            providerName: "codex",
            runtimeMode: "approval-required",
            lastError: null,
            updatedAt: NOW,
          },
        });
        const message = {
          callerThreadId: sender,
          rootThreadId: ROOT,
          commandId: CommandId.make("peer-message"),
          expectedRevision: 0,
          operation: "message" as const,
          recipientThreadIds: [receiver],
          channelId: null,
          text: "Check the dependency artifact",
        };
        yield* plans.mailboxWrite(message);
        yield* plans.mailboxWrite(message);
        let inbox = yield* plans.mailboxRead({ callerThreadId: receiver, rootThreadId: ROOT });
        expect(inbox.envelopes).toHaveLength(1);
        expect(inbox.envelopes[0]!.delivery).toBe("pending");
        expect((yield* plans.read(target)).revision).toBe(plan.revision);
        inbox = yield* plans.mailboxWrite({
          callerThreadId: receiver,
          rootThreadId: ROOT,
          commandId: CommandId.make("ack"),
          expectedRevision: inbox.revision,
          operation: "ack",
          envelopeIds: [inbox.envelopes[0]!.id],
        });
        expect(inbox.envelopes[0]!.delivery).toBe("read");
        const foreign = yield* plans
          .mailboxWrite({
            ...message,
            commandId: CommandId.make("foreign-peer"),
            expectedRevision: inbox.revision,
            recipientThreadIds: [ThreadId.make("foreign")],
          })
          .pipe(Effect.result);
        expect(foreign._tag).toBe("Failure");
        inbox = yield* plans.mailboxWrite({
          callerThreadId: ROOT,
          rootThreadId: ROOT,
          commandId: CommandId.make("channel"),
          expectedRevision: inbox.revision,
          operation: "channelCreate",
          channelId: "review",
          name: "Review",
          members: [sender, receiver],
        });
        inbox = yield* plans.mailboxWrite({
          callerThreadId: ROOT,
          rootThreadId: ROOT,
          commandId: CommandId.make("close-channel"),
          expectedRevision: inbox.revision,
          operation: "channelClose",
          channelId: "review",
          closed: true,
        });
        const closed = yield* plans
          .mailboxWrite({
            ...message,
            commandId: CommandId.make("closed-message"),
            expectedRevision: inbox.revision,
            channelId: "review",
            recipientThreadIds: [],
          })
          .pipe(Effect.result);
        expect(closed._tag).toBe("Failure");
        inbox = yield* plans.mailboxWrite({
          callerThreadId: ROOT,
          rootThreadId: ROOT,
          commandId: CommandId.make("reopen-channel"),
          expectedRevision: inbox.revision,
          operation: "channelClose",
          channelId: "review",
          closed: false,
        });
        inbox = yield* plans.mailboxWrite({
          ...message,
          commandId: CommandId.make("channel-message"),
          expectedRevision: inbox.revision,
          channelId: "review",
          recipientThreadIds: [],
        });
        inbox = yield* plans.mailboxWrite({
          callerThreadId: sender,
          rootThreadId: ROOT,
          commandId: CommandId.make("context"),
          expectedRevision: inbox.revision,
          operation: "contextWrite",
          key: "contract",
          value: "v1",
        });
        const conflict = yield* plans
          .mailboxWrite({
            callerThreadId: receiver,
            rootThreadId: ROOT,
            commandId: CommandId.make("stale-context"),
            expectedRevision: 0,
            operation: "contextWrite",
            key: "contract",
            value: "v2",
          })
          .pipe(Effect.result);
        expect(conflict._tag).toBe("Failure");
        inbox = yield* plans.mailboxWrite({
          callerThreadId: sender,
          rootThreadId: ROOT,
          commandId: CommandId.make("delete-context"),
          expectedRevision: inbox.revision,
          operation: "contextWrite",
          key: "contract",
          value: null,
        });
        expect(inbox.context).toEqual([]);
      }),
  );
  it.effect(
    "atomically dispatches bounded prerequisites, deduplicates wakes and rejects stale artifacts",
    () =>
      Effect.gen(function* () {
        yield* setup;
        const plans = yield* Plans.CoordinationPlans;
        const reactor = yield* Reactor.CoordinationReactor;
        const engine = yield* OrchestrationEngineService;
        const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "diamond" };
        let plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("create-plan"),
          expectedRevision: 0,
          operation: "create",
          policy: { mode: "light", maxConcurrent: 2, retainWorkers: true },
          nodes: [node("a"), node("b"), node("join", ["a", "b"])],
        });
        plan = yield* plans.write({
          ...target,
          commandId: CommandId.make("run-plan"),
          expectedRevision: plan.revision,
          operation: "run",
        });
        yield* reactor.drain(ROOT);
        plan = yield* plans.read(target);
        expect(plan.nodes.map((item) => item.attempts.length)).toEqual([1, 1, 0]);
        yield* reactor.drain(ROOT);
        expect((yield* plans.read(target)).revision).toBe(plan.revision);
        const a = plan.nodes[0]!.attempts[0]!;
        const turn = TurnId.make("turn-a");
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("start-a"),
          threadId: a.workerThreadId,
          createdAt: NOW,
          session: {
            threadId: a.workerThreadId,
            status: "running",
            activeTurnId: turn,
            providerName: "codex",
            runtimeMode: "approval-required",
            lastError: null,
            updatedAt: NOW,
          },
        });
        const report = {
          ...target,
          callerThreadId: a.workerThreadId,
          commandId: CommandId.make("report-a"),
          expectedRevision: plan.revision,
          operation: "complete" as const,
          nodeId: "a",
          attemptNumber: 1,
          workerThreadId: a.workerThreadId,
          turnId: turn,
          artifact,
        };
        plan = yield* plans.write(report);
        expect(plan.nodes[0]!.attempts[0]!.status).toBe("accepted");
        expect(plan.nodes[0]!.attempts[0]!.pendingArtifact).toEqual(artifact);
        const stale = yield* plans
          .write({
            ...report,
            commandId: CommandId.make("stale"),
            expectedRevision: plan.revision,
            turnId: TurnId.make("old-turn"),
          })
          .pipe(Effect.result);
        expect(stale._tag).toBe("Failure");
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("idle-a"),
          threadId: a.workerThreadId,
          createdAt: NOW,
          session: {
            threadId: a.workerThreadId,
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
          commandId: CommandId.make("done-a"),
          threadId: a.workerThreadId,
          turnId: turn,
          completedAt: NOW,
          checkpointRef: CheckpointRef.make("refs/t3/checkpoints/a"),
          status: "ready",
          files: [],
          checkpointTurnCount: 1,
          createdAt: NOW,
        });
        const stolen = yield* engine
          .dispatch({
            type: "thread.worker.send",
            commandId: CommandId.make("steal-before-settle"),
            callerThreadId: ROOT,
            threadId: a.workerThreadId,
            text: "Unrelated next task",
            createdAt: NOW,
          })
          .pipe(Effect.result);
        expect(stolen._tag).toBe("Failure");
        yield* reactor.drain(ROOT);
        plan = yield* plans.read(target);
        expect(plan.nodes[0]!.attempts[0]!.status).toBe("succeeded");
        expect(plan.nodes[2]!.attempts).toHaveLength(0);
        const b = plan.nodes[1]!.attempts[0]!;
        const bTurn = TurnId.make("turn-b");
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("start-b"),
          threadId: b.workerThreadId,
          createdAt: NOW,
          session: {
            threadId: b.workerThreadId,
            status: "running",
            activeTurnId: bTurn,
            providerName: "codex",
            runtimeMode: "approval-required",
            lastError: null,
            updatedAt: NOW,
          },
        });
        plan = yield* plans.write({
          ...target,
          callerThreadId: b.workerThreadId,
          commandId: CommandId.make("report-b"),
          expectedRevision: plan.revision,
          operation: "complete",
          nodeId: "b",
          attemptNumber: 1,
          workerThreadId: b.workerThreadId,
          turnId: bTurn,
          artifact,
        });
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("idle-b"),
          threadId: b.workerThreadId,
          createdAt: NOW,
          session: {
            threadId: b.workerThreadId,
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
          commandId: CommandId.make("done-b"),
          threadId: b.workerThreadId,
          turnId: bTurn,
          completedAt: NOW,
          checkpointRef: CheckpointRef.make("refs/t3/checkpoints/b"),
          status: "ready",
          files: [],
          checkpointTurnCount: 1,
          createdAt: NOW,
        });
        yield* reactor.drain(ROOT);
        plan = yield* plans.read(target);
        expect(plan.nodes[1]!.attempts[0]!.status).toBe("succeeded");
        expect([a.workerThreadId, b.workerThreadId]).toContain(
          plan.nodes[2]!.attempts[0]!.workerThreadId,
        );
        expect(plan.nodes[2]!.attempts[0]!.dependencyVersions).toEqual([
          { nodeId: "a", attemptNumber: 1 },
          { nodeId: "b", attemptNumber: 1 },
        ]);
      }),
  );
  it.effect("rejects deep ungated work and exact-command changed payload replay", () =>
    Effect.gen(function* () {
      yield* setup;
      const plans = yield* Plans.CoordinationPlans;
      const input = {
        callerThreadId: ROOT,
        rootThreadId: ROOT,
        planId: "plan",
        commandId: CommandId.make("create"),
        expectedRevision: 0,
        operation: "create" as const,
        policy: { mode: "light" as const, maxConcurrent: 1, retainWorkers: true },
        nodes: [node("work")],
      };
      const deep = yield* plans
        .write({ ...input, policy: { ...input.policy, mode: "deep" } })
        .pipe(Effect.result);
      expect(deep._tag).toBe("Failure");
      const first = yield* plans.write(input);
      const same = yield* plans.write(input);
      expect(same).toEqual(first);
      const changed = yield* plans
        .write({ ...input, nodes: [node("different")] })
        .pipe(Effect.result);
      expect(changed._tag).toBe("Failure");
    }),
  );
  it.effect("executes one report-only continuation and immutable deep repair gates", () =>
    Effect.gen(function* () {
      yield* setup;
      const plans = yield* Plans.CoordinationPlans;
      const reactor = yield* Reactor.CoordinationReactor;
      const target = { callerThreadId: ROOT, rootThreadId: ROOT, planId: "deep-repair" };
      let plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("deep-repair-create"),
        expectedRevision: 0,
        operation: "create",
        policy: { mode: "deep", maxConcurrent: 1, retainWorkers: true },
        nodes: [node("task"), { ...node("verify", ["task"]), kind: "verify", gateScope: ["task"] }],
      });
      plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("deep-repair-run"),
        expectedRevision: plan.revision,
        operation: "run",
      });
      yield* reactor.drain(ROOT, plan.id);
      plan = yield* plans.read(target);
      const originalMessage = plan.nodes[0]!.attempts[0]!.dispatchMessageId;
      plan = yield* executeAttempt(plan, "task");
      expect(plan.nodes[0]!.attempts).toHaveLength(1);
      expect(plan.nodes[0]!.attempts[0]!.handoffRequested).toBe(true);
      expect(plan.nodes[0]!.attempts[0]!.initialDispatchMessageId).toBe(originalMessage);
      expect(plan.nodes[1]!.attempts).toHaveLength(0);
      plan = yield* executeAttempt(plan, "task", artifact);
      expect(plan.nodes[0]!.attempts[0]!.status).toBe("succeeded");
      plan = yield* executeAttempt(plan, "verify", { ...artifact, verdict: "repair" });
      const failedGate = plan.nodes[1]!.attempts[0]!;
      expect(failedGate.status).toBe("failed");
      plan = yield* plans.write({
        ...target,
        commandId: CommandId.make("deep-repair-frontier"),
        expectedRevision: plan.revision,
        operation: "repair",
        nodeId: "verify",
        successorGateId: "verify-2",
        repairs: [node("fix", ["task"])],
      });
      yield* reactor.drain(ROOT, plan.id);
      plan = yield* plans.read(target);
      expect(plan.nodes[1]!.retired).toBe(true);
      expect(plan.nodes[1]!.attempts[0]).toEqual(failedGate);
      expect(plan.nodes[3]!.attempts).toHaveLength(0);
      plan = yield* executeAttempt(plan, "fix", artifact);
      plan = yield* executeAttempt(plan, "verify-2", { ...artifact, verdict: "pass" });
      expect(plan.nodes[3]!.attempts[0]!.status).toBe("succeeded");
    }),
  );
});
