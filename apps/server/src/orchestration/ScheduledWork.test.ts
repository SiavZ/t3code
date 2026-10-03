import {
  captureNativeUnattendedActivation,
  validateNativeUnattendedAuthority,
} from "./nativeUnattendedAuthority.ts";
import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type ScheduledWorkCreateInput,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as FileSystem from "effect/FileSystem";
import { TestClock } from "effect/testing";
import { ServerConfig } from "../config.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import {
  SqlitePersistenceMemory,
  makeSqlitePersistenceLive,
} from "../persistence/Layers/Sqlite.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ServerSettings from "../serverSettings.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import * as OwnedWorkers from "./OwnedWorkers.ts";
import * as ScheduledWork from "./ScheduledWork.ts";
import * as Activation from "./ScheduledWorkActivation.ts";
import * as UnattendedGrants from "./UnattendedGrants.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";

import { EnvironmentId } from "@t3tools/contracts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import * as BackgroundJobs from "../background/BackgroundJobs.ts";
import * as JobAuthority from "../background/BackgroundJobAuthority.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { AutomationToolkit } from "../mcp/toolkits/automation/tools.ts";
import { AutomationToolkitHandlersLive } from "../mcp/toolkits/automation/handlers.ts";

const NOW = "2026-10-03T00:00:00.000Z";
const ROOT = ThreadId.make("schedule-root");
const PROJECT = ProjectId.make("schedule-project");
const MODEL = { instanceId: ProviderInstanceId.make("codex"), model: "test" };
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
    Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
    Layer.provide(
      Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
        resolve: () => Effect.succeed(null),
      }),
    ),
    Layer.provideMerge(
      databasePath ? makeSqlitePersistenceLive(databasePath) : SqlitePersistenceMemory,
    ),
    Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "t3-scheduler-test-" })),
    Layer.provideMerge(NodeServices.layer),
    Layer.provideMerge(
      ServerSettings.layerTest({
        enableAgentBrowserAccess: true,
        agentToolCapabilities: ["automation", "background-jobs"],
      }),
    ),
  );
  const authority = Layer.mergeAll(OwnedWorkers.layer, UnattendedGrants.layer).pipe(
    Layer.provideMerge(core),
  );
  const activation = Activation.layer.pipe(Layer.provideMerge(authority));
  return ScheduledWork.layer.pipe(Layer.provideMerge(activation));
}
const setup = Effect.gen(function* () {
  const engine = yield* OrchestrationEngineService;
  yield* engine.dispatch({
    type: "project.create",
    commandId: CommandId.make("project"),
    projectId: PROJECT,
    title: "Scheduler",
    workspaceRoot: process.cwd(),
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
  const grants = yield* UnattendedGrants.UnattendedGrants;
  yield* grants.create(
    {
      id: "consent",
      callerThreadId: ROOT,
      ceiling: { runtimeMode: "approval-required", mcpCapabilities: ["workers"] },
      hostJobs: false,
    },
    { source: "client" },
  );
});
const request = (id: string): ScheduledWorkCreateInput => ({
  id,
  callerThreadId: ROOT,
  target: { type: "resume", threadId: ROOT },
  prompt: "Scheduled test",
  delayMs: 1000,
  onBusy: "wait",
  grantId: "consent",
});
const foreground = (id: string) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    yield* engine.dispatch({
      type: "thread.turn.start",
      commandId: CommandId.make(id),
      threadId: ROOT,
      message: { messageId: MessageId.make(id), role: "user", text: "Foreground", attachments: [] },
      runtimeMode: "approval-required",
      interactionMode: "default",
      createdAt: NOW,
    });
  });

it.effect(
  "retained unattended MCP grant cannot borrow broader scheduling or host-job consent",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const engine = yield* OrchestrationEngineService;
        const grants = yield* UnattendedGrants.UnattendedGrants;
        yield* engine.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("nested-full-shell"),
          threadId: ROOT,
          runtimeMode: "full-access",
          createdAt: NOW,
        });
        yield* grants.create(
          {
            id: "G",
            callerThreadId: ROOT,
            ceiling: {
              runtimeMode: "approval-required",
              mcpCapabilities: ["workers", "automation", "background-jobs"],
            },
            hostJobs: false,
          },
          { source: "client" },
        );
        yield* grants.create(
          {
            id: "H",
            callerThreadId: ROOT,
            ceiling: {
              runtimeMode: "full-access",
              mcpCapabilities: ["workers", "automation", "background-jobs"],
            },
            hostJobs: true,
          },
          { source: "client" },
        );
        const context: McpInvocationContext.McpInvocationScope = {
          environmentId: EnvironmentId.make("test"),
          threadId: ROOT,
          providerSessionId: "retained-G",
          providerInstanceId: MODEL.instanceId,
          capabilities: new Set(["workers", "automation", "background-jobs"]),
          issuedAt: 1,
          unattendedAuthority: {
            grantId: "G",
            grantRevision: 1,
            ownerThreadId: ROOT,
            runtimeModeCeiling: "approval-required",
            mcpCapabilityCeiling: ["workers", "automation", "background-jobs"],
          },
        };
        yield* Effect.gen(function* () {
          const toolkit = yield* AutomationToolkit.pipe(
            Effect.provide(AutomationToolkitHandlersLive),
          );
          const nested = { ...request("cross-grant"), grantId: "H" };
          expect(
            (yield* toolkit
              .handle("schedule_create", nested)
              .pipe(Stream.unwrap, Stream.runDrain, Effect.result))._tag,
          ).toBe("Failure");
          yield* toolkit
            .handle("schedule_create", { ...request("same-grant"), grantId: "G" })
            .pipe(Stream.unwrap, Stream.runDrain);
          const schedules = yield* ScheduledWork.ScheduledWork;
          expect(
            (yield* schedules.get({ callerThreadId: ROOT, id: "same-grant" })).ceiling.runtimeMode,
          ).toBe("approval-required");
          expect(
            (yield* toolkit
              .handle("background_job_start", {
                id: "cross-host",
                command: process.execPath,
                args: ["-e", "process.exit(0)"],
                timeoutMs: 1000,
                maxOutputBytes: 1024,
              })
              .pipe(Stream.unwrap, Stream.runDrain, Effect.result))._tag,
          ).toBe("Failure");
          yield* grants.revoke({ callerThreadId: ROOT, id: "G" }, { source: "client" });
          expect(
            (yield* toolkit
              .handle("schedule_create", { ...request("revoked-nested"), grantId: "G" })
              .pipe(Stream.unwrap, Stream.runDrain, Effect.result))._tag,
          ).toBe("Failure");
        }).pipe(Effect.provideService(McpInvocationContext.McpInvocationContext, context));
      }).pipe(
        Effect.provide(
          BackgroundJobs.layer.pipe(
            Layer.provideMerge(JobAuthority.layer.pipe(Layer.provideMerge(WorkspacePaths.layer))),
            Layer.provideMerge(testLayer()),
          ),
        ),
      ),
    ),
);

it.effect(
  "automatically dispatches once at TestClock due time, keeps deterministic IDs and recovers accepted receipt",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const scheduled = yield* ScheduledWork.ScheduledWork;
        const engine = yield* OrchestrationEngineService;
        const sql = yield* SqlClient.SqlClient;
        const created = yield* scheduled.create(request("once"));
        expect(created.dueAt).toBe("1970-01-01T00:00:01.000Z");
        const events = yield* engine.subscribeDomainEvents;
        yield* scheduled.start;
        yield* TestClock.adjust(999);
        expect((yield* scheduled.get({ callerThreadId: ROOT, id: "once" })).state).toBe("queued");
        yield* TestClock.adjust(1);
        yield* events.pipe(
          Stream.filter((event) => event.type === "thread.turn-start-requested"),
          Stream.runHead,
        );
        yield* scheduled.drainDue;
        const accepted = yield* scheduled.get({ callerThreadId: ROOT, id: "once" });
        expect(accepted.state).toBe("accepted");
        expect(accepted.commandId).toBe("scheduled:once");
        const crashed = {
          ...accepted,
          state: "dispatching",
          acceptedSequence: null,
          executionThreadId: null,
        };
        yield* sql`UPDATE scheduled_work SET state = 'dispatching', document_json = ${JSON.stringify(crashed)} WHERE id = 'once'`;
        yield* scheduled.reconcile;
        expect((yield* scheduled.get({ callerThreadId: ROOT, id: "once" })).acceptedSequence).toBe(
          accepted.acceptedSequence,
        );
        const messages = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM projection_thread_messages WHERE message_id = ${accepted.messageId}`;
        expect(messages[0]?.count).toBe(1);
      }).pipe(Effect.provide(testLayer())),
    ),
);

it.effect(
  "foreground admission wins atomically, busy waits without rejected receipt and exact cancellation cannot stop later work",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const scheduled = yield* ScheduledWork.ScheduledWork;
        const engine = yield* OrchestrationEngineService;
        const snapshots = yield* ProjectionSnapshotQuery;
        yield* scheduled.create({ ...request("busy"), delayMs: 0 });
        yield* foreground("human-first");
        yield* scheduled.drainDue;
        expect((yield* scheduled.get({ callerThreadId: ROOT, id: "busy" })).state).toBe("blocked");
        const sql = yield* SqlClient.SqlClient;
        const receipts = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM orchestration_command_receipts WHERE command_id = 'scheduled:busy'`;
        expect(receipts[0]?.count).toBe(0);
        yield* scheduled.cancel({ callerThreadId: ROOT, id: "busy" });
        const loaded = yield* snapshots.getWorkerState(ROOT);
        expect(Option.isSome(loaded) && loaded.value.pendingMessageId).toBe("human-first");
        // Wrong activation cannot cancel the foreground message, even in serialized dispatch.
        const denied = yield* engine
          .dispatch({
            type: "thread.turn.interrupt",
            commandId: CommandId.make("wrong-cancel"),
            threadId: ROOT,
            expectedMessageId: MessageId.make("scheduled:busy"),
            createdAt: NOW,
          })
          .pipe(Effect.result);
        expect(denied._tag).toBe("Failure");
        const after = yield* snapshots.getWorkerState(ROOT);
        expect(Option.isSome(after) && after.value.pendingMessageId).toBe("human-first");
      }).pipe(Effect.provide(testLayer())),
    ),
);

it.effect(
  "revoked consent, expired deadlines, foreign ownership and busy-fail are visible terminal or blocked states",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const scheduled = yield* ScheduledWork.ScheduledWork;
        const grants = yield* UnattendedGrants.UnattendedGrants;
        yield* scheduled.create({
          ...request("expired"),
          latestStartAt: "1970-01-01T00:00:02.000Z",
        });
        yield* TestClock.adjust(3000);
        yield* scheduled.drainDue;
        expect((yield* scheduled.get({ callerThreadId: ROOT, id: "expired" })).state).toBe(
          "failed",
        );
        yield* scheduled.create({ ...request("revoked"), delayMs: 0 });
        yield* grants.revoke({ callerThreadId: ROOT, id: "consent" }, { source: "client" });
        yield* scheduled.drainDue;
        expect((yield* scheduled.get({ callerThreadId: ROOT, id: "revoked" })).reason).toContain(
          "revoked",
        );
        const foreign = yield* scheduled
          .get({ callerThreadId: ThreadId.make("foreign"), id: "revoked" })
          .pipe(Effect.result);
        expect(foreign._tag).toBe("Failure");
      }).pipe(Effect.provide(testLayer())),
    ),
);

it.effect(
  "scheduled spawn lowers inherited runtime mode and retries preserve worker identity",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const engine = yield* OrchestrationEngineService;
        const scheduled = yield* ScheduledWork.ScheduledWork;
        const snapshots = yield* ProjectionSnapshotQuery;
        yield* engine.dispatch({
          type: "thread.runtime-mode.set",
          commandId: CommandId.make("full-parent"),
          threadId: ROOT,
          runtimeMode: "full-access",
          createdAt: NOW,
        });
        const created = yield* scheduled.create({
          ...request("spawn-ceiling"),
          target: { type: "spawn", label: "bounded", modelSelection: MODEL },
          delayMs: 0,
        });
        expect(created.ceiling.runtimeMode).toBe("approval-required");
        yield* scheduled.drainDue;
        const accepted = yield* scheduled.get({ callerThreadId: ROOT, id: "spawn-ceiling" });
        expect(accepted.state).toBe("accepted");
        const worker = yield* snapshots.getWorkerState(accepted.executionThreadId!);
        expect(Option.isSome(worker) && worker.value.thread.runtimeMode).toBe("approval-required");
        expect(Option.isSome(worker) && worker.value.pendingMessageId).toBe(created.messageId);
        const origin = yield* captureNativeUnattendedActivation(
          yield* SqlClient.SqlClient,
          accepted.executionThreadId!,
        );
        expect(origin?.authority.grantId).toBe("consent");
        const workers = yield* OwnedWorkers.OwnedWorkers;
        const child = yield* workers.spawn({
          commandId: CommandId.make("grant-child"),
          callerThreadId: accepted.executionThreadId!,
          label: "child",
          prompt: "bounded child",
          modelSelection: MODEL,
        });
        const childOrigin = yield* captureNativeUnattendedActivation(
          yield* SqlClient.SqlClient,
          child.workerThreadId,
        );
        expect(childOrigin?.authority.grantId).toBe("consent");
        yield* engine.dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make("child-settle"),
          threadId: child.workerThreadId,
          expectedMessageId: childOrigin!.messageId,
          createdAt: NOW,
        });
        yield* workers.send({
          commandId: CommandId.make("child-follow-up"),
          callerThreadId: accepted.executionThreadId!,
          workerThreadId: child.workerThreadId,
          text: "still bounded",
        });
        expect(
          (yield* captureNativeUnattendedActivation(
            yield* SqlClient.SqlClient,
            child.workerThreadId,
          ))?.authority.grantId,
        ).toBe("consent");
        const grants = yield* UnattendedGrants.UnattendedGrants;
        yield* grants.revoke({ callerThreadId: ROOT, id: "consent" }, { source: "client" });
        expect(
          yield* validateNativeUnattendedAuthority(
            yield* SqlClient.SqlClient,
            child.workerThreadId,
            "approval-required",
            childOrigin,
          ),
        ).toBe(false);
        yield* scheduled.drainDue;
        expect(
          (yield* scheduled.get({ callerThreadId: ROOT, id: "spawn-ceiling" })).executionThreadId,
        ).toBe(accepted.executionThreadId);
      }).pipe(Effect.provide(testLayer())),
    ),
);

it.effect("spawn busy-wait retries the same command after owned capacity is released", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setup;
      const workers = yield* OwnedWorkers.OwnedWorkers;
      const schedules = yield* ScheduledWork.ScheduledWork;
      const sql = yield* SqlClient.SqlClient;
      const occupied = [];
      for (let i = 0; i < 4; i++)
        occupied.push(
          yield* workers.spawn({
            commandId: CommandId.make(`capacity:${i}`),
            callerThreadId: ROOT,
            label: `occupied ${i}`,
            prompt: "wait",
            modelSelection: MODEL,
          }),
        );
      yield* schedules.create({
        ...request("capacity-retry"),
        target: { type: "spawn", label: "retry", modelSelection: MODEL },
        delayMs: 0,
      });
      yield* schedules.drainDue;
      expect((yield* schedules.get({ callerThreadId: ROOT, id: "capacity-retry" })).state).toBe(
        "blocked",
      );
      expect(
        (yield* sql`SELECT command_id FROM orchestration_command_receipts WHERE command_id = 'scheduled:capacity-retry'`)
          .length,
      ).toBe(0);
      yield* workers.stop({
        commandId: CommandId.make("release-capacity"),
        callerThreadId: ROOT,
        workerThreadId: occupied[0]!.workerThreadId,
      });
      yield* schedules.drainDue;
      expect((yield* schedules.get({ callerThreadId: ROOT, id: "capacity-retry" })).state).toBe(
        "accepted",
      );
    }).pipe(Effect.provide(testLayer())),
  ),
);

for (const primaryState of ["completed", "error", "interrupted"] as const) {
  it.effect(`primary ${primaryState} remains cancellable until native background quiesces`, () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const schedules = yield* ScheduledWork.ScheduledWork;
        const sql = yield* SqlClient.SqlClient;
        const liveness = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
        const id = `live-${primaryState}`;
        yield* schedules.create({ ...request(id), delayMs: 0 });
        yield* schedules.drainDue;
        const accepted = yield* schedules.get({ callerThreadId: ROOT, id });
        yield* sql`UPDATE projection_turns SET state = ${primaryState}, turn_id = ${`native:${id}`} WHERE pending_message_id = ${accepted.messageId}`;
        yield* sql`UPDATE projection_threads SET latest_turn_id = ${`native:${id}`} WHERE thread_id = ${ROOT}`;
        liveness.recordTaskLiveness({
          threadId: ROOT,
          taskId: "owned-background",
          taskType: "agent",
          status: "running",
          kind: "started",
        });
        yield* schedules.reconcile;
        expect((yield* schedules.get({ callerThreadId: ROOT, id })).state).toBe("running");
        yield* schedules.cancel({ callerThreadId: ROOT, id });
        expect(
          (yield* sql`SELECT message_id FROM projection_turn_cancellations WHERE message_id = ${accepted.messageId}`)
            .length,
        ).toBe(1);
        liveness.clearThreadLiveness(ROOT);
        yield* schedules.reconcile;
        expect(["completed", "failed", "cancelled"]).toContain(
          (yield* schedules.get({ callerThreadId: ROOT, id })).state,
        );
      }).pipe(Effect.provide(testLayer())),
    ),
  );
}

it.effect("exact retries preserve relative due time and reject changed delay or deadline", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* setup;
      const schedules = yield* ScheduledWork.ScheduledWork;
      const original = yield* schedules.create(request("retry-bounds"));
      yield* TestClock.adjust(50);
      expect((yield* schedules.create(request("retry-bounds"))).dueAt).toBe(original.dueAt);
      expect(
        (yield* schedules.create({ ...request("retry-bounds"), delayMs: 2000 }).pipe(Effect.result))
          ._tag,
      ).toBe("Failure");
      expect(
        (yield* schedules
          .create({ ...request("retry-bounds"), latestStartAt: "1970-01-01T00:00:03.000Z" })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(testLayer())),
  ),
);

it.effect(
  "persisted activation authority limits native mode, observes revocation and clears only on explicit normal start",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const schedules = yield* ScheduledWork.ScheduledWork;
        const snapshots = yield* ProjectionSnapshotQuery;
        const sql = yield* SqlClient.SqlClient;
        yield* schedules.create({ ...request("native-consent"), delayMs: 0 });
        yield* schedules.drainDue;
        const current = yield* snapshots.getThreadActivationAuthority(ROOT);
        expect(Option.isSome(current) && current.value.mcpCapabilityCeiling).toEqual(["workers"]);
        expect(yield* validateNativeUnattendedAuthority(sql, ROOT, "approval-required")).toBe(true);
        expect(yield* validateNativeUnattendedAuthority(sql, ROOT, "full-access")).toBe(false);
        expect(yield* validateNativeUnattendedAuthority(sql, ROOT, "auto")).toBe(false);
        expect(yield* validateNativeUnattendedAuthority(sql, ROOT, "auto-accept-edits")).toBe(
          false,
        );
        const captured = yield* captureNativeUnattendedActivation(sql, ROOT);
        const cancelReceipt = yield* (yield* OrchestrationEngineService).dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make("authority-cancel"),
          threadId: ROOT,
          expectedMessageId: captured!.messageId,
          createdAt: NOW,
        });
        const grants = yield* UnattendedGrants.UnattendedGrants;
        yield* grants.revoke({ callerThreadId: ROOT, id: "consent" }, { source: "client" });
        expect(yield* validateNativeUnattendedAuthority(sql, ROOT, "approval-required")).toBe(
          false,
        );
        yield* foreground("explicit-normal-after-revoke");
        expect(Option.isNone(yield* snapshots.getThreadActivationAuthority(ROOT))).toBe(true);
        const staleAck = yield* (yield* OrchestrationEngineService)
          .dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("stale-stop-ack"),
            threadId: ROOT,
            expectedMessageId: captured!.messageId,
            expectedActivationSequence: cancelReceipt.sequence,
            session: {
              threadId: ROOT,
              status: "stopped",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: null,
              lastError: null,
              updatedAt: NOW,
            },
            createdAt: NOW,
          })
          .pipe(Effect.result);
        expect(staleAck._tag).toBe("Failure");
        const sequenceOnlyAck = yield* (yield* OrchestrationEngineService)
          .dispatch({
            type: "thread.session.set",
            commandId: CommandId.make("stale-turn-only-stop-ack"),
            threadId: ROOT,
            expectedActivationSequence: cancelReceipt.sequence,
            session: {
              threadId: ROOT,
              status: "stopped",
              providerName: "codex",
              runtimeMode: "approval-required",
              activeTurnId: null,
              lastError: null,
              updatedAt: NOW,
            },
            createdAt: NOW,
          })
          .pipe(Effect.result);
        expect(sequenceOnlyAck._tag).toBe("Failure");
        expect(
          yield* validateNativeUnattendedAuthority(sql, ROOT, "approval-required", captured),
        ).toBe(false);
        expect(yield* validateNativeUnattendedAuthority(sql, ROOT, "approval-required")).toBe(true);
      }).pipe(Effect.provide(testLayer())),
    ),
);

it.effect(
  "cancel before native activation settles from durable tombstone with no bound session",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* setup;
        const schedules = yield* ScheduledWork.ScheduledWork;
        yield* schedules.create({ ...request("cancel-no-session"), delayMs: 0 });
        yield* schedules.drainDue;
        yield* schedules.cancel({ callerThreadId: ROOT, id: "cancel-no-session" });
        yield* schedules.reconcile;
        expect(
          (yield* schedules.get({ callerThreadId: ROOT, id: "cancel-no-session" })).state,
        ).toBe("cancelled");
      }).pipe(Effect.provide(testLayer())),
    ),
);

it.effect("reopens real SQLite and dispatches persisted one-shot only once", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "t3-scheduler-reopen-" });
      const filename = `${directory}/state.sqlite`;
      yield* Effect.gen(function* () {
        yield* setup;
        const service = yield* ScheduledWork.ScheduledWork;
        yield* service.create({ ...request("restart"), delayMs: 0 });
      }).pipe(Effect.provide(testLayer(filename)));
      yield* Effect.gen(function* () {
        const service = yield* ScheduledWork.ScheduledWork;
        yield* service.drainDue;
        expect((yield* service.get({ callerThreadId: ROOT, id: "restart" })).state).toBe(
          "accepted",
        );
      }).pipe(Effect.provide(testLayer(filename)));
      yield* Effect.gen(function* () {
        const service = yield* ScheduledWork.ScheduledWork;
        yield* service.reconcile;
        yield* service.drainDue;
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM projection_thread_messages WHERE message_id = 'scheduled:restart'`;
        expect(rows[0]?.count).toBe(1);
      }).pipe(Effect.provide(testLayer(filename)));
    }).pipe(Effect.provide(NodeServices.layer)),
  ),
);
