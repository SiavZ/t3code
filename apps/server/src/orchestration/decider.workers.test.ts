import { it } from "@effect/vitest";
import { NodeServices } from "@effect/platform-node";
import { expect } from "vite-plus/test";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import {
  CommandId,
  MessageId,
  OrchestrationThread,
  OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationCommand,
  type OrchestrationReadModel,
  type ThreadWorkerMetadata,
} from "@t3tools/contracts";
import { decideOrchestrationCommand } from "./decider.ts";
import { projectEvent } from "./projector.ts";
import type { WorkerThreadState } from "./Services/ProjectionSnapshotQuery.ts";

const NOW = "2026-10-02T17:00:00.000Z";
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" };
const rootId = ThreadId.make("root");
const workerId = ThreadId.make("worker");
const decodeThread = Schema.decodeUnknownSync(OrchestrationThread);
const decodeThreadShell = Schema.decodeUnknownSync(OrchestrationThreadShell);
const metadata: ThreadWorkerMetadata = {
  ownerThreadId: rootId,
  rootThreadId: rootId,
  depth: 1,
  spawnCommandId: CommandId.make("spawn-worker"),
  spawnFingerprint: "fingerprint",
  label: "worker",
  runtimeModeCeiling: "approval-required",
  mcpCapabilityCeiling: ["workers"],
  stopRequestedAt: null,
  lastStopSequence: null,
};
function thread(id = rootId, worker: ThreadWorkerMetadata | null = null) {
  return decodeThread({
    id,
    worker,
    projectId: "project",
    title: id,
    modelSelection,
    runtimeMode: "approval-required",
    interactionMode: "default",
    branch: "main",
    worktreePath: "/workspace",
    pullRequests: [],
    branchPullRequest: null,
    latestTurn: null,
    createdAt: NOW,
    updatedAt: NOW,
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    unsettledAt: null,
    snoozedUntil: null,
    snoozedAt: null,
    deletedAt: null,
    messages: [],
    activities: [],
    proposedPlans: [],
    checkpoints: [],
    session: null,
  });
}
function state(threads = [thread()]): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    updatedAt: NOW,
    threads,
    projects: [
      {
        id: ProjectId.make("project"),
        title: "project",
        workspaceRoot: "/workspace",
        defaultModelSelection: null,
        scripts: [],
        createdAt: NOW,
        updatedAt: NOW,
        deletedAt: null,
      },
    ],
  };
}
function spawn(id = workerId, callerThreadId = rootId): OrchestrationCommand {
  return {
    type: "thread.worker.spawn",
    commandId: CommandId.make(`spawn-${id}`),
    threadId: id,
    callerThreadId,
    label: id,
    prompt: "Inspect the source",
    modelSelection,
    mcpCapabilityCeiling: ["workers"],
    spawnFingerprint: "fingerprint",
    createdAt: NOW,
  };
}
function live(id: string, rootThreadId = rootId): WorkerThreadState {
  return {
    thread: decodeThreadShell({
      ...thread(ThreadId.make(id), { ...metadata, rootThreadId }),
      latestUserMessageAt: NOW,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      backgroundLiveness: null,
      planProgress: null,
    }),
    pendingMessageId: MessageId.make(`pending-${id}`),
  };
}
function turnStart(): OrchestrationCommand {
  return {
    type: "thread.turn.start",
    commandId: CommandId.make("start"),
    threadId: workerId,
    message: {
      messageId: MessageId.make("followup"),
      role: "user",
      text: "Continue",
      attachments: [],
    },
    runtimeMode: "full-access",
    interactionMode: "default",
    createdAt: NOW,
  };
}

it.layer(NodeServices.layer)("owned worker state decisions", (it) => {
  it.effect("stopped workers cannot spawn or send through in-flight agent invocations", () =>
    Effect.gen(function* () {
      const nestedId = ThreadId.make("nested");
      const parent = thread(workerId, { ...metadata, stopRequestedAt: NOW, lastStopSequence: 1 });
      const nested = thread(nestedId, { ...metadata, ownerThreadId: workerId, depth: 2 });
      const readModel = state([thread(), parent, nested]);
      expect(
        yield* decideOrchestrationCommand({
          command: spawn(ThreadId.make("new-nested"), workerId),
          readModel,
          workerStates: [],
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
      expect(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.worker.send",
            commandId: CommandId.make("stopped-send"),
            callerThreadId: workerId,
            threadId: nestedId,
            text: "Continue",
            createdAt: NOW,
          },
          readModel,
          workerStates: [],
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
    }),
  );
  it.effect(
    "owner deletion stops pending and waiting descendants without deleting their history",
    () =>
      Effect.gen(function* () {
        const nestedId = ThreadId.make("nested");
        const parent = thread(workerId, metadata);
        const nested = thread(nestedId, { ...metadata, ownerThreadId: workerId, depth: 2 });
        const waiting = live("nested");
        const workerStates: WorkerThreadState[] = [
          live("worker"),
          {
            ...waiting,
            thread: { ...waiting.thread, hasPendingApprovals: true, worker: nested.worker },
            pendingMessageId: null,
          },
        ];
        for (const deletedId of [rootId, workerId]) {
          const result = yield* decideOrchestrationCommand({
            command: {
              type: "thread.delete",
              commandId: CommandId.make(`delete-${deletedId}`),
              threadId: deletedId,
            },
            readModel: state([thread(), parent, nested]),
            workerStates,
          });
          const events = Array.isArray(result) ? result : [result];
          expect(events.at(-1)?.type).toBe("thread.deleted");
          expect(events.filter((event) => event.type === "thread.deleted")).toHaveLength(1);
          expect(
            events
              .filter((event) => event.type === "thread.session-stop-requested")
              .map((event) => event.payload.threadId),
          ).toEqual(deletedId === rootId ? [workerId, nestedId] : [nestedId]);
        }
      }),
  );
  it.effect(
    "creates an ordinary thread and initial turn atomically with explicit inherited permissions",
    () =>
      Effect.gen(function* () {
        const result = yield* decideOrchestrationCommand({
          command: spawn(),
          readModel: state(),
          workerStates: [],
        });
        expect(Array.isArray(result)).toBe(true);
        const events = Array.isArray(result) ? result : [result];
        expect(events.map((event) => event.type)).toEqual([
          "thread.created",
          "thread.message-sent",
          "thread.turn-start-requested",
        ]);
        expect(events[0]?.payload).toMatchObject({
          branch: "main",
          worktreePath: "/workspace",
          runtimeMode: "approval-required",
          worker: metadata,
        });
        expect(events[2]?.payload).toMatchObject({ runtimeMode: "approval-required" });
      }),
  );
  it.effect("enforces root and environment live ceilings against pending reservations", () =>
    Effect.gen(function* () {
      for (const candidates of [
        Array.from({ length: 4 }, (_, i) => live(`root-${i}`)),
        Array.from({ length: 16 }, (_, i) =>
          live(`environment-${i}`, ThreadId.make(`other-root-${i}`)),
        ),
      ]) {
        const error = yield* decideOrchestrationCommand({
          command: spawn(),
          readModel: state(),
          workerStates: candidates,
        }).pipe(Effect.flip);
        expect(error).toMatchObject({ _tag: "WorkerOperationError", code: "limit" });
      }
      const error = yield* decideOrchestrationCommand({
        command: turnStart(),
        readModel: state([thread(), thread(workerId, metadata)]),
        workerStates: [live("worker")],
      }).pipe(Effect.flip);
      expect(error).toMatchObject({ code: "busy" });
    }),
  );
  it.effect("bounds depth and cannot grow the caller capability ceiling", () =>
    Effect.gen(function* () {
      const deepId = ThreadId.make("nested");
      const deep = thread(deepId, { ...metadata, ownerThreadId: workerId, depth: 2 });
      const readModel = state([thread(), thread(workerId, metadata), deep]);
      expect(
        yield* decideOrchestrationCommand({
          command: spawn(ThreadId.make("too-deep"), deepId),
          readModel,
          workerStates: [],
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "limit" });
      const command = spawn(ThreadId.make("wide"), workerId);
      if (command.type !== "thread.worker.spawn") throw new Error("unexpected command");
      expect(
        yield* decideOrchestrationCommand({
          command: { ...command, mcpCapabilityCeiling: ["device"] },
          readModel,
          workerStates: [],
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
    }),
  );
  it.effect("rechecks current owner permissions for ordinary UI turn and runtime changes", () =>
    Effect.gen(function* () {
      const worker = {
        ...thread(workerId, { ...metadata, runtimeModeCeiling: "full-access" }),
        runtimeMode: "full-access" as const,
      };
      const readModel = state([thread(), worker]);
      expect(
        yield* decideOrchestrationCommand({
          command: turnStart(),
          readModel,
          workerStates: [],
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
      expect(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.runtime-mode.set",
            commandId: CommandId.make("widen"),
            threadId: workerId,
            runtimeMode: "full-access",
            createdAt: NOW,
          },
          readModel,
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
    }),
  );
  it.effect("rejects orphan, cross-project and sibling control", () =>
    Effect.gen(function* () {
      const worker = thread(workerId, metadata);
      for (const readModel of [
        state([worker]),
        state([{ ...thread(), projectId: ProjectId.make("other-project") }, worker]),
      ]) {
        expect(
          yield* decideOrchestrationCommand({
            command: turnStart(),
            readModel,
            workerStates: [],
          }).pipe(Effect.flip),
        ).toMatchObject({ code: "forbidden" });
      }
      expect(
        yield* decideOrchestrationCommand({
          command: {
            type: "thread.worker.stop",
            commandId: CommandId.make("sibling-stop"),
            callerThreadId: ThreadId.make("sibling"),
            threadId: workerId,
            createdAt: NOW,
          },
          readModel: state([thread(), worker, thread(ThreadId.make("sibling"), metadata)]),
        }).pipe(Effect.flip),
      ).toMatchObject({ code: "forbidden" });
    }),
  );
  it.effect(
    "stop remains available after permission downgrade and a later activation clears only its pending marker",
    () =>
      Effect.gen(function* () {
        let readModel = state([
          thread(),
          { ...thread(workerId, metadata), runtimeMode: "full-access" as const },
        ]);
        const stopped = yield* decideOrchestrationCommand({
          command: {
            type: "thread.worker.stop",
            commandId: CommandId.make("stop"),
            callerThreadId: rootId,
            threadId: workerId,
            createdAt: NOW,
          },
          readModel,
        });
        const events = Array.isArray(stopped) ? stopped : [stopped];
        const stopEvent = events[0];
        if (!stopEvent) throw new Error("missing stop event");
        readModel = yield* projectEvent(readModel, { ...stopEvent, sequence: 1 });
        expect(readModel.threads[1]?.worker).toMatchObject({
          stopRequestedAt: NOW,
          lastStopSequence: 1,
        });
        readModel = {
          ...readModel,
          threads: readModel.threads.map((entry) => ({
            ...entry,
            runtimeMode: "approval-required" as const,
          })),
        };
        const restarted = yield* decideOrchestrationCommand({
          command: turnStart(),
          readModel,
          workerStates: [],
        });
        for (const [index, event] of (Array.isArray(restarted)
          ? restarted
          : [restarted]
        ).entries()) {
          readModel = yield* projectEvent(readModel, { ...event, sequence: index + 2 });
        }
        expect(readModel.threads[1]?.worker).toMatchObject({
          stopRequestedAt: null,
          lastStopSequence: 1,
        });
      }),
  );
});
