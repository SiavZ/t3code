import {
  type ThreadUnattendedAuthority,
  isWorkerRuntimeModeAllowed,
  ThreadId,
  WorkerOperationError,
  WorkerSpawnInput,
  WorkerListInput,
  WorkerGetInput,
  WorkerSendInput,
  WorkerStopInput,
  WorkerWaitInput,
  type WorkerOperationResult,
  type WorkerListResult,
  type WorkerGetResult,
  type WorkerWaitResult,
  type WorkerSummary,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Encoding from "effect/Encoding";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

import { ServerConfig } from "../config.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import {
  OrchestrationCommandIdConflictError,
  OrchestrationCommandPreviouslyRejectedError,
} from "./Errors.ts";
import * as OrchestrationEngine from "./Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";

export interface WorkerSpawnAuthority {
  readonly unattendedAuthority?: ThreadUnattendedAuthority;
  readonly mcpCapabilityCeiling?: ReadonlyArray<
    NonNullable<OrchestrationThreadShell["worker"]>["mcpCapabilityCeiling"][number]
  >;
}

export class OwnedWorkers extends Context.Service<
  OwnedWorkers,
  {
    readonly spawn: (
      input: WorkerSpawnInput,
      authority?: WorkerSpawnAuthority,
    ) => Effect.Effect<WorkerOperationResult, WorkerOperationError>;
    readonly list: (
      input: WorkerListInput,
    ) => Effect.Effect<WorkerListResult, WorkerOperationError>;
    readonly get: (input: WorkerGetInput) => Effect.Effect<WorkerGetResult, WorkerOperationError>;
    readonly send: (
      input: WorkerSendInput,
      authority?: WorkerSpawnAuthority,
    ) => Effect.Effect<WorkerOperationResult, WorkerOperationError>;
    readonly stop: (
      input: WorkerStopInput,
    ) => Effect.Effect<WorkerOperationResult, WorkerOperationError>;
    readonly wait: (
      input: WorkerWaitInput,
    ) => Effect.Effect<WorkerWaitResult, WorkerOperationError>;
  }
>()("t3/orchestration/OwnedWorkers") {}

type Operation = WorkerOperationError["operation"];
type WorkerState = ProjectionSnapshotQuery.WorkerThreadState;

const isWorkerOperationError = Schema.is(WorkerOperationError);
const isCommandConflict = Schema.is(
  Schema.Union([OrchestrationCommandIdConflictError, OrchestrationCommandPreviouslyRejectedError]),
);
const decodeSpawnInput = Schema.decodeUnknownEffect(WorkerSpawnInput);
const decodeListInput = Schema.decodeUnknownEffect(WorkerListInput);
const decodeGetInput = Schema.decodeUnknownEffect(WorkerGetInput);
const decodeSendInput = Schema.decodeUnknownEffect(WorkerSendInput);
const decodeStopInput = Schema.decodeUnknownEffect(WorkerStopInput);
const decodeWaitInput = Schema.decodeUnknownEffect(WorkerWaitInput);

const failure = (operation: Operation, code: WorkerOperationError["code"], detail: string) =>
  new WorkerOperationError({ operation, code, detail });

function summarize(state: WorkerState): WorkerSummary {
  const { thread, pendingMessageId } = state;
  const worker = thread.worker!;
  const turn = thread.latestTurn;
  const failedStartup =
    pendingMessageId === null &&
    thread.session?.status === "error" &&
    thread.session.activeTurnId === null &&
    turn?.state !== "running" &&
    !thread.hasPendingApprovals &&
    !thread.hasPendingUserInput &&
    thread.backgroundLiveness == null;
  const live =
    !failedStartup &&
    (pendingMessageId !== null ||
      turn?.state === "running" ||
      thread.session?.status === "starting" ||
      (thread.session?.status === "running" && thread.session.activeTurnId !== null) ||
      thread.backgroundLiveness != null ||
      (worker.stopRequestedAt !== null &&
        thread.session != null &&
        thread.session.status !== "stopped" &&
        thread.session.status !== "error"));
  const status: WorkerSummary["status"] =
    worker.stopRequestedAt !== null
      ? live
        ? "stopping"
        : "stopped"
      : failedStartup
        ? "failed"
        : thread.hasPendingApprovals || thread.hasPendingUserInput
          ? "waiting"
          : pendingMessageId !== null || thread.session?.status === "starting"
            ? "pending"
            : live
              ? "running"
              : turn?.state === "error" || thread.session?.status === "error"
                ? "failed"
                : turn?.state === "interrupted" || thread.session?.status === "interrupted"
                  ? "interrupted"
                  : turn?.state === "completed"
                    ? "completed"
                    : "idle";
  return {
    threadId: thread.id,
    ownerThreadId: worker.ownerThreadId,
    rootThreadId: worker.rootThreadId,
    label: worker.label,
    depth: worker.depth,
    modelSelection: thread.modelSelection,
    runtimeMode: thread.runtimeMode,
    status,
    pendingMessageId,
    turnId: turn?.turnId ?? null,
    result:
      status === "completed" &&
      turn?.state === "completed" &&
      turn.assistantMessageId !== null &&
      turn.completedAt !== null
        ? {
            assistantMessageId: turn.assistantMessageId,
            turnId: turn.turnId,
            completedAt: turn.completedAt,
          }
        : null,
    createdAt: thread.createdAt,
    updatedAt: thread.updatedAt,
  };
}

const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const threadCapabilities = yield* McpInvocationContext.makeThreadMcpCapabilities;
  const crypto = yield* Crypto.Crypto;
  const config = yield* ServerConfig;
  const path = yield* Path.Path;

  const wrap = <A, E>(operation: Operation, effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        isWorkerOperationError(cause)
          ? cause
          : isCommandConflict(cause)
            ? new WorkerOperationError({
                operation,
                code: "conflict",
                detail: "Worker command ID has already been used or rejected.",
                cause,
              })
            : new WorkerOperationError({
                operation,
                code: "internal",
                detail: "Worker operation failed.",
                cause,
              }),
      ),
    );
  const read = Effect.fnUntraced(function* (operation: Operation, threadId: ThreadId) {
    const state = yield* wrap(operation, snapshots.getWorkerState(threadId));
    if (Option.isNone(state)) return yield* failure(operation, "not-found", "Thread not found.");
    return state.value;
  });
  // Follow persisted ownership, never a caller-provided root or project. Missing ancestors
  // revoke access rather than leaving an orphan accessible under a stale root label.
  const lineage = Effect.fnUntraced(function* (
    operation: Operation,
    threadId: ThreadId,
    loaded?: Map<ThreadId, WorkerState>,
  ) {
    const states: WorkerState[] = [];
    const seen = new Set<ThreadId>();
    let id = threadId;
    while (true) {
      if (seen.has(id) || states.length > 2)
        return yield* failure(operation, "forbidden", "Invalid worker ownership.");
      seen.add(id);
      const state = loaded?.get(id) ?? (yield* read(operation, id));
      loaded?.set(id, state);
      states.push(state);
      if (!state.thread.worker) {
        if (
          states.some(
            (ancestor, index) =>
              ancestor.thread.projectId !== state.thread.projectId ||
              (ancestor.thread.worker &&
                (ancestor.thread.worker.rootThreadId !== state.thread.id ||
                  ancestor.thread.worker.depth !== states.length - index - 1)),
          )
        ) {
          return yield* failure(operation, "forbidden", "Invalid worker ownership.");
        }
        return states;
      }
      id = state.thread.worker.ownerThreadId;
    }
  });
  const target = Effect.fnUntraced(function* (
    operation: Operation,
    callerThreadId: ThreadId,
    workerThreadId: ThreadId,
    loaded?: Map<ThreadId, WorkerState>,
  ) {
    yield* lineage(operation, callerThreadId, loaded);
    const chain = yield* lineage(operation, workerThreadId, loaded);
    if (
      !chain[0]!.thread.worker ||
      !chain.slice(1).some((state) => state.thread.id === callerThreadId)
    ) {
      return yield* failure(operation, "forbidden", "Worker is not owned by this thread.");
    }
    return chain[0]!;
  });
  const hash = (value: unknown) =>
    wrap(
      "spawn",
      crypto
        .digest("SHA-256", new TextEncoder().encode(JSON.stringify(value)))
        .pipe(Effect.map(Encoding.encodeHex)),
    );
  const spawn = Effect.fn("OwnedWorkers.spawn")(function* (
    rawInput: WorkerSpawnInput,
    authority?: WorkerSpawnAuthority,
  ) {
    const input = yield* decodeSpawnInput(rawInput).pipe(
      Effect.mapError(() => failure("spawn", "invalid-input", "Invalid worker spawn input.")),
    );
    const chain = yield* lineage("spawn", input.callerThreadId);
    const caller = chain[0]!.thread;
    const project = yield* wrap("spawn", snapshots.getProjectShellById(caller.projectId));
    if (Option.isNone(project)) return yield* failure("spawn", "not-found", "Project not found.");
    if (
      caller.worktreePath === null &&
      normalizeProjectPathForComparison(project.value.workspaceRoot) ===
        normalizeProjectPathForComparison(path.resolve(config.baseDir, "scratch"))
    ) {
      return yield* failure(
        "spawn",
        "invalid-input",
        "Start the scratch owner thread before spawning a worker.",
      );
    }
    const inherited = yield* wrap(
      "spawn",
      snapshots.getThreadActivationAuthority(input.callerThreadId),
    );
    const unattendedAuthority = authority?.unattendedAuthority ?? Option.getOrUndefined(inherited);
    const runtimeModeCeiling = unattendedAuthority
      ? isWorkerRuntimeModeAllowed(caller.runtimeMode, unattendedAuthority.runtimeModeCeiling)
        ? caller.runtimeMode
        : unattendedAuthority.runtimeModeCeiling
      : undefined;
    let mcpCapabilityCeiling = [...((yield* threadCapabilities(input.callerThreadId)) ?? [])];
    if (unattendedAuthority)
      mcpCapabilityCeiling = mcpCapabilityCeiling.filter((capability) =>
        unattendedAuthority.mcpCapabilityCeiling.includes(capability),
      );
    if (authority?.mcpCapabilityCeiling)
      mcpCapabilityCeiling = mcpCapabilityCeiling.filter((capability) =>
        authority.mcpCapabilityCeiling!.includes(capability),
      );
    if (!mcpCapabilityCeiling.includes("workers"))
      return yield* failure(
        "spawn",
        "forbidden",
        "Worker spawning is outside this thread's capabilities.",
      );
    mcpCapabilityCeiling.sort();
    const threadId = ThreadId.make(
      `worker:${yield* hash([input.callerThreadId, input.commandId])}`,
    );
    const options = Object.entries(input.modelSelection.options ?? {}).sort(([left], [right]) =>
      left.localeCompare(right),
    );
    const spawnFingerprint = yield* hash([
      input.callerThreadId,
      input.label,
      input.prompt,
      input.modelSelection.instanceId,
      input.modelSelection.model,
      options,
      ...(runtimeModeCeiling === undefined ? [] : [runtimeModeCeiling]),
      ...(unattendedAuthority === undefined ? [] : [unattendedAuthority]),
    ]);
    const receipt = yield* wrap(
      "spawn",
      engine.dispatch({
        type: "thread.worker.spawn",
        ...input,
        threadId,
        mcpCapabilityCeiling,
        spawnFingerprint,
        ...(runtimeModeCeiling === undefined ? {} : { runtimeModeCeiling }),
        ...(unattendedAuthority === undefined ? {} : { unattendedAuthority }),
        createdAt: DateTime.formatIso(yield* DateTime.now),
      }),
    );
    return { workerThreadId: threadId, sequence: receipt.sequence };
  });
  const list = Effect.fn("OwnedWorkers.list")(function* (rawInput: WorkerListInput) {
    const input = yield* decodeListInput(rawInput).pipe(
      Effect.mapError(() => failure("list", "invalid-input", "Invalid worker list input.")),
    );
    const chain = yield* lineage("list", input.callerThreadId);
    const rootThreadId = chain.at(-1)!.thread.id;
    const states = yield* wrap(
      "list",
      snapshots.listWorkerStates({
        rootThreadId,
        ...(chain[0]!.thread.worker ? { ownerThreadId: input.callerThreadId } : {}),
      }),
    );
    const loaded = new Map([...chain, ...states].map((state) => [state.thread.id, state]));
    const workers: WorkerSummary[] = [];
    for (const state of states) {
      const owned = yield* target("list", input.callerThreadId, state.thread.id, loaded).pipe(
        Effect.catchTag("WorkerOperationError", (error) =>
          error.code === "forbidden" || error.code === "not-found"
            ? Effect.succeed(null)
            : Effect.fail(error),
        ),
      );
      if (owned) workers.push(summarize(owned));
    }
    return { workers: workers.slice(0, 200), truncated: states.length > 200 };
  });
  const get = Effect.fn("OwnedWorkers.get")(function* (rawInput: WorkerGetInput) {
    const input = yield* decodeGetInput(rawInput).pipe(
      Effect.mapError(() => failure("get", "invalid-input", "Invalid worker get input.")),
    );
    const state = yield* target("get", input.callerThreadId, input.workerThreadId);
    const turnLimit = input.turnLimit ?? 1;
    const detail = yield* wrap(
      "get",
      snapshots.getThreadDetailSnapshot(
        input.workerThreadId,
        { turnLimit },
        { includeHistoryArtifacts: false, boundedConversation: true },
      ),
    );
    if (Option.isNone(detail)) return yield* failure("get", "not-found", "Worker not found.");
    return { worker: summarize(state), detail: detail.value };
  });
  const send = Effect.fn("OwnedWorkers.send")(function* (
    rawInput: WorkerSendInput,
    authority?: WorkerSpawnAuthority,
  ) {
    const input = yield* decodeSendInput(rawInput).pipe(
      Effect.mapError(() => failure("send", "invalid-input", "Invalid worker send input.")),
    );
    const targetState = yield* target("send", input.callerThreadId, input.workerThreadId);
    const callerAuthority = yield* wrap(
      "send",
      snapshots.getThreadActivationAuthority(input.callerThreadId),
    );
    const unattendedAuthority =
      authority?.unattendedAuthority ?? Option.getOrUndefined(callerAuthority);
    const runtimeModeCeiling =
      unattendedAuthority &&
      !isWorkerRuntimeModeAllowed(
        targetState.thread.runtimeMode,
        unattendedAuthority.runtimeModeCeiling,
      )
        ? unattendedAuthority.runtimeModeCeiling
        : targetState.thread.runtimeMode;
    const receipt = yield* wrap(
      "send",
      engine.dispatch({
        type: "thread.worker.send",
        ...(unattendedAuthority ? { unattendedAuthority, runtimeModeCeiling } : {}),
        commandId: input.commandId,
        callerThreadId: input.callerThreadId,
        threadId: input.workerThreadId,
        text: input.text,
        createdAt: DateTime.formatIso(yield* DateTime.now),
      }),
    );
    return { workerThreadId: input.workerThreadId, sequence: receipt.sequence };
  });
  const stop = Effect.fn("OwnedWorkers.stop")(function* (rawInput: WorkerStopInput) {
    const input = yield* decodeStopInput(rawInput).pipe(
      Effect.mapError(() => failure("stop", "invalid-input", "Invalid worker stop input.")),
    );
    yield* target("stop", input.callerThreadId, input.workerThreadId);
    const receipt = yield* wrap(
      "stop",
      engine.dispatch({
        type: "thread.worker.stop",
        commandId: input.commandId,
        callerThreadId: input.callerThreadId,
        threadId: input.workerThreadId,
        createdAt: DateTime.formatIso(yield* DateTime.now),
      }),
    );
    return { workerThreadId: input.workerThreadId, sequence: receipt.sequence };
  });
  const wait = Effect.fn("OwnedWorkers.wait")(function* (rawInput: WorkerWaitInput) {
    const input = yield* decodeWaitInput(rawInput).pipe(
      Effect.mapError(() => failure("wait", "invalid-input", "Invalid worker wait input.")),
    );
    return yield* Effect.scoped(
      Effect.gen(function* () {
        const events = yield* engine.subscribeDomainEvents;
        const observedThreadIds = new Set<string>([input.callerThreadId]);
        for (const threadId of input.workerThreadIds) {
          for (const state of yield* lineage("wait", threadId))
            observedThreadIds.add(state.thread.id);
        }
        const check = Effect.gen(function* () {
          const loaded = new Map<ThreadId, WorkerState>();
          return yield* Effect.forEach(input.workerThreadIds, (id) =>
            target("wait", input.callerThreadId, id, loaded).pipe(Effect.map(summarize)),
          );
        });
        const ready = (workers: readonly WorkerSummary[]) => {
          const settled = (worker: WorkerSummary) =>
            ["idle", "completed", "failed", "interrupted", "stopped"].includes(worker.status);
          return input.mode === "all" ? workers.every(settled) : workers.some(settled);
        };
        const initial = yield* check;
        if (ready(initial)) return { timedOut: false, workers: initial };
        // The acquired subscription buffers events emitted during the initial read.
        // Parent deletion and background task completion matter too, so recheck on
        // thread events rather than only on turn-completed events for a target.
        const outcome = yield* events.pipe(
          Stream.filter(
            (event) =>
              event.aggregateKind === "thread" &&
              observedThreadIds.has(event.aggregateId) &&
              !(
                event.type === "thread.message-sent" &&
                event.payload.streaming &&
                (event.payload.role === "assistant" || event.payload.role === "reasoning")
              ),
          ),
          Stream.mapEffect(() => check),
          Stream.filter(ready),
          Stream.runHead,
          Effect.timeoutOption(input.timeoutMs),
        );
        if (Option.isSome(outcome) && Option.isSome(outcome.value))
          return { timedOut: false, workers: outcome.value.value };
        return { timedOut: true, workers: yield* check };
      }),
    );
  });
  return OwnedWorkers.of({ spawn, list, get, send, stop, wait });
});

export const layer = Layer.effect(OwnedWorkers, make);
