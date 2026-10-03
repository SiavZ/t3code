import {
  EventId,
  MAX_SCRIPT_ID_LENGTH,
  SCRIPT_RUN_COMMAND_PATTERN,
  MessageId,
  ThreadLinkedPullRequest,
  UserInputRequestedPayload,
  isImportedAgentSessionMessageId,
  isWorkerRuntimeModeAllowed,
  WORKER_MAX_DEPTH,
  WORKER_MAX_LIVE_PER_ROOT,
  WORKER_MAX_LIVE_PER_ENVIRONMENT,
  WorkerOperationError,
  type RuntimeMode,
  type ThreadId,
  type OrchestrationCommand,
  type OrchestrationEvent,
  type OrchestrationReadModel,
  type OrchestrationThread,
  type ThreadPullRequestKey,
  type ThreadPullRequestLink,
  type OrchestrationThreadActivity,
} from "@t3tools/contracts";
import {
  legacyLinkedPullRequestOf,
  legacyThreadPullRequestKey,
  normalizeThreadPullRequestKey,
  threadPullRequestKeysEqual,
} from "@t3tools/shared/threadPullRequests";
import { compareDateTimeStrings } from "@t3tools/shared/dateTime";
import * as DateTime from "effect/DateTime";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as Predicate from "effect/Predicate";
import type * as PlatformError from "effect/PlatformError";

import {
  OrchestrationCommandInvariantError,
  OrchestrationThreadSettleBlockedError,
  type OrchestrationCommandRejection,
} from "./Errors.ts";
import {
  listThreadsByProjectId,
  requireActiveProjectWorkspaceRootAbsent,
  requireProject,
  requireProjectAbsent,
  requireThread,
  requireThreadArchived,
  requireThreadAbsent,
  requireThreadNotArchived,
} from "./commandInvariants.ts";
import { projectEvent } from "./projector.ts";
import { threadHasQueuedTurnStart } from "./ThreadSettlementPolicy.ts";
import { applyMailboxWrite } from "./coordinationMailbox.ts";
import {
  applyPlanWrite,
  assignmentIdentity,
  bindAttempt,
  dispatchPrompt,
  latestAttempt,
  readyNodes,
  settleAttempt,
} from "./coordinationGraph.ts";
import {
  CoordinationError,
  CommandId,
  ThreadId as ThreadIdSchema,
  type CoordinationPlan,
} from "@t3tools/contracts";
import type { WorkerThreadState } from "./Services/ProjectionSnapshotQuery.ts";
import { runtimeHandoffDecision } from "./runtimeHandoffDecision.ts";

const monogramSegmenter = new Intl.Segmenter(undefined, { granularity: "grapheme" });

const isScriptRunCommand = Schema.is(SCRIPT_RUN_COMMAND_PATTERN);

const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
const decodeUserInputRequestedPayload = Schema.decodeUnknownOption(UserInputRequestedPayload);
const threadPullRequestLinksEqual = Schema.toEquivalence(Schema.NullOr(ThreadLinkedPullRequest));

/**
 * Blocked-on-you work derived from the thread's retained activities: an
 * approval or user-input request with no later resolution for the same
 * requestId. The server-side twin of the shell's hasPendingApprovals /
 * hasPendingUserInput flags, which the decider read model does not carry.
 * The clearing rules MUST match ProjectionPipeline's pending accounting —
 * resolved activities always clear, respond.failed clears only when the
 * failure detail marks the request stale/unknown — or settle would be
 * rejected on threads whose shell flags read as clear.
 */
function isStaleRequestFailureDetail(payload: Record<string, unknown> | null): boolean {
  const detail = typeof payload?.detail === "string" ? payload.detail.toLowerCase() : null;
  if (detail === null) return false;
  return (
    detail.includes("stale pending approval request") ||
    detail.includes("unknown pending approval request") ||
    detail.includes("unknown pending permission request") ||
    detail.includes("stale pending user-input request") ||
    detail.includes("unknown pending user-input request") ||
    detail.includes("unknown pending user input request") ||
    detail.includes("unknown pending codex user input request")
  );
}

// Scans the read model's activities, which the projector caps at the most
// recent 500 plus pending async questions. Async questions remain actionable
// while the agent works, so they must not expire with the activity window.
function openRequests(thread: Pick<OrchestrationThread, "activities">) {
  const requests = new Map<string, OrchestrationThreadActivity>();
  for (const activity of thread.activities) {
    const payload =
      typeof activity.payload === "object" && activity.payload !== null
        ? (activity.payload as Record<string, unknown>)
        : null;
    const requestId = typeof payload?.requestId === "string" ? payload.requestId : null;
    if (requestId === null) continue;
    if (activity.kind === "approval.requested" || activity.kind === "user-input.requested") {
      requests.set(requestId, activity);
    } else if (activity.kind === "approval.resolved" || activity.kind === "user-input.resolved") {
      requests.delete(requestId);
    } else if (
      (activity.kind === "provider.approval.respond.failed" ||
        activity.kind === "provider.user-input.respond.failed") &&
      isStaleRequestFailureDetail(payload)
    ) {
      requests.delete(requestId);
    }
  }
  return requests;
}

/** Apply the shared shell-level rule to the detailed command read model. */
function hasQueuedTurnStartForThread(
  thread: Pick<OrchestrationThread, "messages" | "latestTurn" | "session">,
  now: string,
): boolean {
  let latestUserMessageAt: string | null = null;
  let latestUserMessageAtMs = Number.NEGATIVE_INFINITY;
  for (const message of thread.messages) {
    if (message.role !== "user" || isImportedAgentSessionMessageId(message.id)) continue;
    const messageAtMs = Date.parse(message.createdAt);
    latestUserMessageAtMs = Math.max(latestUserMessageAtMs, messageAtMs);
    if (messageAtMs === latestUserMessageAtMs) {
      latestUserMessageAt = message.createdAt;
    }
  }
  return threadHasQueuedTurnStart(
    {
      latestUserMessageAt: Number.isFinite(latestUserMessageAtMs) ? latestUserMessageAt : null,
      latestTurn: thread.latestTurn,
      session: thread.session,
    },
    now,
  );
}

function findPullRequestLink(
  thread: Pick<OrchestrationThread, "pullRequests">,
  key: ThreadPullRequestKey,
): ThreadPullRequestLink | undefined {
  return thread.pullRequests.find((link) => threadPullRequestKeysEqual(link, key));
}

function withEventBase(
  input: Pick<OrchestrationCommand, "commandId"> & {
    readonly aggregateKind: OrchestrationEvent["aggregateKind"];
    readonly aggregateId: OrchestrationEvent["aggregateId"];
    readonly occurredAt: string;
    readonly metadata?: OrchestrationEvent["metadata"];
  },
): Effect.Effect<
  Omit<OrchestrationEvent, "sequence" | "type" | "payload">,
  PlatformError.PlatformError,
  Crypto.Crypto
> {
  return Crypto.Crypto.pipe(
    Effect.flatMap((crypto) =>
      crypto.randomUUIDv4.pipe(
        Effect.map((eventId) => ({
          eventId: EventId.make(eventId),
          aggregateKind: input.aggregateKind,
          aggregateId: input.aggregateId,
          occurredAt: input.occurredAt,
          commandId: input.commandId,
          causationEventId: null,
          correlationId: input.commandId,
          metadata: input.metadata ?? {},
        })),
      ),
    ),
  );
}

type PlannedOrchestrationEvent = Omit<OrchestrationEvent, "sequence">;

type DecideOrchestrationCommandResult =
  | PlannedOrchestrationEvent
  | ReadonlyArray<PlannedOrchestrationEvent>;

function workerIsLive({ thread, pendingMessageId }: WorkerThreadState): boolean {
  return (
    pendingMessageId !== null ||
    thread.session?.status === "starting" ||
    (thread.session?.status === "running" && thread.session.activeTurnId !== null) ||
    thread.latestTurn?.state === "running" ||
    thread.hasPendingApprovals ||
    thread.hasPendingUserInput ||
    thread.backgroundLiveness != null ||
    (thread.worker?.stopRequestedAt != null &&
      thread.session != null &&
      thread.session.status !== "stopped" &&
      thread.session.status !== "error")
  );
}

const requireWorkerChain = Effect.fnUntraced(function* (
  readModel: OrchestrationReadModel,
  thread: OrchestrationThread,
  operation: "spawn" | "send" | "stop",
  requestedMode: RuntimeMode = thread.runtimeMode,
) {
  let cursor = thread;
  for (let depth = 0; cursor.worker != null; depth += 1) {
    const metadata = cursor.worker;
    const owner = readModel.threads.find(
      (entry) => entry.id === metadata.ownerThreadId && entry.deletedAt === null,
    );
    if (
      depth >= WORKER_MAX_DEPTH ||
      !owner ||
      owner.projectId !== thread.projectId ||
      metadata.rootThreadId !== (owner.worker?.rootThreadId ?? owner.id) ||
      metadata.depth !== (owner.worker?.depth ?? 0) + 1 ||
      (operation !== "stop" &&
        (!isWorkerRuntimeModeAllowed(requestedMode, metadata.runtimeModeCeiling) ||
          !isWorkerRuntimeModeAllowed(requestedMode, owner.runtimeMode)))
    ) {
      return yield* new WorkerOperationError({
        operation,
        code: "forbidden",
        detail: "Worker ownership or inherited permission ceiling is no longer valid.",
      });
    }
    cursor = owner;
  }
});

const requireOwnedWorker = Effect.fnUntraced(function* (
  readModel: OrchestrationReadModel,
  callerThreadId: ThreadId,
  thread: OrchestrationThread,
  operation: "send" | "stop",
) {
  yield* requireWorkerChain(readModel, thread, operation);
  let cursor = thread;
  for (let depth = 0; depth < WORKER_MAX_DEPTH && cursor.worker != null; depth += 1) {
    if (cursor.worker.ownerThreadId === callerThreadId) return;
    const owner = readModel.threads.find((entry) => entry.id === cursor.worker?.ownerThreadId);
    if (!owner) break;
    cursor = owner;
  }
  return yield* new WorkerOperationError({
    operation,
    code: "forbidden",
    detail: "The caller does not own this worker.",
  });
});

const requireWorkerAdmission = Effect.fnUntraced(function* (
  rootThreadId: ThreadId,
  workerStates: ReadonlyArray<WorkerThreadState>,
  operation: "spawn" | "send",
  targetThreadId?: ThreadId,
) {
  const live = workerStates.filter(workerIsLive);
  if (targetThreadId !== undefined && live.some(({ thread }) => thread.id === targetThreadId)) {
    return yield* new WorkerOperationError({
      operation,
      code: "busy",
      detail: "The worker still has active or waiting work.",
    });
  }
  if (
    live.length >= WORKER_MAX_LIVE_PER_ENVIRONMENT ||
    live.filter(({ thread }) => thread.worker?.rootThreadId === rootThreadId).length >=
      WORKER_MAX_LIVE_PER_ROOT
  ) {
    return yield* new WorkerOperationError({
      operation,
      code: "limit",
      detail: "The owned-worker concurrency limit has been reached.",
    });
  }
});

const decideCommandSequence = Effect.fn("decideCommandSequence")(function* ({
  commands,
  readModel,
  workerStates,
}: {
  readonly commands: ReadonlyArray<OrchestrationCommand>;
  readonly readModel: OrchestrationReadModel;
  readonly workerStates?: ReadonlyArray<WorkerThreadState>;
}): Effect.fn.Return<
  ReadonlyArray<PlannedOrchestrationEvent>,
  OrchestrationCommandRejection | PlatformError.PlatformError,
  Crypto.Crypto
> {
  let nextReadModel = readModel;
  let nextSequence = readModel.snapshotSequence;
  const plannedEvents: PlannedOrchestrationEvent[] = [];

  for (const nextCommand of commands) {
    const decided = yield* decideOrchestrationCommand({
      command: nextCommand,
      readModel: nextReadModel,
      ...(workerStates !== undefined ? { workerStates } : {}),
    });
    const nextEvents = Array.isArray(decided) ? decided : [decided];
    for (const nextEvent of nextEvents) {
      plannedEvents.push(nextEvent);
      nextSequence += 1;
      nextReadModel = yield* projectEvent(nextReadModel, {
        ...nextEvent,
        sequence: nextSequence,
      }).pipe(Effect.orDie);
    }
  }

  return plannedEvents;
});

export const decideOrchestrationCommand = Effect.fn("decideOrchestrationCommand")(function* ({
  command,
  readModel,
  userInputActivity,
  workerStates,
  coordinationPlan,
  coordinationTurnBound,
  coordinationMailbox,
  coordinationReusableWorker,
  coordinationCancellationTargets,
  coordinationExecutionAuthority,
  coordinationExecutionDenied,
}: {
  readonly command: OrchestrationCommand;
  readonly readModel: OrchestrationReadModel;
  readonly userInputActivity?: OrchestrationThreadActivity;
  readonly workerStates?: ReadonlyArray<WorkerThreadState>;
  readonly coordinationPlan?: CoordinationPlan | null;
  readonly coordinationTurnBound?: boolean;
  readonly coordinationMailbox?: import("@t3tools/contracts").CoordinationMailbox | null;
  readonly coordinationReusableWorker?: WorkerThreadState;
  readonly coordinationCancellationTargets?: ReadonlyArray<ThreadId>;
  readonly coordinationExecutionAuthority?: import("@t3tools/contracts").ThreadUnattendedAuthority;
  readonly coordinationExecutionDenied?: boolean;
}): Effect.fn.Return<
  DecideOrchestrationCommandResult,
  OrchestrationCommandRejection | PlatformError.PlatformError,
  Crypto.Crypto
> {
  switch (command.type) {
    case "coordination.mailbox.write": {
      const root = yield* requireThread({ readModel, command, threadId: command.threadId });
      if (
        root.worker ||
        root.deletedAt !== null ||
        command.input.rootThreadId !== root.id ||
        command.input.commandId !== command.commandId
      )
        return yield* new CoordinationError({
          code: "forbidden",
          detail: "Mailbox command authority mismatch.",
        });
      const members = new Set(
        readModel.threads
          .filter(
            (thread) =>
              thread.deletedAt === null &&
              (thread.id === root.id ||
                (thread.worker?.rootThreadId === root.id &&
                  thread.worker.stopRequestedAt === null)),
          )
          .map((thread) => thread.id),
      );
      const mailbox = yield* Effect.try({
        try: () => applyMailboxWrite(coordinationMailbox ?? null, command.input, members),
        catch: (cause) =>
          cause instanceof CoordinationError
            ? cause
            : new CoordinationError({ code: "invalid", detail: "Invalid mailbox transition." }),
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: root.id,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "coordination.mailbox.updated" as const,
        payload: { mailbox },
      };
    }
    case "coordination.plan.write":
    case "coordination.plan.dispatch":
    case "coordination.plan.recover":
    case "coordination.plan.abandon":
    case "coordination.plan.settle": {
      const root = yield* requireThread({ readModel, command, threadId: command.threadId });
      if (root.worker || root.deletedAt !== null)
        return yield* new CoordinationError({
          code: "forbidden",
          detail: "Plan authority must be a live root thread.",
        });
      let plan = coordinationPlan ?? null;
      let workerEvents: ReadonlyArray<PlannedOrchestrationEvent> = [];
      if (command.type === "coordination.plan.write") {
        if (
          command.input.rootThreadId !== command.threadId ||
          command.input.commandId !== command.commandId
        )
          return yield* new CoordinationError({
            code: "forbidden",
            detail: "Plan command identity mismatch.",
          });
        if (command.input.operation === "complete") {
          const input = command.input;
          const node = plan?.nodes.find((node) => node.id === input.nodeId);
          const attempt = node && latestAttempt(node);
          const worker = readModel.threads.find((thread) => thread.id === input.workerThreadId);
          if (
            !attempt ||
            !worker ||
            worker.worker?.rootThreadId !== root.id ||
            !coordinationTurnBound
          )
            return yield* new CoordinationError({
              code: "forbidden",
              detail: "Artifact turn does not match its durable dispatch message.",
            });
          plan = {
            ...plan!,
            nodes: plan!.nodes.map((entry) =>
              entry.id === node!.id
                ? {
                    ...entry,
                    attempts: [
                      ...entry.attempts.slice(0, -1),
                      { ...attempt, turnId: input.turnId },
                    ],
                  }
                : entry,
            ),
          };
        }
        plan = yield* Effect.try({
          try: () => applyPlanWrite(plan, command.input),
          catch: (cause) =>
            cause instanceof CoordinationError
              ? cause
              : new CoordinationError({ code: "invalid", detail: "Invalid plan transition." }),
        });
        if (["create", "run", "retry", "repair", "salvage"].includes(command.input.operation))
          plan = {
            ...plan,
            executionAuthority: plan.executionAuthority ?? coordinationExecutionAuthority ?? null,
          };
        if (command.input.operation === "cancel") {
          const commands: OrchestrationCommand[] = plan.nodes.flatMap((node) => {
            const attempt = latestAttempt(node);
            return attempt?.status === "accepted" &&
              coordinationCancellationTargets?.includes(attempt.workerThreadId)
              ? [
                  {
                    type: "thread.session.stop" as const,
                    commandId: command.commandId,
                    threadId: attempt.workerThreadId,
                    expectedMessageId: attempt.dispatchMessageId,
                    createdAt: command.createdAt,
                  },
                ]
              : [];
          });
          workerEvents = yield* decideCommandSequence({
            readModel,
            commands,
            ...(workerStates !== undefined ? { workerStates } : {}),
          });
        }
      } else {
        if (!plan || plan.rootThreadId !== root.id || plan.revision !== command.expectedRevision)
          return yield* new CoordinationError({
            code: "conflict",
            detail: "Plan revision mismatch.",
          });
        if (command.type === "coordination.plan.abandon") {
          const node = plan.nodes.find((entry) => entry.id === command.nodeId);
          const attempt = node && latestAttempt(node);
          const state =
            attempt && workerStates?.find((entry) => entry.thread.id === attempt.workerThreadId);
          const thread =
            attempt && readModel.threads.find((entry) => entry.id === attempt.workerThreadId);
          if (
            !attempt ||
            attempt.status !== "accepted" ||
            attempt.dispatchMessageId !== command.expectedMessageId
          )
            return yield* new CoordinationError({
              code: "conflict",
              detail: "Orphan settlement no longer binds the assignment.",
            });
          if (
            state?.pendingMessageId != null ||
            thread?.latestTurn?.state === "running" ||
            thread?.session?.activeTurnId != null ||
            thread?.session?.status === "starting" ||
            state?.thread.backgroundLiveness != null ||
            state?.thread.hasPendingApprovals ||
            state?.thread.hasPendingUserInput
          )
            return yield* new CoordinationError({
              code: "busy",
              detail: "Assignment is not quiescent.",
            });
          plan = {
            ...plan,
            revision: plan.revision + 1,
            nodes: plan.nodes.map((entry) =>
              entry.id === node!.id
                ? {
                    ...entry,
                    attempts: [
                      ...entry.attempts.slice(0, -1),
                      {
                        ...attempt,
                        status: "interrupted" as const,
                        failureCode: "activationInterrupted",
                        artifact: attempt.pendingArtifact ?? attempt.artifact,
                        pendingArtifact: null,
                      },
                    ],
                  }
                : entry,
            ),
          };
        } else if (command.type === "coordination.plan.recover") {
          // Restart never silently resumes a graph. Preserve live assignments, and fail
          // orphaned activations only after startup has reconciled provider ownership.
          plan = {
            ...plan,
            paused: true,
            revision: plan.revision + 1,
            nodes: plan.nodes.map((node) => {
              const attempt = latestAttempt(node);
              if (attempt?.status !== "accepted") return node;
              const thread = readModel.threads.find((entry) => entry.id === attempt.workerThreadId);
              const state = workerStates?.find(
                (entry) => entry.thread.id === attempt.workerThreadId,
              );
              const live =
                state?.pendingMessageId != null ||
                thread?.latestTurn?.state === "running" ||
                thread?.session?.status === "starting" ||
                thread?.session?.activeTurnId != null ||
                state?.thread.backgroundLiveness != null ||
                state?.thread.hasPendingApprovals ||
                state?.thread.hasPendingUserInput;
              if (live) return node;
              return {
                ...node,
                attempts: [
                  ...node.attempts.slice(0, -1),
                  {
                    ...attempt,
                    status: "interrupted" as const,
                    failureCode: "serverRestart",
                    artifact: attempt.pendingArtifact ?? attempt.artifact,
                    pendingArtifact: null,
                  },
                ],
              };
            }),
          };
        } else if (command.type === "coordination.plan.dispatch") {
          const node = readyNodes(plan).find((node) => node.id === command.nodeId);
          if (!node)
            return yield* new CoordinationError({ code: "busy", detail: "Node is not ready." });
          const identity = assignmentIdentity(root.id, plan.id, node.id, node.attempts.length + 1);
          const dispatchId = CommandId.make(`coordination:${identity}`);
          const reusable = coordinationReusableWorker;
          const workerId =
            reusable?.thread.id ?? ThreadIdSchema.make(`coordination-worker:${identity}`);
          const prompt = yield* Effect.try({
            try: () => dispatchPrompt(plan!, node),
            catch: (cause) =>
              cause instanceof CoordinationError
                ? cause
                : new CoordinationError({ code: "invalid", detail: "Invalid dependency handoff." }),
          });
          const decided = yield* decideOrchestrationCommand({
            readModel,
            ...(workerStates !== undefined ? { workerStates } : {}),
            command: reusable
              ? {
                  type: "thread.worker.send",
                  ...(coordinationExecutionAuthority !== undefined
                    ? {
                        unattendedAuthority: coordinationExecutionAuthority,
                        runtimeModeCeiling: coordinationExecutionAuthority.runtimeModeCeiling,
                      }
                    : {}),
                  commandId: dispatchId,
                  callerThreadId: root.id,
                  threadId: workerId,
                  text: prompt,
                  createdAt: command.createdAt,
                }
              : {
                  type: "thread.worker.spawn",
                  ...(coordinationExecutionAuthority !== undefined
                    ? {
                        unattendedAuthority: coordinationExecutionAuthority,
                        runtimeModeCeiling: coordinationExecutionAuthority.runtimeModeCeiling,
                      }
                    : {}),
                  commandId: dispatchId,
                  callerThreadId: root.id,
                  threadId: workerId,
                  label: node.id.slice(0, 80),
                  prompt,
                  modelSelection: node.modelSelection,
                  mcpCapabilityCeiling: ["workers"],
                  spawnFingerprint: JSON.stringify([
                    plan.id,
                    node.id,
                    node.attempts.length + 1,
                    node.modelSelection,
                    coordinationExecutionAuthority ?? null,
                  ]),
                  createdAt: command.createdAt,
                },
          }).pipe(
            Effect.mapError((cause) =>
              Schema.is(WorkerOperationError)(cause) &&
              (cause.code === "busy" || cause.code === "limit")
                ? new CoordinationError({
                    code: "busy",
                    detail: "Worker capacity is temporarily unavailable.",
                  })
                : cause,
            ),
          );
          workerEvents = Array.isArray(decided) ? decided : [decided];
          plan = yield* Effect.try({
            try: () =>
              bindAttempt(plan!, node.id, {
                workerThreadId: workerId,
                dispatchMessageId: MessageId.make(`worker-message:${dispatchId}`),
                turnId: null,
              }),
            catch: () =>
              new CoordinationError({ code: "invalid", detail: "Cannot bind assignment." }),
          });
        } else {
          const node = plan.nodes.find((node) => node.id === command.nodeId);
          const attempt = node && latestAttempt(node);
          const worker =
            attempt && readModel.threads.find((thread) => thread.id === attempt.workerThreadId);
          const state = workerStates?.find((state) => state.thread.id === worker?.id);
          if (
            !attempt ||
            !worker ||
            worker.latestTurn?.turnId !== command.turnId ||
            !coordinationTurnBound
          )
            return yield* new CoordinationError({
              code: "conflict",
              detail: "Settlement receipt does not bind assignment.",
            });
          const live =
            worker.latestTurn.state === "running" ||
            state?.pendingMessageId != null ||
            state?.thread.session?.activeTurnId != null ||
            state?.thread.session?.status === "starting" ||
            state?.thread.backgroundLiveness != null ||
            state?.thread.hasPendingApprovals ||
            state?.thread.hasPendingUserInput;
          if (live)
            return yield* new CoordinationError({
              code: "busy",
              detail: "Assignment still has native work or pending requests.",
            });
          if (coordinationExecutionDenied) {
            const bound = {
              ...plan,
              nodes: plan.nodes.map((entry) =>
                entry.id === node!.id
                  ? {
                      ...entry,
                      attempts: [
                        ...entry.attempts.slice(0, -1),
                        { ...attempt, turnId: command.turnId },
                      ],
                    }
                  : entry,
              ),
            };
            plan = yield* Effect.try({
              try: () => settleAttempt(bound, command.nodeId, command.turnId, "interrupted", true),
              catch: () =>
                new CoordinationError({
                  code: "conflict",
                  detail: "Stale revoked assignment settlement.",
                }),
            });
            plan = {
              ...plan,
              paused: true,
              nodes: plan.nodes.map((entry) =>
                entry.id === node!.id
                  ? {
                      ...entry,
                      attempts: [
                        ...entry.attempts.slice(0, -1),
                        { ...entry.attempts.at(-1)!, failureCode: "executionAuthorityUnavailable" },
                      ],
                    }
                  : entry,
              ),
            };
          } else if (
            plan.policy.mode === "deep" &&
            command.outcome === "completed" &&
            !attempt.pendingArtifact &&
            !attempt.handoffRequested &&
            !plan.cancelled
          ) {
            const handoffId = CommandId.make(
              `coordination-handoff:${assignmentIdentity(root.id, plan.id, node!.id, attempt.number)}`,
            );
            const events = yield* decideOrchestrationCommand({
              readModel,
              ...(workerStates !== undefined ? { workerStates } : {}),
              command: {
                type: "thread.worker.send",
                ...(coordinationExecutionAuthority !== undefined
                  ? {
                      unattendedAuthority: coordinationExecutionAuthority,
                      runtimeModeCeiling: coordinationExecutionAuthority.runtimeModeCeiling,
                    }
                  : {}),
                commandId: handoffId,
                callerThreadId: root.id,
                threadId: attempt.workerThreadId,
                text: `The native turn completed without the required typed artifact. Do not repeat task execution. Read coordination plan ${plan.id} and report the current assignment artifact for node ${node!.id}, attempt ${attempt.number}. Evidence remains agent-reported. This is the only report continuation.`,
                createdAt: command.createdAt,
              },
            });
            workerEvents = Array.isArray(events) ? events : [events];
            plan = {
              ...plan,
              revision: plan.revision + 1,
              nodes: plan.nodes.map((entry) =>
                entry.id === node!.id
                  ? {
                      ...entry,
                      attempts: [
                        ...entry.attempts.slice(0, -1),
                        {
                          ...attempt,
                          turnId: null,
                          handoffRequested: true,
                          initialDispatchMessageId: attempt.dispatchMessageId,
                          dispatchMessageId: MessageId.make(`worker-message:${handoffId}`),
                        },
                      ],
                    }
                  : entry,
              ),
            };
          } else {
            const bound = {
              ...plan,
              nodes: plan.nodes.map((entry) =>
                entry.id === node!.id
                  ? {
                      ...entry,
                      attempts: [
                        ...entry.attempts.slice(0, -1),
                        { ...attempt, turnId: command.turnId },
                      ],
                    }
                  : entry,
              ),
            };
            plan = yield* Effect.try({
              try: () =>
                settleAttempt(bound, command.nodeId, command.turnId, command.outcome, true),
              catch: () => new CoordinationError({ code: "conflict", detail: "Stale settlement." }),
            });
            if (!plan.policy.retainWorkers) {
              const stopped = yield* decideOrchestrationCommand({
                readModel,
                command: {
                  type: "thread.session.stop",
                  commandId: command.commandId,
                  threadId: attempt.workerThreadId,
                  expectedMessageId: attempt.dispatchMessageId,
                  expectedTurnId: command.turnId,
                  createdAt: command.createdAt,
                },
              });
              workerEvents = Array.isArray(stopped) ? stopped : [stopped];
            }
          }
        }
      }
      if (new TextEncoder().encode(JSON.stringify(plan)).length > 262_144)
        return yield* new CoordinationError({
          code: "invalid",
          detail: "Persisted plan exceeds the 256K document limit.",
        });
      return [
        ...workerEvents,
        {
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: root.id,
            occurredAt: command.createdAt,
            commandId: command.commandId,
          })),
          type: "coordination.plan.updated" as const,
          payload: { plan },
        },
      ];
    }
    case "thread.worker.spawn": {
      const caller = yield* requireThread({ readModel, command, threadId: command.callerThreadId });
      if (caller.worker?.stopRequestedAt != null) {
        return yield* new WorkerOperationError({
          operation: "spawn",
          code: "forbidden",
          detail: "A stopped worker cannot initiate new work until its owner resumes it.",
        });
      }
      yield* requireWorkerChain(readModel, caller, "spawn");
      const depth = (caller.worker?.depth ?? 0) + 1;
      if (depth > WORKER_MAX_DEPTH) {
        return yield* new WorkerOperationError({
          operation: "spawn",
          code: "limit",
          detail: "The owned-worker nesting limit has been reached.",
        });
      }
      const rootThreadId = caller.worker?.rootThreadId ?? caller.id;
      const runtimeMode = command.runtimeModeCeiling ?? caller.runtimeMode;
      if (!isWorkerRuntimeModeAllowed(runtimeMode, caller.runtimeMode))
        return yield* new WorkerOperationError({
          operation: "spawn",
          code: "forbidden",
          detail: "Requested worker runtime ceiling exceeds its owner.",
        });
      yield* requireWorkerAdmission(rootThreadId, workerStates ?? [], "spawn");
      if (
        caller.worker &&
        command.mcpCapabilityCeiling.some(
          (capability) => !caller.worker?.mcpCapabilityCeiling.includes(capability),
        )
      ) {
        return yield* new WorkerOperationError({
          operation: "spawn",
          code: "forbidden",
          detail: "Worker capabilities cannot exceed its owner's ceiling.",
        });
      }
      const events = yield* decideCommandSequence({
        readModel,
        commands: [
          {
            type: "thread.create",
            commandId: command.commandId,
            threadId: command.threadId,
            projectId: caller.projectId,
            title: command.label,
            modelSelection: command.modelSelection,
            runtimeMode,
            interactionMode: caller.interactionMode,
            branch: caller.branch,
            worktreePath: caller.worktreePath,
            createdAt: command.createdAt,
          },
          {
            type: "thread.turn.start",
            ...(command.unattendedAuthority !== undefined
              ? { unattendedAuthority: command.unattendedAuthority }
              : {}),
            commandId: command.commandId,
            threadId: command.threadId,
            message: {
              messageId: MessageId.make(`worker-message:${command.commandId}`),
              role: "user",
              text: command.prompt,
              attachments: [],
            },
            modelSelection: command.modelSelection,
            runtimeMode,
            interactionMode: caller.interactionMode,
            createdAt: command.createdAt,
          },
        ],
      });
      return events.map((event) =>
        event.type === "thread.created"
          ? {
              ...event,
              payload: {
                ...event.payload,
                worker: {
                  ownerThreadId: caller.id,
                  rootThreadId,
                  depth,
                  spawnCommandId: command.commandId,
                  spawnFingerprint: command.spawnFingerprint,
                  label: command.label,
                  runtimeModeCeiling: runtimeMode,
                  mcpCapabilityCeiling: command.mcpCapabilityCeiling,
                  stopRequestedAt: null,
                  lastStopSequence: null,
                },
              },
            }
          : event,
      );
    }
    case "thread.worker.send": {
      const caller = yield* requireThread({ readModel, command, threadId: command.callerThreadId });
      if (caller.worker?.stopRequestedAt != null) {
        return yield* new WorkerOperationError({
          operation: "send",
          code: "forbidden",
          detail: "A stopped worker cannot initiate new work until its owner resumes it.",
        });
      }
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      yield* requireOwnedWorker(readModel, command.callerThreadId, thread, "send");
      if (
        command.runtimeModeCeiling !== undefined &&
        !isWorkerRuntimeModeAllowed(command.runtimeModeCeiling, thread.runtimeMode)
      )
        return yield* new WorkerOperationError({
          operation: "send",
          code: "forbidden",
          detail: "Worker continuation cannot widen its existing runtime ceiling.",
        });
      return yield* decideOrchestrationCommand({
        readModel,
        ...(workerStates !== undefined ? { workerStates } : {}),
        command: {
          type: "thread.turn.start",
          commandId: command.commandId,
          threadId: command.threadId,
          message: {
            messageId: MessageId.make(`worker-message:${command.commandId}`),
            role: "user",
            text: command.text,
            attachments: [],
          },
          modelSelection: thread.modelSelection,
          runtimeMode: command.runtimeModeCeiling ?? thread.runtimeMode,
          ...(command.unattendedAuthority !== undefined
            ? { unattendedAuthority: command.unattendedAuthority }
            : {}),
          interactionMode: thread.interactionMode,
          createdAt: command.createdAt,
        },
      });
    }
    case "thread.worker.stop": {
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      yield* requireOwnedWorker(readModel, command.callerThreadId, thread, "stop");
      return yield* decideOrchestrationCommand({
        readModel,
        command: {
          type: "thread.session.stop",
          commandId: command.commandId,
          threadId: command.threadId,
          createdAt: command.createdAt,
        },
      });
    }
    case "project.create": {
      yield* requireProjectAbsent({
        readModel,
        command,
        projectId: command.projectId,
      });
      yield* requireActiveProjectWorkspaceRootAbsent({
        readModel,
        command,
        workspaceRoot: command.workspaceRoot,
        exceptProjectId: command.projectId,
      });

      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "project.created",
        payload: {
          projectId: command.projectId,
          title: command.title,
          workspaceRoot: command.workspaceRoot,
          // Project creation has no user model choice. Older clients sent an
          // automatic seed here, but only a metadata update records an
          // explicit project default.
          defaultModelSelection: null,
          faviconPath: null,
          projectIcon: null,
          scripts: [],
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "project.meta.update": {
      const project = yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      if (
        command.projectIcon?.kind === "monogram" &&
        Array.from(monogramSegmenter.segment(command.projectIcon.text)).length > 2
      ) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Project monograms must contain at most two characters.",
        });
      }
      if (command.scripts !== undefined) {
        // Persisted IDs predate shortcut validation. Let users edit or remove them
        // without allowing another invalid ID to enter the project.
        const existingIds = new Set(project.scripts.map((script) => script.id));
        for (const script of command.scripts) {
          if (!existingIds.has(script.id) && !isScriptRunCommand(`script.${script.id}.run`)) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: command.type,
              detail: `Script ID '${script.id}' must be 1-${MAX_SCRIPT_ID_LENGTH} lowercase letters, digits or hyphens, starting with a letter or digit.`,
            });
          }
        }
      }
      if (command.workspaceRoot !== undefined) {
        yield* requireActiveProjectWorkspaceRootAbsent({
          readModel,
          command,
          workspaceRoot: command.workspaceRoot,
          exceptProjectId: command.projectId,
        });
      }
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "project.meta-updated",
        payload: {
          projectId: command.projectId,
          ...(command.title !== undefined ? { title: command.title } : {}),
          ...(command.workspaceRoot !== undefined ? { workspaceRoot: command.workspaceRoot } : {}),
          ...(command.defaultModelSelection !== undefined
            ? { defaultModelSelection: command.defaultModelSelection }
            : {}),
          ...(command.defaultThreadEnvMode !== undefined
            ? { defaultThreadEnvMode: command.defaultThreadEnvMode }
            : {}),
          ...(command.autoPull !== undefined ? { autoPull: command.autoPull } : {}),
          ...(command.faviconPath !== undefined ? { faviconPath: command.faviconPath } : {}),
          ...(command.projectIcon !== undefined ? { projectIcon: command.projectIcon } : {}),
          ...(command.scripts !== undefined ? { scripts: command.scripts } : {}),
          updatedAt: occurredAt,
        },
      };
    }

    case "project.delete": {
      yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      const activeThreads = listThreadsByProjectId(readModel, command.projectId).filter(
        (thread) => thread.deletedAt === null,
      );
      if (activeThreads.length > 0 && command.force !== true) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Project '${command.projectId}' is not empty and cannot be deleted without force=true.`,
        });
      }
      if (activeThreads.length > 0) {
        return yield* decideCommandSequence({
          readModel,
          ...(workerStates !== undefined ? { workerStates } : {}),
          commands: [
            ...activeThreads.map(
              (thread): Extract<OrchestrationCommand, { type: "thread.delete" }> => ({
                type: "thread.delete",
                commandId: command.commandId,
                threadId: thread.id,
              }),
            ),
            {
              type: "project.delete",
              commandId: command.commandId,
              projectId: command.projectId,
            },
          ],
        });
      }

      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "project",
          aggregateId: command.projectId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "project.deleted" as const,
        payload: {
          projectId: command.projectId,
          deletedAt: occurredAt,
        },
      };
    }

    case "thread.create": {
      yield* requireProject({
        readModel,
        command,
        projectId: command.projectId,
      });
      yield* requireThreadAbsent({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          ...(command.historyImport === true ? { metadata: { historyImport: true } } : {}),
        })),
        type: "thread.created",
        payload: {
          threadId: command.threadId,
          projectId: command.projectId,
          title: command.title,
          modelSelection: command.modelSelection,
          runtimeMode: command.runtimeMode,
          interactionMode: command.interactionMode,
          branch: command.branch,
          worktreePath: command.worktreePath,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.delete": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const ownedIds = new Set<ThreadId>([command.threadId]);
      for (let depth = 0; depth < WORKER_MAX_DEPTH; depth += 1) {
        for (const thread of readModel.threads) {
          if (
            thread.deletedAt === null &&
            thread.worker &&
            ownedIds.has(thread.worker.ownerThreadId)
          ) {
            ownedIds.add(thread.id);
          }
        }
      }
      const descendantStops: PlannedOrchestrationEvent[] = [];
      for (const state of workerStates ?? []) {
        if (
          state.thread.id === command.threadId ||
          !ownedIds.has(state.thread.id) ||
          !workerIsLive(state)
        )
          continue;
        const stop = yield* decideOrchestrationCommand({
          readModel,
          command: {
            type: "thread.session.stop",
            commandId: command.commandId,
            threadId: state.thread.id,
            createdAt: yield* nowIso,
          },
        });
        descendantStops.push(...(Array.isArray(stop) ? stop : [stop]));
      }
      const occurredAt = yield* nowIso;
      const deleted: PlannedOrchestrationEvent = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.deleted",
        payload: {
          threadId: command.threadId,
          deletedAt: occurredAt,
        },
      };
      return descendantStops.length === 0 ? deleted : [...descendantStops, deleted];
    }

    case "thread.archive": {
      yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.archived",
        payload: {
          threadId: command.threadId,
          archivedAt: occurredAt,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.unarchive": {
      yield* requireThreadArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.unarchived",
        payload: {
          threadId: command.threadId,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.settle":
    case "thread.auto-settle": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (
        command.type === "thread.auto-settle" &&
        (thread.settledOverride !== null || thread.autoSettleDisabledAt != null)
      ) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} changed before automatic settlement`,
        });
      }
      // The server owns settle eligibility. A stale command must not settle
      // a thread whose session is coming alive or working.
      if (thread.session?.status === "starting" || thread.session?.status === "running") {
        return yield* new OrchestrationThreadSettleBlockedError({ threadId: command.threadId });
      }
      const pendingRequests = openRequests(thread);
      // Manual settlement dismisses async questions without answering them.
      // Native callbacks and approvals still need a response or interruption.
      if (
        Array.from(pendingRequests.values()).some(
          (activity) =>
            command.type === "thread.auto-settle" ||
            activity.kind !== "user-input.requested" ||
            !Predicate.isObject(activity.payload) ||
            activity.payload.responseMode !== "message",
        )
      ) {
        return yield* new OrchestrationThreadSettleBlockedError({ threadId: command.threadId });
      }
      const occurredAt = yield* nowIso;
      // Settling inside the adoption window would hide just-requested work.
      if (hasQueuedTurnStartForThread(thread, occurredAt)) {
        return yield* new OrchestrationThreadSettleBlockedError({ threadId: command.threadId });
      }
      // Settling an already-settled thread re-emits with the original
      // settledAt: the engine rejects zero-event commands, and bulk-settle /
      // double-click must stay silent no-ops rather than surface errors.
      const alreadySettled = thread.settledOverride === "settled" && thread.settledAt !== null;
      const settledEvent = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.settled" as const,
        payload: {
          threadId: command.threadId,
          settledAt: alreadySettled
            ? thread.settledAt
            : command.type === "thread.auto-settle"
              ? command.settledAt
              : occurredAt,
          // A re-emission is a projected no-op: keep the existing updatedAt
          // so duplicate settles neither rewind nor churn ordering. A fresh
          // settle stamps the command time.
          updatedAt: alreadySettled ? thread.updatedAt : occurredAt,
        },
      };
      // Settling is "I'm done with this": clear states that would keep the
      // row pinned or snoozed instead of showing the new settled state.
      const companionEvents: Array<Omit<OrchestrationEvent, "sequence">> = [];
      for (const [requestId, request] of pendingRequests) {
        companionEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt,
            commandId: command.commandId,
          })),
          type: "thread.activity-appended",
          payload: {
            threadId: command.threadId,
            activity: {
              id: EventId.make(`settle:${command.commandId}:${requestId}`),
              kind: "user-input.resolved",
              summary: "User input dismissed",
              tone: "info",
              turnId: request.turnId,
              createdAt: occurredAt,
              payload: { requestId, responseMode: "message" },
            },
          },
        });
      }
      if (thread.pinnedAt != null) {
        companionEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt,
            commandId: command.commandId,
          })),
          type: "thread.unpinned" as const,
          payload: {
            threadId: command.threadId,
            updatedAt: occurredAt,
          },
        });
      }
      if (thread.snoozedUntil != null) {
        companionEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt,
            commandId: command.commandId,
          })),
          type: "thread.unsnoozed",
          payload: {
            threadId: command.threadId,
            reason: "user",
            updatedAt: occurredAt,
          },
        });
      }
      return companionEvents.length > 0 ? [settledEvent, ...companionEvents] : settledEvent;
    }

    case "thread.unsettle": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Idempotent by re-emission (see thread.settle): reducing the event a
      // second time lands on the same override state. A re-emission keeps
      // the existing updatedAt so duplicates do not churn ordering.
      const alreadyPinnedActive = thread.settledOverride === "active";
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: command.reason,
          updatedAt: alreadyPinnedActive ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.snooze": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      // A wake time in the past would create a thread that is snoozed and
      // woken at once — the row would never leave the inbox but still carry
      // snooze state. Reject instead of silently normalizing. The negated
      // comparison also catches unparseable wake times (IsoDateTime is
      // structurally just a string): NaN fails every comparison, and an
      // unparseable snoozedUntil must never persist.
      if (!(Date.parse(command.snoozedUntil) > Date.parse(occurredAt))) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} snooze wake time ${command.snoozedUntil} is not in the future`,
        });
      }
      // Blocked-on-you work must not be snoozed away: a pending approval or
      // user-input request is the agent waiting on the user, and hiding it
      // defeats the request. (A running session IS snoozable — snooze only
      // affects visibility, never the agent.)
      if (openRequests(thread).size > 0) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} has a pending approval or user-input request and cannot be snoozed`,
        });
      }
      // A queued turn start — a user message no turn has adopted yet — is
      // invisible pending work: no session, no pending flags. Snoozing in
      // that window would hide a just-requested turn exactly the way settle
      // would.
      if (hasQueuedTurnStartForThread(thread, occurredAt)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} has a queued turn start and cannot be snoozed`,
        });
      }
      // Re-snoozing an already-snoozed thread to the SAME wake time is a
      // duplicate (double-click, raced clients): re-emit with the original
      // timestamps so the projection is a no-op. A different wake time is a
      // real change and stamps fresh.
      const existingSnoozedAt =
        thread.snoozedUntil === command.snoozedUntil && thread.snoozedAt != null
          ? thread.snoozedAt
          : null;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.snoozed",
        payload: {
          threadId: command.threadId,
          snoozedUntil: command.snoozedUntil,
          snoozedAt: existingSnoozedAt ?? occurredAt,
          updatedAt: existingSnoozedAt !== null ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.unsnooze": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Idempotent by re-emission (see thread.settle): waking a thread that
      // is not snoozed lands on the same null state without churning
      // updatedAt.
      const alreadyAwake = thread.snoozedUntil == null;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.unsnoozed",
        payload: {
          threadId: command.threadId,
          reason: command.reason,
          updatedAt: alreadyAwake ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.pin": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      // Re-pinning an already-pinned thread is a duplicate (double-click,
      // raced clients): re-emit with the original timestamps so the
      // projection is a no-op. Pinning has no lifecycle invariants — a pin
      // only ever promotes visibility, so it can never hide pending work.
      const existingPinnedAt = thread.pinnedAt ?? null;
      const pinnedEvent = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.pinned" as const,
        payload: {
          threadId: command.threadId,
          pinnedAt: existingPinnedAt ?? occurredAt,
          // A fresh pin takes the client's slot in the arranged order; on a
          // re-pin the existing key wins so raced duplicates cannot move a
          // thread the user already placed.
          ...(existingPinnedAt === null && command.orderKey !== undefined
            ? { pinOrderKey: command.orderKey }
            : {}),
          updatedAt: existingPinnedAt !== null ? thread.updatedAt : occurredAt,
        },
      };
      // Pinning is a promotion: it clears the parked states rather than
      // silently outranking them. An explicit settle un-settles (reason
      // "user", same override the un-settle button stamps), and a snooze's
      // return ticket is spent — the thread is on top NOW, not on Tuesday.
      const promotionEvents: Array<Omit<OrchestrationEvent, "sequence">> = [];
      if (thread.settledOverride === "settled") {
        promotionEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt,
            commandId: command.commandId,
          })),
          type: "thread.unsettled",
          payload: {
            threadId: command.threadId,
            reason: "user",
            updatedAt: occurredAt,
          },
        });
      }
      if (thread.snoozedUntil != null) {
        promotionEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt,
            commandId: command.commandId,
          })),
          type: "thread.unsnoozed",
          payload: {
            threadId: command.threadId,
            reason: "user",
            updatedAt: occurredAt,
          },
        });
      }
      return promotionEvents.length > 0 ? [pinnedEvent, ...promotionEvents] : pinnedEvent;
    }

    case "thread.unpin": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Idempotent by re-emission (see thread.settle): unpinning a thread
      // that is not pinned lands on the same null state without churning
      // updatedAt.
      const alreadyUnpinned = thread.pinnedAt == null;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.unpinned",
        payload: {
          threadId: command.threadId,
          updatedAt: alreadyUnpinned ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.pin.reorder": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Only pinned threads have a slot in the arranged order. Rejecting
      // (rather than silently pinning) keeps a raced reorder-after-unpin
      // from resurrecting a pin the user just cleared.
      if (thread.pinnedAt == null) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} is not pinned and cannot be reordered`,
        });
      }
      // Idempotent by re-emission (see thread.settle): a duplicate drop on
      // the same slot keeps the existing updatedAt so it projects as a no-op.
      const keyUnchanged = thread.pinOrderKey === command.orderKey;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.pin-reordered",
        payload: {
          threadId: command.threadId,
          orderKey: command.orderKey,
          updatedAt: keyUnchanged ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.auto-settle.set": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Idempotent by re-emission (see thread.unpin): setting the current
      // state again keeps the existing timestamps so duplicates do not churn
      // ordering. The flag is independent of the settled lifecycle: it only
      // gates the automatic paths, so it never blocks a manual settle.
      const currentlyDisabledAt = thread.autoSettleDisabledAt ?? null;
      const unchanged = command.enabled
        ? currentlyDisabledAt === null
        : currentlyDisabledAt !== null;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.auto-settle-set",
        payload: {
          threadId: command.threadId,
          autoSettleDisabledAt: command.enabled ? null : (currentlyDisabledAt ?? occurredAt),
          updatedAt: unchanged ? thread.updatedAt : occurredAt,
        },
      };
    }

    case "thread.active.reorder": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      // Snooze retains this slot. Changing it cannot wake the thread, and
      // accepting it handles races with snooze and retained wake timestamps.
      if (
        thread.deletedAt !== null ||
        thread.pinnedAt != null ||
        thread.settledOverride === "settled"
      ) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} is not active and cannot be reordered`,
        });
      }
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          activeOrderKey: command.orderKey,
          // Arranging the list is not thread activity or a lifecycle transition.
          updatedAt: thread.updatedAt,
        },
      };
    }

    case "thread.meta.update": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (
        thread.worker &&
        ((command.branch !== undefined && command.branch !== thread.branch) ||
          (command.worktreePath !== undefined && command.worktreePath !== thread.worktreePath))
      ) {
        return yield* new WorkerOperationError({
          operation: "send",
          code: "forbidden",
          detail: "Owned workers share their owner's checkout and branch.",
        });
      }
      // Old clients only see the derived single link. Unlink that request through
      // the same command path as modern clients, including stack dismissal, while
      // retaining other links they cannot see. Historical metadata events still replay unchanged.
      const legacy = legacyLinkedPullRequestOf(
        thread.pullRequests,
        thread.projectId,
        readModel.projects.find((project) => project.id === thread.projectId)?.repositoryIdentity,
      );
      const currentPullRequest =
        legacy === null
          ? null
          : (thread.pullRequests.find(
              (link) => link.url === legacy.url && link.number === legacy.number,
            ) ?? null);
      if (command.linkedPullRequest != null) {
        const { linkedPullRequest: linked, ...metadata } = command;
        const project = readModel.projects.find((project) => project.id === thread.projectId);
        // Historical clients can send links without a parseable URL.
        const host = URL.canParse(linked.url)
          ? new URL(linked.url).hostname
          : (project?.repositoryIdentity?.canonicalKey.split("/")[0] ?? "unknown");
        const hasMetadata = Object.entries(metadata).some(
          ([key, value]) => !["type", "commandId", "threadId"].includes(key) && value !== undefined,
        );
        return yield* decideCommandSequence({
          readModel,
          commands: [
            ...(hasMetadata ? [metadata] : []),
            ...(currentPullRequest?.source === "manual"
              ? [
                  {
                    type: "thread.pull-request.unlink" as const,
                    commandId: command.commandId,
                    threadId: command.threadId,
                    host: currentPullRequest.host,
                    repository: currentPullRequest.repository,
                    number: currentPullRequest.number,
                  },
                ]
              : []),
            {
              type: "thread.pull-request.link",
              commandId: command.commandId,
              threadId: command.threadId,
              ...legacyThreadPullRequestKey(linked, host),
              url: linked.url,
              source: "manual",
            },
          ],
        });
      }

      if (command.linkedPullRequest === null && currentPullRequest !== null) {
        const { linkedPullRequest: _linkedPullRequest, ...metadata } = command;
        const hasMetadata = Object.entries(metadata).some(
          ([key, value]) => !["type", "commandId", "threadId"].includes(key) && value !== undefined,
        );
        return yield* decideCommandSequence({
          readModel,
          commands: [
            ...(hasMetadata ? [metadata] : []),
            {
              type: "thread.pull-request.unlink",
              commandId: command.commandId,
              threadId: command.threadId,
              host: currentPullRequest.host,
              repository: currentPullRequest.repository,
              number: currentPullRequest.number,
            },
          ],
        });
      }
      const branch =
        command.branch !== undefined &&
        command.expectedBranch !== undefined &&
        thread.branch !== command.expectedBranch
          ? thread.branch
          : command.branch;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          ...(command.title !== undefined
            ? {
                title: command.title,
                titleState: {
                  source: "manual" as const,
                  version: command.commandId,
                  needsRefinement: false,
                },
              }
            : {}),
          ...(command.regenerateTitle === true
            ? {
                titleState: {
                  source: "generated" as const,
                  version: command.commandId,
                  needsRefinement: false,
                },
                regenerateTitle: true as const,
                previousTitle: thread.title,
                titleRegeneration: {
                  requestId: command.commandId,
                  startedAt: occurredAt,
                },
              }
            : {}),
          ...(command.title !== undefined && thread.titleRegeneration != null
            ? { titleRegeneration: null }
            : {}),
          ...(command.modelSelection !== undefined
            ? { modelSelection: command.modelSelection }
            : {}),
          ...(branch !== undefined ? { branch } : {}),
          ...(command.worktreePath !== undefined ? { worktreePath: command.worktreePath } : {}),
          ...(command.linkedPullRequest !== undefined
            ? { linkedPullRequest: command.linkedPullRequest }
            : {}),
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.pull-request.link": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const key = normalizeThreadPullRequestKey(command);
      const existing = findPullRequestLink(thread, key);
      // An explicit link on a dismissed stack member un-dismisses it; any
      // other duplicate is a no-op the engine would reject as zero-event.
      const undismisses =
        existing?.source === "stack-dismissed" &&
        (command.source === "manual" || command.source === "agent" || command.source === "created");
      if (existing !== undefined && !undismisses) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `pull request ${key.host}/${key.repository}#${key.number} is already linked to thread ${command.threadId}`,
        });
      }
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.pull-request-linked",
        payload: {
          threadId: command.threadId,
          link:
            existing !== undefined
              ? { ...existing, url: command.url, source: command.source }
              : {
                  ...key,
                  url: command.url,
                  source: command.source,
                  linkedAt: occurredAt,
                  snapshot: null,
                  stack: null,
                },
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.pull-request.unlink": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const key = normalizeThreadPullRequestKey(command);
      const existing = findPullRequestLink(thread, key);
      if (existing === undefined) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `pull request ${key.host}/${key.repository}#${key.number} is not linked to thread ${command.threadId}`,
        });
      }
      const occurredAt = yield* nowIso;
      const eventBase = yield* withEventBase({
        aggregateKind: "thread",
        aggregateId: command.threadId,
        occurredAt,
        commandId: command.commandId,
      });
      // Any known native-stack member needs a tombstone, regardless of who linked it.
      // A sibling can rediscover it even before this link has its own stack snapshot.
      const belongsToStack =
        existing.source === "stack" ||
        existing.stack !== null ||
        thread.pullRequests.some(
          (link) =>
            link.host.toLowerCase() === key.host &&
            link.repository.toLowerCase() === key.repository &&
            link.stack?.layers.some((layer) => layer.number === key.number),
        );
      if (belongsToStack) {
        return {
          ...eventBase,
          type: "thread.pull-request-linked",
          payload: {
            threadId: command.threadId,
            link: { ...existing, source: "stack-dismissed" },
            updatedAt: occurredAt,
          },
        };
      }
      return {
        ...eventBase,
        type: "thread.pull-request-unlinked",
        payload: {
          threadId: command.threadId,
          ...key,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.pull-request-link.sync": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const key = normalizeThreadPullRequestKey(command);
      if (findPullRequestLink(thread, key) === undefined) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `pull request ${key.host}/${key.repository}#${key.number} is not linked to thread ${command.threadId}`,
        });
      }
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.pull-request-synced",
        payload: {
          threadId: command.threadId,
          ...key,
          snapshot: command.snapshot,
          stack: command.stack,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.pull-request.sync": {
      const thread = yield* requireThreadNotArchived({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (thread.deletedAt !== null) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} was deleted before pull request discovery`,
        });
      }
      if (
        thread.projectId !== command.projectId ||
        thread.branch !== command.expected.branch ||
        thread.worktreePath !== command.expected.worktreePath ||
        !threadPullRequestLinksEqual(
          thread.linkedPullRequest ?? null,
          command.expected.linkedPullRequest,
        ) ||
        !threadPullRequestLinksEqual(
          thread.branchPullRequest ?? null,
          command.expected.branchPullRequest,
        )
      ) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `thread ${command.threadId} changed before pull request discovery`,
        });
      }
      const project = yield* requireProject({ readModel, command, projectId: command.projectId });
      if (project.deletedAt !== null || project.workspaceRoot !== command.expected.workspaceRoot) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `project ${command.projectId} changed before pull request discovery`,
        });
      }
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          branchPullRequest: command.branchPullRequest,
          ...(command.linkedPullRequest !== undefined
            ? { linkedPullRequest: command.linkedPullRequest }
            : {}),
          updatedAt: thread.updatedAt,
        },
      };
    }

    case "thread.title.generate.complete": {
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      const current =
        thread.deletedAt === null &&
        thread.titleState?.source !== "manual" &&
        thread.title === command.expectedTitle &&
        (thread.titleState?.version ?? null) === command.expectedVersion &&
        thread.titleRegeneration == null;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: yield* nowIso,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          ...(current
            ? {
                title: command.title,
                titleState: {
                  source: "generated" as const,
                  version: command.commandId,
                  needsRefinement: command.needsRefinement,
                },
              }
            : {}),
          updatedAt: thread.updatedAt,
        },
      };
    }

    case "thread.title.refine": {
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      const current =
        thread.deletedAt === null &&
        thread.latestTurn?.state === "completed" &&
        thread.session?.status === "ready" &&
        thread.titleState?.source === "generated" &&
        thread.titleState.version === command.expectedVersion &&
        thread.titleState.needsRefinement &&
        thread.titleRegeneration == null;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          ...(current
            ? {
                titleState: {
                  source: "generated" as const,
                  version: command.commandId,
                  needsRefinement: false,
                },
                regenerateTitle: true as const,
                previousTitle: thread.title,
                titleRegeneration: { requestId: command.commandId, startedAt: occurredAt },
              }
            : {}),
          updatedAt: thread.updatedAt,
        },
      };
    }

    case "thread.title.regeneration.complete": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const requestIsCurrent = thread.titleRegeneration?.requestId === command.requestId;
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.meta-updated",
        payload: {
          threadId: command.threadId,
          ...(requestIsCurrent && command.title !== undefined ? { title: command.title } : {}),
          ...(requestIsCurrent ? { titleRegeneration: null } : {}),
          updatedAt: requestIsCurrent ? occurredAt : thread.updatedAt,
        },
      };
    }

    case "thread.runtime-mode.set": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (thread.worker) yield* requireWorkerChain(readModel, thread, "send", command.runtimeMode);
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.runtime-mode-set",
        payload: {
          threadId: command.threadId,
          runtimeMode: command.runtimeMode,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.interaction-mode.set": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const occurredAt = yield* nowIso;
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt,
          commandId: command.commandId,
        })),
        type: "thread.interaction-mode-set",
        payload: {
          threadId: command.threadId,
          interactionMode: command.interactionMode,
          updatedAt: occurredAt,
        },
      };
    }

    case "thread.turn.start": {
      if (isImportedAgentSessionMessageId(command.message.messageId)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Message id '${command.message.messageId}' uses the reserved imported-session namespace.`,
        });
      }
      const targetThread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (targetThread.runtimeHandoff?.status === "pending")
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "A runtime handoff must finish or fail before accepting another turn.",
        });
      if (targetThread.worker) {
        yield* requireWorkerChain(readModel, targetThread, "send");
        yield* requireWorkerAdmission(
          targetThread.worker.rootThreadId,
          workerStates ?? [],
          "send",
          targetThread.id,
        );
      }
      const sourceProposedPlan = command.sourceProposedPlan;
      const sourceThread = sourceProposedPlan
        ? yield* requireThread({
            readModel,
            command,
            threadId: sourceProposedPlan.threadId,
          })
        : null;
      const sourcePlan =
        sourceProposedPlan && sourceThread
          ? sourceThread.proposedPlans.find((entry) => entry.id === sourceProposedPlan.planId)
          : null;
      if (sourceProposedPlan && !sourcePlan) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Proposed plan '${sourceProposedPlan.planId}' does not exist on thread '${sourceProposedPlan.threadId}'.`,
        });
      }
      if (sourceThread && sourceThread.projectId !== targetThread.projectId) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Proposed plan '${sourceProposedPlan?.planId}' belongs to thread '${sourceThread.id}' in a different project.`,
        });
      }
      // A worktree bootstrap persists the message ahead of the turn with
      // `thread.message.user.append`; the turn then only references it.
      const persistedUserMessage = targetThread.messages.find(
        (message) =>
          message.id === command.message.messageId &&
          message.role === "user" &&
          message.turnId === null,
      );
      const userMessageEvent: Omit<OrchestrationEvent, "sequence"> | null = persistedUserMessage
        ? null
        : {
            ...(yield* withEventBase({
              aggregateKind: "thread",
              aggregateId: command.threadId,
              occurredAt: command.createdAt,
              commandId: command.commandId,
            })),
            type: "thread.message-sent",
            payload: {
              threadId: command.threadId,
              messageId: command.message.messageId,
              role: "user",
              text: command.message.text,
              attachments: command.message.attachments,
              ...(command.message.context !== undefined
                ? { context: command.message.context }
                : {}),
              turnId: null,
              streaming: false,
              createdAt: command.createdAt,
              updatedAt: command.createdAt,
            },
          };
      const turnStartRequestedEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        ...(userMessageEvent ? { causationEventId: userMessageEvent.eventId } : {}),
        type: "thread.turn-start-requested",
        payload: {
          ...(command.unattendedAuthority !== undefined
            ? { unattendedAuthority: command.unattendedAuthority }
            : {}),
          threadId: command.threadId,
          messageId: command.message.messageId,
          ...(command.modelSelection !== undefined
            ? { modelSelection: command.modelSelection }
            : {}),
          ...(command.titleSeed !== undefined ? { titleSeed: command.titleSeed } : {}),
          runtimeMode: targetThread.runtimeMode,
          interactionMode: targetThread.interactionMode,
          ...(sourceProposedPlan !== undefined ? { sourceProposedPlan } : {}),
          createdAt: command.createdAt,
        },
      };
      // Real activity resets ANY override: it wakes an explicitly settled
      // thread, and it clears a keep-active pin back to neutral so the
      // thread can auto-settle again after this burst of work goes stale.
      // A snooze clears the same way — sending a message to a snoozed
      // thread is the user re-engaging, so the return ticket is spent.
      const lifecycleResetEvents: Array<Omit<OrchestrationEvent, "sequence">> = [];
      if (targetThread.settledOverride !== null) {
        lifecycleResetEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt: command.createdAt,
            commandId: command.commandId,
          })),
          type: "thread.unsettled",
          payload: {
            threadId: command.threadId,
            reason: "activity",
            updatedAt: command.createdAt,
          },
        });
      }
      if (targetThread.snoozedUntil != null) {
        lifecycleResetEvents.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt: command.createdAt,
            commandId: command.commandId,
          })),
          type: "thread.unsnoozed",
          payload: {
            threadId: command.threadId,
            reason: "activity",
            updatedAt: command.createdAt,
          },
        });
      }
      return [
        ...lifecycleResetEvents,
        ...(userMessageEvent ? [userMessageEvent] : []),
        turnStartRequestedEvent,
      ];
    }

    case "thread.message.user.append": {
      if (isImportedAgentSessionMessageId(command.message.messageId)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Message id '${command.message.messageId}' uses the reserved imported-session namespace.`,
        });
      }
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (thread.messages.some((message) => message.id === command.message.messageId)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Message '${command.message.messageId}' already exists on thread '${command.threadId}'.`,
        });
      }
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: { deferredTurn: true },
        })),
        type: "thread.message-sent",
        payload: {
          threadId: command.threadId,
          messageId: command.message.messageId,
          role: "user",
          text: command.message.text,
          attachments: command.message.attachments,
          ...(command.message.context !== undefined ? { context: command.message.context } : {}),
          turnId: null,
          streaming: false,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.turn.interrupt": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.turn-interrupt-requested",
        payload: {
          threadId: command.threadId,
          ...(command.expectedMessageId !== undefined
            ? { expectedMessageId: command.expectedMessageId }
            : {}),
          ...(command.expectedTurnId !== undefined
            ? { expectedTurnId: command.expectedTurnId }
            : {}),
          ...(command.turnId !== undefined ? { turnId: command.turnId } : {}),
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.approval.respond": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: {
            requestId: command.requestId,
          },
        })),
        type: "thread.approval-response-requested",
        payload: {
          threadId: command.threadId,
          requestId: command.requestId,
          decision: command.decision,
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.user-input.respond": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const request = userInputActivity;
      const attachments = Object.values(command.attachmentsByQuestionId ?? {}).flat();
      let questionTextById: Record<string, string> = {};
      if (attachments.length > 0) {
        const payload =
          request?.kind === "user-input.requested"
            ? decodeUserInputRequestedPayload(request.payload)
            : Option.none();
        if (Option.isNone(payload)) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail:
              request?.kind === "user-input.resolved"
                ? "This question has already been answered."
                : "This question is no longer pending.",
          });
        }
        questionTextById = Object.fromEntries(
          payload.value.questions.map((question) => [question.id, question.question]),
        );
        for (const questionId of Object.keys(command.attachmentsByQuestionId ?? {})) {
          const question = payload.value.questions.find((question) => question.id === questionId);
          if (!question || question.allowCustomAnswer === false) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: command.type,
              detail: "This question does not accept file references.",
            });
          }
        }
      }
      if (
        request &&
        Predicate.isObject(request.payload) &&
        request.payload.responseMode === "message"
      ) {
        const payload = decodeUserInputRequestedPayload(request.payload);
        if (request.kind !== "user-input.requested" || Option.isNone(payload)) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail: "This question has already been answered.",
          });
        }
        const replies: string[] = [];
        for (const question of payload.value.questions) {
          const answer = command.answers[question.id];
          if (
            typeof answer !== "string" ||
            (answer.trim().length === 0 && !command.attachmentsByQuestionId?.[question.id]?.length)
          ) {
            return yield* new OrchestrationCommandInvariantError({
              commandType: command.type,
              detail: "Answer each question before sending.",
            });
          }
          const questionAttachments = command.attachmentsByQuestionId?.[question.id] ?? [];
          const attachmentLabels = questionAttachments
            .map((attachment) => `Attached file: ${attachment.name} (${attachment.id})`)
            .join("\n");
          replies.push(
            [`${question.question}\n${answer.trim()}`, attachmentLabels].filter(Boolean).join("\n"),
          );
        }
        // Commit the answer and its message together. The normal turn path
        // steers a running agent or resumes an idle session.
        return yield* decideCommandSequence({
          readModel,
          commands: [
            {
              type: "thread.activity.append",
              commandId: command.commandId,
              threadId: command.threadId,
              createdAt: command.createdAt,
              activity: {
                id: EventId.make(`async-answer:${command.requestId}`),
                kind: "user-input.resolved",
                summary: "User input submitted",
                tone: "info",
                turnId: request.turnId,
                createdAt: command.createdAt,
                payload: {
                  requestId: command.requestId,
                  responseMode: "message",
                  answers: command.answers,
                  ...(command.attachmentsByQuestionId
                    ? { attachmentsByQuestionId: command.attachmentsByQuestionId }
                    : {}),
                },
              },
            },
            {
              type: "thread.turn.start",
              commandId: command.commandId,
              threadId: command.threadId,
              createdAt: command.createdAt,
              runtimeMode: thread.runtimeMode,
              interactionMode: thread.interactionMode,
              message: {
                messageId: MessageId.make(`async-answer:${command.requestId}`),
                role: "user",
                text: replies.join("\n\n"),
                attachments,
              },
            },
          ],
        });
      }
      const responseEvent = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: { requestId: command.requestId },
        })),
        type: "thread.user-input-response-requested" as const,
        payload: {
          threadId: command.threadId,
          requestId: command.requestId,
          answers: command.answers,
          ...(command.attachmentsByQuestionId
            ? { attachmentsByQuestionId: command.attachmentsByQuestionId }
            : {}),
          createdAt: command.createdAt,
        },
      };
      if (attachments.length === 0) return responseEvent;
      const historyEvent = yield* decideOrchestrationCommand({
        readModel,
        command: {
          type: "thread.activity.append",
          commandId: command.commandId,
          threadId: command.threadId,
          createdAt: command.createdAt,
          activity: {
            id: EventId.make(`question-answer:${command.commandId}`),
            kind: "user-input.answer-submitted",
            summary: "Question answer submitted",
            tone: "info",
            turnId: request?.turnId ?? null,
            createdAt: command.createdAt,
            payload: {
              requestId: command.requestId,
              answers: command.answers,
              questionTextById,
              attachmentsByQuestionId: command.attachmentsByQuestionId,
              detail: attachments.map((attachment) => attachment.name).join("\n"),
            },
          },
        },
      });
      return [...(Array.isArray(historyEvent) ? historyEvent : [historyEvent]), responseEvent];
    }

    case "thread.user-input.dismiss": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const request = userInputActivity;
      if (request === undefined || request.kind !== "user-input.requested") {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "This question has already been answered.",
        });
      }
      // Only async questions can be dropped silently. A native callback
      // question leaves the provider blocked until it gets a reply, so it
      // still needs an answer or an interrupted turn.
      if (!Predicate.isObject(request.payload) || request.payload.responseMode !== "message") {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "This question needs an answer. Answer it or stop the turn.",
        });
      }
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.activity-appended",
        payload: {
          threadId: command.threadId,
          activity: {
            id: EventId.make(`async-dismiss:${command.requestId}`),
            kind: "user-input.resolved",
            summary: "User input dismissed",
            tone: "info",
            turnId: request.turnId,
            createdAt: command.createdAt,
            payload: { requestId: command.requestId, responseMode: "message" },
          },
        },
      };
    }

    case "thread.conversation.revert":
    case "thread.checkpoint.revert": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.checkpoint-revert-requested",
        payload: {
          threadId: command.threadId,
          turnCount: command.turnCount,
          ...(command.type === "thread.conversation.revert" ? { restoreFiles: false } : {}),
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.session.stop": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      // Settle-cleanup stops are conditional: between the settle landing and
      // this command, another client may have re-engaged the thread (a turn
      // start unsettles it and brings the session alive). Commands are
      // decided serially against this read model, so checking here — not in
      // the dispatcher's pre-settle snapshot — closes that race.
      if (command.onlyIfSettled === true) {
        const sessionComingAlive =
          thread.session?.status === "starting" || thread.session?.status === "running";
        if (
          thread.settledOverride !== "settled" ||
          sessionComingAlive ||
          hasQueuedTurnStartForThread(thread, command.createdAt)
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: command.type,
            detail: `thread ${command.threadId} was re-engaged after settle; skipping session stop`,
          });
        }
      }
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.session-stop-requested",
        payload: {
          threadId: command.threadId,
          ...(command.expectedMessageId !== undefined
            ? { expectedMessageId: command.expectedMessageId }
            : {}),
          ...(command.expectedTurnId !== undefined
            ? { expectedTurnId: command.expectedTurnId }
            : {}),
          createdAt: command.createdAt,
        },
      };
    }

    case "thread.session.set": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const sessionSetEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          metadata: {},
        })),
        type: "thread.session-set",
        payload: {
          threadId: command.threadId,
          session: command.session,
        },
      };
      // Only a session coming alive is activity worth waking a settled thread
      // for — status writes like ready/stopped/error arrive after the fact and
      // must not fight a user's explicit settle. Snooze is deliberately NOT
      // cleared here: snooze never pauses the agent, so its session starting
      // or erroring is not the user re-engaging. Blocked/failed work still
      // surfaces immediately — effectiveSnoozed refuses to classify a thread
      // with a raised hand (approval / input / failure / fresh completion)
      // as snoozed, without spending the return ticket.
      const isSessionActivity =
        command.session.status === "starting" || command.session.status === "running";
      // Real activity resets ANY override (settled wakes, active unpins).
      if (thread.settledOverride === null || !isSessionActivity) {
        return sessionSetEvent;
      }
      const unsettledEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: "activity",
          updatedAt: command.createdAt,
        },
      };
      return [unsettledEvent, sessionSetEvent];
    }

    case "thread.message.assistant.delta":
    case "thread.message.reasoning.delta": {
      if (isImportedAgentSessionMessageId(command.messageId)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Message id '${command.messageId}' uses the reserved imported-session namespace.`,
        });
      }
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.message-sent",
        payload: {
          threadId: command.threadId,
          messageId: command.messageId,
          role: command.type === "thread.message.reasoning.delta" ? "reasoning" : "assistant",
          text: command.delta,
          turnId: command.turnId ?? null,
          streaming: true,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.message.assistant.complete":
    case "thread.message.reasoning.complete": {
      if (isImportedAgentSessionMessageId(command.messageId)) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Message id '${command.messageId}' uses the reserved imported-session namespace.`,
        });
      }
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.message-sent",
        payload: {
          threadId: command.threadId,
          messageId: command.messageId,
          role: command.type === "thread.message.reasoning.complete" ? "reasoning" : "assistant",
          text: "",
          turnId: command.turnId ?? null,
          streaming: false,
          createdAt: command.createdAt,
          updatedAt: command.createdAt,
        },
      };
    }

    case "thread.runtime.handoff": {
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      const admission = runtimeHandoffDecision(thread, command.expectedUpdatedAt, {
        pendingTurn: hasQueuedTurnStartForThread(thread, command.createdAt),
        unresolvedApproval: openRequests(thread).size > 0,
        unresolvedInput: false,
        nativeBackgroundWork:
          workerStates?.find((state) => state.thread.id === thread.id)?.thread.backgroundLiveness !=
          null,
        ownedWorkerActivation: [...(workerStates?.values() ?? [])].some(
          (state) =>
            state.thread.worker?.ownerThreadId === thread.id &&
            (state.pendingMessageId !== null ||
              state.thread.session?.status === "running" ||
              state.thread.session?.status === "starting"),
        ),
      });
      if (admission || thread.runtimeHandoff?.status === "pending")
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: admission?.detail ?? "A runtime handoff is already pending.",
        });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: thread.id,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.runtime-handoff-requested",
        payload: {
          threadId: thread.id,
          handoff: {
            operationId: command.operationId,
            epochId: command.epochId,
            status: "pending",
            targetModelSelection: command.targetModelSelection,
            seed: command.seed,
            requestedAt: command.createdAt,
          },
          updatedAt: command.createdAt,
        },
      };
    }
    case "thread.runtime.handoff.commit": {
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      if (
        thread.runtimeHandoff?.status !== "pending" ||
        thread.runtimeHandoff.epochId !== command.expectedEpochId ||
        (thread.session !== null && thread.session.status !== "stopped") ||
        openRequests(thread).size > 0 ||
        hasQueuedTurnStartForThread(thread, command.createdAt)
      )
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Handoff epoch changed or native stop has not been acknowledged.",
        });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: thread.id,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.runtime-handoff-committed",
        payload: {
          threadId: thread.id,
          epochId: command.expectedEpochId,
          targetModelSelection: thread.runtimeHandoff.targetModelSelection,
          updatedAt: command.createdAt,
        },
      };
    }
    case "thread.runtime.handoff.fail": {
      const thread = yield* requireThread({ readModel, command, threadId: command.threadId });
      if (
        thread.runtimeHandoff?.status !== "pending" ||
        thread.runtimeHandoff.epochId !== command.expectedEpochId
      )
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Pending handoff epoch changed before failure acknowledgement.",
        });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: thread.id,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.runtime-handoff-failed",
        payload: {
          threadId: thread.id,
          epochId: command.expectedEpochId,
          updatedAt: command.createdAt,
        },
      };
    }
    case "thread.runtime.fork": {
      const source = yield* requireThread({ readModel, command, threadId: command.sourceThreadId });
      const orderedMessages = source.messages.toSorted(
        (a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id),
      );
      const boundary = orderedMessages.findIndex(
        (message) => message.id === command.throughMessageId,
      );
      if (
        source.updatedAt !== command.expectedUpdatedAt ||
        source.deletedAt !== null ||
        boundary < 0 ||
        orderedMessages[boundary]?.streaming ||
        (orderedMessages[boundary]?.role !== "user" &&
          orderedMessages[boundary]?.role !== "assistant")
      )
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Fork source changed or cut point is not a completed visible message.",
        });
      const messages = orderedMessages
        .slice(0, boundary + 1)
        .filter((message) => message.role === "user" || message.role === "assistant");
      if (
        messages.length > 2000 ||
        messages.reduce((size, message) => size + message.text.length, 0) > 4_194_304
      )
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Fork history exceeds the bounded import limit.",
        });
      const events = yield* decideCommandSequence({
        readModel,
        commands: [
          {
            type: "thread.create",
            commandId: command.commandId,
            threadId: command.threadId,
            projectId: source.projectId,
            title: command.title,
            modelSelection: command.modelSelection,
            runtimeMode: source.runtimeMode,
            interactionMode: source.interactionMode,
            branch: source.branch,
            worktreePath: source.worktreePath,
            createdAt: command.createdAt,
            historyImport: true,
          },
          {
            type: "thread.history.import",
            commandId: command.commandId,
            threadId: command.threadId,
            messages: messages.map((message, index) => ({
              messageId: MessageId.make(
                `import:fork:${command.threadId}:${String(index).padStart(6, "0")}`,
              ),
              role: message.role === "user" ? ("user" as const) : ("assistant" as const),
              text: message.text,
              createdAt: message.createdAt,
            })),
          },
        ],
      });
      return [
        ...events,
        {
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt: command.createdAt,
            commandId: command.commandId,
          })),
          type: "thread.runtime-forked" as const,
          payload: {
            threadId: command.threadId,
            sourceThreadId: source.id,
            throughMessageId: command.throughMessageId,
            sourceMessageIds: messages.map((message) => message.id),
            updatedAt: command.createdAt,
          },
        },
      ];
    }
    case "thread.history.import": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      if (
        thread.deletedAt !== null ||
        thread.archivedAt !== null ||
        thread.messages.length > 0 ||
        thread.latestTurn !== null ||
        thread.session !== null ||
        openRequests(thread).size > 0
      ) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `Thread '${command.threadId}' must be active and empty before history can be imported.`,
        });
      }
      const firstMessage = command.messages[0];
      if (firstMessage === undefined) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: "Thread history imports require at least one message.",
        });
      }

      const events: Array<PlannedOrchestrationEvent> = [];
      for (const message of command.messages) {
        events.push({
          ...(yield* withEventBase({
            aggregateKind: "thread",
            aggregateId: command.threadId,
            occurredAt: message.createdAt,
            commandId: command.commandId,
            metadata: { historyImport: true },
          })),
          type: "thread.message-sent",
          payload: {
            threadId: command.threadId,
            messageId: message.messageId,
            role: message.role,
            text: message.text,
            turnId: null,
            streaming: false,
            createdAt: message.createdAt,
            updatedAt: message.createdAt,
          },
        });
      }
      const settledAt = command.messages.reduce(
        (latest, message) =>
          compareDateTimeStrings(message.createdAt, latest) > 0 ? message.createdAt : latest,
        firstMessage.createdAt,
      );
      events.push({
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: settledAt,
          commandId: command.commandId,
          metadata: { historyImport: true },
        })),
        type: "thread.settled",
        payload: {
          threadId: command.threadId,
          settledAt,
          updatedAt: settledAt,
        },
      });
      return events;
    }

    case "thread.proposed-plan.upsert": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.proposed-plan-upserted",
        payload: {
          threadId: command.threadId,
          proposedPlan: command.proposedPlan,
        },
      };
    }

    case "thread.turn.diff.complete": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      // A placeholder (status "missing") must never replace a checkpoint that
      // was already captured with a real git ref. Provider diff ingestion
      // checks this before dispatching, but CheckpointReactor can commit the
      // real capture in between; the decider runs under the engine's command
      // lock, so rejecting here closes that window.
      const existingCheckpoint = thread.checkpoints.find(
        (checkpoint) => checkpoint.turnId === command.turnId,
      );
      if (
        command.status === "missing" &&
        existingCheckpoint !== undefined &&
        existingCheckpoint.status !== "missing"
      ) {
        return yield* new OrchestrationCommandInvariantError({
          commandType: command.type,
          detail: `turn ${command.turnId} already has a captured checkpoint`,
        });
      }
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.turn-diff-completed",
        payload: {
          threadId: command.threadId,
          turnId: command.turnId,
          checkpointTurnCount: command.checkpointTurnCount,
          checkpointRef: command.checkpointRef,
          status: command.status,
          files: command.files,
          assistantMessageId: command.assistantMessageId ?? null,
          completedAt: command.completedAt,
        },
      };
    }

    case "thread.revert.complete": {
      yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      return {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.reverted",
        payload: {
          threadId: command.threadId,
          turnCount: command.turnCount,
        },
      };
    }

    case "thread.activity.append": {
      const thread = yield* requireThread({
        readModel,
        command,
        threadId: command.threadId,
      });
      const requestId =
        typeof command.activity.payload === "object" &&
        command.activity.payload !== null &&
        "requestId" in command.activity.payload &&
        typeof (command.activity.payload as { requestId?: unknown }).requestId === "string"
          ? ((command.activity.payload as { requestId: string })
              .requestId as OrchestrationEvent["metadata"]["requestId"])
          : undefined;
      const activityAppendedEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
          ...(requestId !== undefined ? { metadata: { requestId } } : {}),
        })),
        type: "thread.activity-appended",
        payload: {
          threadId: command.threadId,
          activity: command.activity,
        },
      };
      // An approval or user-input request is blocked-on-you work — it must
      // never stay hidden inside a settled slim row.
      const wakesSettledThread =
        command.activity.kind === "approval.requested" ||
        command.activity.kind === "user-input.requested";
      // Real activity resets ANY override (settled wakes, active unpins).
      if (thread.settledOverride === null || !wakesSettledThread) {
        return activityAppendedEvent;
      }
      const unsettledEvent: Omit<OrchestrationEvent, "sequence"> = {
        ...(yield* withEventBase({
          aggregateKind: "thread",
          aggregateId: command.threadId,
          occurredAt: command.createdAt,
          commandId: command.commandId,
        })),
        type: "thread.unsettled",
        payload: {
          threadId: command.threadId,
          reason: "activity",
          updatedAt: command.createdAt,
        },
      };
      return [unsettledEvent, activityAppendedEvent];
    }

    default: {
      command satisfies never;
      const fallback = command as never as { type: string };
      return yield* new OrchestrationCommandInvariantError({
        commandType: fallback.type,
        detail: `Unknown command type: ${fallback.type}`,
      });
    }
  }
});
