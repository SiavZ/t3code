import type {
  OrchestrationClientOrigin,
  OrchestrationEvent,
  OrchestrationReadModel,
  ProjectId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";
import {
  OrchestrationCommand,
  ThreadId,
  WorkerOperationError,
  CoordinationPlan,
  CoordinationError,
  CoordinationMailbox,
  RuntimeMode,
  WorkerMcpCapability,
  isWorkerRuntimeModeAllowed,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Clock from "effect/Clock";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as Metric from "effect/Metric";
import * as Option from "effect/Option";
import * as PubSub from "effect/PubSub";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

const decodeCoordinationPlan = Schema.decodeUnknownEffect(CoordinationPlan);
const decodeCoordinationMailbox = Schema.decodeUnknownEffect(CoordinationMailbox);
const decodeGrantCeiling = Schema.decodeUnknownEffect(
  Schema.fromJsonString(
    Schema.Struct({ runtimeMode: RuntimeMode, mcpCapabilities: Schema.Array(WorkerMcpCapability) }),
  ),
);
const isWorkerOperationError = Schema.is(WorkerOperationError);

import {
  metricAttributes,
  orchestrationCommandAckDuration,
  orchestrationCommandsTotal,
  orchestrationCommandDuration,
} from "../../observability/Metrics.ts";
import { toPersistenceSqlError } from "../../persistence/Errors.ts";
import { OrchestrationEventStore } from "../../persistence/Services/OrchestrationEventStore.ts";
import { OrchestrationCommandReceiptRepository } from "../../persistence/Services/OrchestrationCommandReceipts.ts";
import {
  isOrchestrationCommandRejection,
  OrchestrationCommandIdConflictError,
  OrchestrationCommandInvariantError,
  OrchestrationCommandPreviouslyRejectedError,
  type OrchestrationDispatchError,
  type OrchestrationProjectorDecodeError,
} from "../Errors.ts";
import { decideOrchestrationCommand } from "../decider.ts";
import { createEmptyReadModel, projectEvent } from "../projector.ts";
import { OrchestrationProjectionPipeline } from "../Services/ProjectionPipeline.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { ThreadBackgroundLivenessService } from "../ThreadBackgroundLiveness.ts";
import {
  OrchestrationEngineService,
  type OrchestrationEngineShape,
} from "../Services/OrchestrationEngine.ts";
const isOrchestrationCommandPreviouslyRejectedError = Schema.is(
  OrchestrationCommandPreviouslyRejectedError,
);
const isOrchestrationCommandIdConflictError = Schema.is(OrchestrationCommandIdConflictError);

interface CommandEnvelope {
  command: OrchestrationCommand;
  origin: OrchestrationClientOrigin | undefined;
  result: Deferred.Deferred<{ sequence: number }, OrchestrationDispatchError>;
  startedAtMs: number;
}

function commandToAggregateRef(command: OrchestrationCommand): {
  readonly aggregateKind: "project" | "thread";
  readonly aggregateId: ProjectId | ThreadId;
} {
  switch (command.type) {
    case "project.create":
    case "project.meta.update":
    case "project.delete":
      return {
        aggregateKind: "project",
        aggregateId: command.projectId,
      };
    default:
      return {
        aggregateKind: "thread",
        aggregateId: command.threadId,
      };
  }
}

const makeOrchestrationEngine = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const eventStore = yield* OrchestrationEventStore;
  const commandReceiptRepository = yield* OrchestrationCommandReceiptRepository;
  const projectionPipeline = yield* OrchestrationProjectionPipeline;
  const projectionSnapshotQuery = yield* ProjectionSnapshotQuery;
  const threadBackgroundLiveness = yield* ThreadBackgroundLivenessService;
  const crypto = yield* Crypto.Crypto;

  const nowIso = Effect.map(DateTime.now, DateTime.formatIso);
  let commandReadModel = createEmptyReadModel(yield* nowIso);

  const commandQueue = yield* Queue.unbounded<CommandEnvelope>();
  const eventPubSub = yield* PubSub.unbounded<OrchestrationEvent>();

  const projectEventsOntoReadModel = (
    baseReadModel: OrchestrationReadModel,
    events: ReadonlyArray<OrchestrationEvent>,
  ): Effect.Effect<OrchestrationReadModel, OrchestrationProjectorDecodeError, never> =>
    Effect.gen(function* () {
      let nextReadModel = baseReadModel;
      for (const event of events) {
        nextReadModel = yield* projectEvent(nextReadModel, event);
      }
      return nextReadModel;
    });

  const processEnvelope = (envelope: CommandEnvelope): Effect.Effect<void> => {
    const coordinationFingerprint = envelope.command.type.startsWith("coordination.")
      ? NodeCrypto.createHash("sha256")
          .update(JSON.stringify({ ...envelope.command, createdAt: undefined }))
          .digest("hex")
      : undefined;
    const workerCommand =
      envelope.command.type === "thread.worker.spawn" ||
      envelope.command.type === "thread.worker.send" ||
      envelope.command.type === "thread.worker.stop"
        ? {
            type: envelope.command.type,
            callerThreadId: envelope.command.callerThreadId,
            fingerprint: NodeCrypto.createHash("sha256")
              .update(
                JSON.stringify([
                  envelope.command.type,
                  envelope.command.callerThreadId,
                  envelope.command.threadId,
                  envelope.command.type === "thread.worker.spawn"
                    ? envelope.command.spawnFingerprint
                    : envelope.command.type === "thread.worker.send"
                      ? envelope.command.text
                      : null,
                ]),
              )
              .digest("hex"),
          }
        : undefined;
    const dispatchStartSequence = commandReadModel.snapshotSequence;
    let existingReceiptFound = false;
    let processingStartedAtMs = 0;
    const aggregateRef = commandToAggregateRef(envelope.command);
    const baseMetricAttributes = {
      commandType: envelope.command.type,
      aggregateKind: aggregateRef.aggregateKind,
    } as const;
    const reconcileReadModelAfterDispatchFailure = Effect.gen(function* () {
      const persistedEvents = yield* Stream.runCollect(
        eventStore.readFromSequence(dispatchStartSequence),
      ).pipe(Effect.map((chunk): OrchestrationEvent[] => Array.from(chunk)));
      if (persistedEvents.length === 0) {
        return;
      }

      commandReadModel = yield* projectEventsOntoReadModel(commandReadModel, persistedEvents);

      for (const persistedEvent of persistedEvents) {
        yield* PubSub.publish(eventPubSub, persistedEvent);
      }
    });

    return Effect.exit(
      Effect.gen(function* () {
        processingStartedAtMs = yield* Clock.currentTimeMillis;
        yield* Effect.annotateCurrentSpan({
          "orchestration.command_id": envelope.command.commandId,
          "orchestration.command_type": envelope.command.type,
          "orchestration.aggregate_kind": aggregateRef.aggregateKind,
          "orchestration.aggregate_id": aggregateRef.aggregateId,
        });

        const existingReceipt = yield* commandReceiptRepository.getByCommandId({
          commandId: envelope.command.commandId,
        });
        if (Option.isSome(existingReceipt)) {
          existingReceiptFound = true;
          // A receipt only proves this exact command was handled. Replaying it
          // for a command aimed at another aggregate would report success for
          // work that never happened.
          if (
            existingReceipt.value.aggregateKind !== aggregateRef.aggregateKind ||
            existingReceipt.value.aggregateId !== aggregateRef.aggregateId
          ) {
            return yield* new OrchestrationCommandIdConflictError({
              commandId: envelope.command.commandId,
              receiptAggregateKind: existingReceipt.value.aggregateKind,
              receiptAggregateId: existingReceipt.value.aggregateId,
              commandAggregateKind: aggregateRef.aggregateKind,
              commandAggregateId: aggregateRef.aggregateId,
            });
          }
          if (existingReceipt.value.status === "accepted") {
            if (coordinationFingerprint !== undefined) {
              const receiptEvents = yield* Stream.runCollect(
                eventStore.readFromSequence(existingReceipt.value.resultSequence - 1, 1),
              );
              if (
                Array.from(receiptEvents)[0]?.metadata.coordinationFingerprint !==
                coordinationFingerprint
              )
                return yield* new CoordinationError({
                  code: "conflict",
                  detail: "Coordination command ID was accepted with different inputs.",
                });
            }
            if (workerCommand !== undefined) {
              const receiptEvents = yield* Stream.runCollect(
                eventStore.readFromSequence(existingReceipt.value.resultSequence - 1, 1),
              );
              const receiptEvent = Array.from(receiptEvents)[0];
              const savedCommand = receiptEvent?.metadata.workerCommand;
              if (
                receiptEvent?.sequence !== existingReceipt.value.resultSequence ||
                savedCommand?.type !== workerCommand.type ||
                savedCommand.callerThreadId !== workerCommand.callerThreadId ||
                savedCommand.fingerprint !== workerCommand.fingerprint
              ) {
                return yield* new WorkerOperationError({
                  operation:
                    envelope.command.type === "thread.worker.spawn"
                      ? "spawn"
                      : envelope.command.type === "thread.worker.stop"
                        ? "stop"
                        : "send",
                  code: "conflict",
                  detail:
                    "This command identifier was already accepted for a different worker operation or input.",
                });
              }
            }
            if (envelope.command.type === "thread.worker.spawn") {
              const savedWorker = yield* projectionSnapshotQuery.getWorkerSpawnMetadata(
                envelope.command.threadId,
              );
              if (
                Option.isNone(savedWorker) ||
                savedWorker.value.spawnCommandId !== envelope.command.commandId ||
                savedWorker.value.ownerThreadId !== envelope.command.callerThreadId ||
                savedWorker.value.spawnFingerprint !== envelope.command.spawnFingerprint
              ) {
                return yield* new WorkerOperationError({
                  operation: "spawn",
                  code: "conflict",
                  detail: "This worker spawn command was already accepted with different inputs.",
                });
              }
            }
            return {
              sequence: existingReceipt.value.resultSequence,
            };
          }
          return yield* new OrchestrationCommandPreviouslyRejectedError({
            commandId: envelope.command.commandId,
            detail: existingReceipt.value.error ?? "Previously rejected.",
          });
        }

        if (
          envelope.command.type === "thread.auto-settle" &&
          (yield* eventStore.hasEventAfter({
            aggregateKind: "thread",
            aggregateId: envelope.command.threadId,
            sequenceExclusive: envelope.command.snapshotSequence,
          }))
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} changed before automatic settlement`,
          });
        }

        // The decider compares the lookup inputs. Only recreation needs an
        // event check, since it can reset a thread to the same field values.
        if (
          envelope.command.type === "thread.pull-request.sync" &&
          (yield* eventStore.hasEventAfter({
            aggregateKind: "thread",
            aggregateId: envelope.command.threadId,
            sequenceExclusive: envelope.command.snapshotSequence,
            type: "thread.created",
          }))
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} was recreated before pull request discovery`,
          });
        }

        if (
          envelope.command.type === "thread.auto-settle" &&
          threadBackgroundLiveness.getThreadBackgroundLiveness(envelope.command.threadId) !== null
        ) {
          return yield* new OrchestrationCommandInvariantError({
            commandType: envelope.command.type,
            detail: `thread ${envelope.command.threadId} has live background work`,
          });
        }

        // New and moved projects do not carry a resolved identity in the event-derived
        // command model. Legacy PR edits need it to identify the link they replace.
        if (
          envelope.command.type === "thread.meta.update" &&
          envelope.command.linkedPullRequest !== undefined
        ) {
          const threadId = envelope.command.threadId;
          const thread = commandReadModel.threads.find((thread) => thread.id === threadId);
          if (thread !== undefined) {
            const project = yield* projectionSnapshotQuery.getProjectShellById(thread.projectId);
            if (Option.isSome(project)) {
              commandReadModel = {
                ...commandReadModel,
                projects: commandReadModel.projects.map((entry) =>
                  entry.id === thread.projectId
                    ? { ...entry, repositoryIdentity: project.value.repositoryIdentity }
                    : entry,
                ),
              };
            }
          }
        }

        // Command snapshots omit activities at startup and cap them while running.
        // Read this request's durable state before deciding how to send the answer.
        const userInputActivity =
          envelope.command.type === "thread.user-input.respond" ||
          envelope.command.type === "thread.user-input.dismiss"
            ? yield* projectionSnapshotQuery.getUserInputActivity(envelope.command)
            : Option.none();
        const currentCommand = envelope.command;
        if (
          currentCommand.type === "thread.session.set" &&
          (currentCommand.expectedActivationSequence !== undefined ||
            currentCommand.expectedMessageId !== undefined)
        ) {
          const activations = yield* sql<{
            message_id: string;
            event_sequence: number;
          }>`SELECT message_id, event_sequence FROM projection_thread_activation_authorities WHERE thread_id = ${currentCommand.threadId}`;
          const activation = activations[0];
          if (
            !activation ||
            (currentCommand.expectedMessageId !== undefined &&
              activation.message_id !== currentCommand.expectedMessageId) ||
            (currentCommand.expectedActivationSequence !== undefined &&
              activation.event_sequence > currentCommand.expectedActivationSequence)
          ) {
            return yield* new CoordinationError({
              code: "conflict",
              detail: "Cancellation acknowledgement targets a superseded activation.",
            });
          }
        }
        if (
          currentCommand.type === "thread.worker.send" ||
          currentCommand.type === "thread.turn.start"
        ) {
          const reservations = yield* sql<{
            plan_id: string;
          }>`SELECT p.plan_id FROM projection_coordination_plans p,
            json_each(p.document_json, '$.nodes') n, json_each(n.value, '$.attempts') a
            WHERE json_extract(a.value, '$.workerThreadId') = ${currentCommand.threadId}
              AND json_extract(a.value, '$.status') = 'accepted' LIMIT 1`;
          if (reservations.length > 0)
            return yield* new CoordinationError({
              code: "busy",
              detail:
                "Worker is reserved by an accepted coordination assignment. Settle or cancel that assignment first.",
            });
        }
        if (
          (currentCommand.type === "thread.turn.start" ||
            currentCommand.type === "thread.worker.spawn" ||
            currentCommand.type === "thread.worker.send") &&
          currentCommand.unattendedAuthority !== undefined
        ) {
          const authority = currentCommand.unattendedAuthority;
          const rows = yield* sql<{
            owner_thread_id: string;
            project_id: string;
            revision: number;
            revoked: number;
            ceiling_json: string;
          }>`SELECT owner_thread_id, project_id, revision, revoked, ceiling_json FROM unattended_grants WHERE grant_id = ${authority.grantId}`;
          const grant = rows[0];
          const owner = yield* projectionSnapshotQuery.getWorkerState(authority.ownerThreadId);
          const target = yield* projectionSnapshotQuery.getWorkerState(
            currentCommand.type === "thread.worker.spawn"
              ? currentCommand.callerThreadId
              : currentCommand.threadId,
          );
          if (
            !grant ||
            grant.revoked !== 0 ||
            grant.revision !== authority.grantRevision ||
            grant.owner_thread_id !== authority.ownerThreadId ||
            Option.isNone(owner) ||
            Option.isNone(target) ||
            owner.value.thread.worker ||
            grant.project_id !== owner.value.thread.projectId ||
            target.value.thread.projectId !== grant.project_id ||
            (target.value.thread.id !== authority.ownerThreadId &&
              target.value.thread.worker?.rootThreadId !== authority.ownerThreadId)
          ) {
            return yield* new CoordinationError({
              code: "forbidden",
              detail:
                "Unattended activation grant is revoked, stale, or outside its ownership scope.",
            });
          }
          const ceiling = yield* decodeGrantCeiling(grant.ceiling_json).pipe(
            Effect.mapError(
              () =>
                new CoordinationError({
                  code: "invalid",
                  detail: "Stored grant ceiling is invalid.",
                }),
            ),
          );
          const modeAllowed = isWorkerRuntimeModeAllowed;
          if (
            !Array.isArray(ceiling.mcpCapabilities) ||
            !modeAllowed(authority.runtimeModeCeiling, ceiling.runtimeMode) ||
            !modeAllowed(authority.runtimeModeCeiling, owner.value.thread.runtimeMode) ||
            !modeAllowed(
              currentCommand.type === "thread.turn.start"
                ? currentCommand.runtimeMode
                : (currentCommand.runtimeModeCeiling ?? target.value.thread.runtimeMode),
              authority.runtimeModeCeiling,
            ) ||
            authority.mcpCapabilityCeiling.some(
              (capability) => !ceiling.mcpCapabilities.includes(capability),
            )
          ) {
            return yield* new CoordinationError({
              code: "forbidden",
              detail: "Unattended activation exceeds its current grant ceiling.",
            });
          }
        }
        const coordinationMailbox =
          currentCommand.type === "coordination.mailbox.write"
            ? yield* Effect.gen(function* () {
                const rows = yield* sql<{
                  document_json: string;
                }>`SELECT document_json FROM projection_coordination_mailboxes WHERE root_thread_id = ${currentCommand.threadId}`;
                return rows[0]
                  ? yield* decodeCoordinationMailbox(JSON.parse(rows[0].document_json)).pipe(
                      Effect.mapError(
                        () =>
                          new CoordinationError({
                            code: "invalid",
                            detail: "Stored mailbox is invalid.",
                          }),
                      ),
                    )
                  : null;
              })
            : undefined;
        if (
          (currentCommand.type === "thread.turn.start" && currentCommand.expectedIdle) ||
          ((currentCommand.type === "thread.turn.interrupt" ||
            currentCommand.type === "thread.session.stop") &&
            (currentCommand.expectedMessageId !== undefined ||
              currentCommand.expectedTurnId !== undefined))
        ) {
          const guarded = yield* projectionSnapshotQuery.getWorkerState(currentCommand.threadId);
          if (Option.isNone(guarded))
            return yield* new CoordinationError({
              code: "notFound",
              detail: "Guarded target is unavailable.",
            });
          const { thread, pendingMessageId } = guarded.value;
          if (currentCommand.type === "thread.turn.start") {
            const busy =
              pendingMessageId !== null ||
              thread.latestTurn?.state === "running" ||
              thread.session?.status === "starting" ||
              thread.session?.activeTurnId != null ||
              thread.hasPendingApprovals ||
              thread.hasPendingUserInput ||
              thread.backgroundLiveness != null ||
              threadBackgroundLiveness.getThreadBackgroundLiveness(thread.id) !== null;
            if (busy)
              return yield* new CoordinationError({
                code: "busy",
                detail: "Guarded start requires an idle target.",
              });
          } else {
            const currentRequests =
              currentCommand.expectedMessageId === undefined
                ? []
                : yield* sql<{
                    pending_message_id: string;
                  }>`SELECT pending_message_id FROM projection_turns WHERE thread_id = ${thread.id} AND turn_id = ${thread.latestTurn?.turnId ?? ""} LIMIT 1`;
            // A queued activation supersedes the terminal turn's message identity.
            const messageMatches =
              currentCommand.expectedMessageId === undefined ||
              (pendingMessageId !== null
                ? pendingMessageId === currentCommand.expectedMessageId
                : currentRequests[0]?.pending_message_id === currentCommand.expectedMessageId);
            const turnMatches =
              currentCommand.expectedTurnId === undefined ||
              thread.latestTurn?.turnId === currentCommand.expectedTurnId;
            if (!messageMatches || !turnMatches)
              return yield* new CoordinationError({
                code: "conflict",
                detail: "Guarded cancellation no longer targets the current assignment.",
              });
          }
        }
        const coordinationPlan =
          currentCommand.type === "coordination.plan.write" ||
          currentCommand.type === "coordination.plan.dispatch" ||
          currentCommand.type === "coordination.plan.abandon" ||
          currentCommand.type === "coordination.plan.recover" ||
          currentCommand.type === "coordination.plan.settle"
            ? yield* Effect.gen(function* () {
                const id =
                  currentCommand.type === "coordination.plan.write"
                    ? currentCommand.input.planId
                    : currentCommand.planId;
                const rows = yield* sql<{
                  document_json: string;
                }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id = ${id}`;
                return rows[0]
                  ? yield* decodeCoordinationPlan(JSON.parse(rows[0].document_json)).pipe(
                      Effect.mapError(
                        () =>
                          new CoordinationError({
                            code: "invalid",
                            detail: "Stored plan is invalid.",
                          }),
                      ),
                    )
                  : null;
              })
            : undefined;
        let coordinationExecutionAuthority:
          | import("@t3tools/contracts").ThreadUnattendedAuthority
          | undefined;
        let coordinationExecutionDenied = false;
        if (
          currentCommand.type === "coordination.plan.write" ||
          currentCommand.type === "coordination.plan.dispatch" ||
          currentCommand.type === "coordination.plan.settle" ||
          currentCommand.type === "coordination.plan.abandon" ||
          currentCommand.type === "coordination.plan.recover"
        ) {
          const stored = coordinationPlan?.executionAuthority ?? undefined;
          const capture =
            currentCommand.type === "coordination.plan.write" &&
            ["create", "run", "retry", "repair", "salvage"].includes(
              currentCommand.input.operation,
            );
          const supplied =
            currentCommand.type === "coordination.plan.write"
              ? currentCommand.executionAuthority
              : undefined;
          const current = capture
            ? yield* projectionSnapshotQuery.getThreadActivationAuthority(currentCommand.threadId)
            : Option.none();
          const incoming = supplied ?? Option.getOrUndefined(current);
          coordinationExecutionAuthority = stored ?? (capture ? incoming : undefined);
          const launchingWrite =
            currentCommand.type === "coordination.plan.write" &&
            ["create", "run", "retry", "repair", "salvage"].includes(
              currentCommand.input.operation,
            );
          if (
            stored &&
            launchingWrite &&
            incoming &&
            JSON.stringify(stored) !== JSON.stringify(incoming)
          )
            return yield* new CoordinationError({
              code: "forbidden",
              detail: "A later activation cannot replace the immutable plan execution grant.",
            });
          const handoffNode =
            currentCommand.type === "coordination.plan.settle"
              ? coordinationPlan?.nodes.find((node) => node.id === currentCommand.nodeId)
              : undefined;
          const handoffAttempt = handoffNode?.attempts.at(-1);
          const handoff =
            currentCommand.type === "coordination.plan.settle" &&
            coordinationPlan?.policy.mode === "deep" &&
            !coordinationPlan.cancelled &&
            currentCommand.outcome === "completed" &&
            handoffAttempt &&
            !handoffAttempt.pendingArtifact &&
            !handoffAttempt.handoffRequested;
          if (handoff && handoffAttempt) {
            const workerAuthority = yield* projectionSnapshotQuery.getThreadActivationAuthority(
              handoffAttempt.workerThreadId,
            );
            if (
              JSON.stringify(Option.getOrUndefined(workerAuthority) ?? null) !==
              JSON.stringify(coordinationExecutionAuthority ?? null)
            )
              coordinationExecutionDenied = true;
          }
          if (
            coordinationExecutionAuthority &&
            (launchingWrite || currentCommand.type === "coordination.plan.dispatch" || handoff)
          ) {
            const authority = coordinationExecutionAuthority;
            const authorization = yield* Effect.gen(function* () {
              const rows = yield* sql<{
                owner_thread_id: string;
                project_id: string;
                revision: number;
                revoked: number;
                ceiling_json: string;
              }>`SELECT owner_thread_id, project_id, revision, revoked, ceiling_json FROM unattended_grants WHERE grant_id = ${authority.grantId}`;
              const grant = rows[0];
              const owner = yield* projectionSnapshotQuery.getWorkerState(authority.ownerThreadId);
              const target = yield* projectionSnapshotQuery.getWorkerState(currentCommand.threadId);
              if (
                !grant ||
                grant.revoked !== 0 ||
                grant.revision !== authority.grantRevision ||
                grant.owner_thread_id !== authority.ownerThreadId ||
                authority.ownerThreadId !== currentCommand.threadId ||
                Option.isNone(owner) ||
                Option.isNone(target) ||
                owner.value.thread.worker ||
                grant.project_id !== owner.value.thread.projectId ||
                target.value.thread.projectId !== grant.project_id
              )
                return yield* new CoordinationError({
                  code: "forbidden",
                  detail:
                    "The immutable graph execution grant is stale, revoked, or outside its root.",
                });
              const ceiling = yield* decodeGrantCeiling(grant.ceiling_json).pipe(
                Effect.mapError(
                  () =>
                    new CoordinationError({
                      code: "invalid",
                      detail: "Stored graph grant ceiling is invalid.",
                    }),
                ),
              );
              if (
                !isWorkerRuntimeModeAllowed(authority.runtimeModeCeiling, ceiling.runtimeMode) ||
                !isWorkerRuntimeModeAllowed(
                  authority.runtimeModeCeiling,
                  owner.value.thread.runtimeMode,
                ) ||
                !authority.mcpCapabilityCeiling.includes("workers") ||
                authority.mcpCapabilityCeiling.some(
                  (capability) => !ceiling.mcpCapabilities.includes(capability),
                )
              )
                return yield* new CoordinationError({
                  code: "forbidden",
                  detail: "Graph execution exceeds its immutable grant ceiling.",
                });
            }).pipe(Effect.result);
            if (authorization._tag === "Failure") {
              if (handoff && authorization.failure instanceof CoordinationError)
                coordinationExecutionDenied = true;
              else return yield* Effect.fail(authorization.failure);
            }
          }
        }
        let workerStates =
          currentCommand.type.startsWith("coordination.plan.") ||
          currentCommand.type === "thread.worker.spawn" ||
          currentCommand.type === "thread.worker.send" ||
          currentCommand.type === "thread.delete" ||
          currentCommand.type === "project.delete" ||
          (currentCommand.type === "thread.turn.start" &&
            commandReadModel.threads.some(
              (thread) => thread.id === currentCommand.threadId && thread.worker != null,
            ))
            ? yield* projectionSnapshotQuery.getWorkerAdmissionStates(
                threadBackgroundLiveness.getLiveThreadIds().map((id) => ThreadId.make(id)),
              )
            : undefined;
        let coordinationCancellationTargets: ReadonlyArray<ThreadId> | undefined;
        if (
          coordinationPlan &&
          (currentCommand.type === "coordination.plan.abandon" ||
            currentCommand.type === "coordination.plan.recover" ||
            (currentCommand.type === "coordination.plan.write" &&
              currentCommand.input.operation === "cancel"))
        ) {
          const targets: ThreadId[] = [];
          const assignments = coordinationPlan.nodes.flatMap((node) =>
            node.attempts.at(-1)?.status === "accepted" ? [node.attempts.at(-1)!] : [],
          );
          for (const attempt of assignments) {
            const state = yield* projectionSnapshotQuery.getWorkerState(attempt.workerThreadId);
            if (Option.isNone(state)) continue;
            workerStates = [
              ...(workerStates ?? []).filter((entry) => entry.thread.id !== attempt.workerThreadId),
              state.value,
            ];
            if (state.value.pendingMessageId !== null) {
              if (state.value.pendingMessageId === attempt.dispatchMessageId)
                targets.push(attempt.workerThreadId);
            } else {
              const bindings = yield* sql<{
                pending_message_id: string;
              }>`SELECT pending_message_id FROM projection_turns WHERE thread_id = ${attempt.workerThreadId} AND turn_id = ${state.value.thread.latestTurn?.turnId ?? ""} LIMIT 1`;
              if (bindings[0]?.pending_message_id === attempt.dispatchMessageId)
                targets.push(attempt.workerThreadId);
            }
          }
          coordinationCancellationTargets = targets;
        }
        let coordinationTurnBound = false;
        let coordinationReusableWorker:
          | import("../Services/ProjectionSnapshotQuery.ts").WorkerThreadState
          | undefined;
        if (
          currentCommand.type === "coordination.plan.dispatch" &&
          coordinationPlan?.policy.retainWorkers
        ) {
          const node = coordinationPlan.nodes.find((entry) => entry.id === currentCommand.nodeId);
          const root = commandReadModel.threads.find(
            (entry) => entry.id === currentCommand.threadId,
          );
          if (node && root) {
            // Never steal an idle-looking worker whose accepted graph assignment is still live.
            const candidates = yield* sql<{ thread_id: string }>`
              SELECT t.thread_id FROM projection_threads t
              WHERE t.deleted_at IS NULL AND t.archived_at IS NULL
                AND json_extract(t.worker_json, '$.ownerThreadId') = ${root.id}
                AND json_extract(t.worker_json, '$.rootThreadId') = ${root.id}
                AND json_extract(t.worker_json, '$.stopRequestedAt') IS NULL
                AND NOT EXISTS (
                  SELECT 1 FROM projection_coordination_plans p, json_each(p.document_json, '$.nodes') n,
                    json_each(n.value, '$.attempts') a
                  WHERE json_extract(a.value, '$.workerThreadId') = t.thread_id
                    AND json_extract(a.value, '$.status') = 'accepted'
                )
              ORDER BY t.thread_id LIMIT 16`;
            for (const candidate of candidates) {
              const state = yield* projectionSnapshotQuery.getWorkerState(
                ThreadId.make(candidate.thread_id),
              );
              if (Option.isNone(state)) continue;
              const { thread, pendingMessageId } = state.value;
              const sameSelection =
                thread.modelSelection.instanceId === node.modelSelection.instanceId &&
                thread.modelSelection.model === node.modelSelection.model &&
                JSON.stringify(Object.entries(thread.modelSelection.options ?? {}).sort()) ===
                  JSON.stringify(Object.entries(node.modelSelection.options ?? {}).sort());
              const activationAuthority =
                yield* projectionSnapshotQuery.getThreadActivationAuthority(thread.id);
              if (
                JSON.stringify(Option.getOrUndefined(activationAuthority) ?? null) !==
                JSON.stringify(coordinationExecutionAuthority ?? null)
              )
                continue;
              if (
                !sameSelection ||
                thread.runtimeMode !==
                  (coordinationExecutionAuthority?.runtimeModeCeiling ?? root.runtimeMode) ||
                thread.branch !== root.branch ||
                thread.worktreePath !== root.worktreePath ||
                pendingMessageId !== null ||
                thread.latestTurn?.state === "running" ||
                thread.session?.activeTurnId != null ||
                thread.session?.status === "starting" ||
                thread.hasPendingApprovals ||
                thread.hasPendingUserInput ||
                thread.backgroundLiveness != null ||
                threadBackgroundLiveness.getThreadBackgroundLiveness(thread.id) !== null
              )
                continue;
              coordinationReusableWorker = state.value;
              break;
            }
          }
        }
        if (
          coordinationPlan &&
          ((currentCommand.type === "coordination.plan.write" &&
            currentCommand.input.operation === "complete") ||
            currentCommand.type === "coordination.plan.settle")
        ) {
          const completion =
            currentCommand.type === "coordination.plan.write" &&
            currentCommand.input.operation === "complete"
              ? currentCommand.input
              : undefined;
          const nodeId =
            completion?.nodeId ??
            (currentCommand.type === "coordination.plan.settle" ? currentCommand.nodeId : "");
          const turnId =
            completion?.turnId ??
            (currentCommand.type === "coordination.plan.settle" ? currentCommand.turnId : "");
          const attempt = coordinationPlan.nodes
            .find((node) => node.id === nodeId)
            ?.attempts.at(-1);
          if (attempt) {
            const bindings = yield* sql<{
              turn_id: string;
            }>`SELECT turn_id FROM projection_turns WHERE thread_id = ${attempt.workerThreadId} AND pending_message_id = ${attempt.dispatchMessageId} AND turn_id = ${turnId} LIMIT 1`;
            coordinationTurnBound = bindings.length === 1;
          }
        }
        const eventBase = yield* decideOrchestrationCommand({
          command: envelope.command,
          readModel: commandReadModel,
          ...(coordinationPlan !== undefined ? { coordinationPlan } : {}),
          ...(coordinationMailbox !== undefined ? { coordinationMailbox } : {}),
          coordinationTurnBound,
          ...(coordinationCancellationTargets !== undefined
            ? { coordinationCancellationTargets }
            : {}),
          ...(coordinationReusableWorker !== undefined ? { coordinationReusableWorker } : {}),
          ...(coordinationExecutionAuthority !== undefined
            ? { coordinationExecutionAuthority }
            : {}),
          coordinationExecutionDenied,
          ...(workerStates !== undefined ? { workerStates } : {}),
          ...(Option.isSome(userInputActivity)
            ? { userInputActivity: userInputActivity.value }
            : {}),
        }).pipe(
          Effect.provideService(Crypto.Crypto, crypto),
          Effect.mapError((cause) =>
            isOrchestrationCommandRejection(cause)
              ? cause
              : new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Failed to generate an event identifier.",
                  cause,
                }),
          ),
        );
        const decidedEvents = (Array.isArray(eventBase) ? eventBase : [eventBase]).map((event) =>
          coordinationFingerprint === undefined
            ? event
            : { ...event, metadata: { ...event.metadata, coordinationFingerprint } },
        );
        const plannedEvents =
          workerCommand === undefined
            ? decidedEvents
            : decidedEvents.map((event) => ({
                ...event,
                metadata: { ...event.metadata, workerCommand },
              }));
        // Stamp the dispatching client's origin onto every event the command
        // produced. The decider stays pure; attribution is an engine concern.
        const eventBases =
          envelope.origin === undefined
            ? plannedEvents
            : plannedEvents.map((planned) => ({
                ...planned,
                metadata: { ...planned.metadata, origin: envelope.origin },
              }));
        const committedCommand = yield* sql
          .withTransaction(
            Effect.gen(function* () {
              const committedEvents: OrchestrationEvent[] = [];
              const attachmentCleanups: Effect.Effect<void>[] = [];
              let nextCommandReadModel = commandReadModel;

              for (const nextEvent of eventBases) {
                const savedEvent = yield* eventStore.append(nextEvent);
                nextCommandReadModel = yield* projectEvent(nextCommandReadModel, savedEvent);
                const cleanup = yield* projectionPipeline.projectEventDeferred(savedEvent);
                attachmentCleanups.push(cleanup);
                committedEvents.push(savedEvent);
              }

              const lastSavedEvent = committedEvents.at(-1) ?? null;
              if (lastSavedEvent === null) {
                return yield* new OrchestrationCommandInvariantError({
                  commandType: envelope.command.type,
                  detail: "Command produced no events.",
                });
              }

              yield* commandReceiptRepository.upsert({
                commandId: envelope.command.commandId,
                aggregateKind: lastSavedEvent.aggregateKind,
                aggregateId: lastSavedEvent.aggregateId,
                acceptedAt: lastSavedEvent.occurredAt,
                resultSequence: lastSavedEvent.sequence,
                status: "accepted",
                error: null,
              });

              return {
                committedEvents,
                attachmentCleanups,
                lastSequence: lastSavedEvent.sequence,
                nextCommandReadModel,
              } as const;
            }),
          )
          .pipe(
            Effect.catchTag("SqlError", (sqlError) =>
              Effect.fail(
                toPersistenceSqlError("OrchestrationEngine.processEnvelope:transaction")(sqlError),
              ),
            ),
          );

        commandReadModel = committedCommand.nextCommandReadModel;
        for (const cleanup of committedCommand.attachmentCleanups) {
          yield* cleanup;
        }
        for (const [index, event] of committedCommand.committedEvents.entries()) {
          yield* PubSub.publish(eventPubSub, event);
          if (index === 0) {
            yield* Metric.update(
              Metric.withAttributes(
                orchestrationCommandAckDuration,
                metricAttributes({
                  ...baseMetricAttributes,
                  ackEventType: event.type,
                }),
              ),
              Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - envelope.startedAtMs)),
            );
          }
        }
        return { sequence: committedCommand.lastSequence };
      }).pipe(Effect.withSpan(`orchestration.command.${envelope.command.type}`)),
    ).pipe(
      Effect.flatMap((exit) =>
        Effect.gen(function* () {
          const outcome = Exit.isSuccess(exit)
            ? "success"
            : Cause.hasInterruptsOnly(exit.cause)
              ? "interrupt"
              : "failure";
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandDuration,
              metricAttributes(baseMetricAttributes),
            ),
            Duration.millis(Math.max(0, (yield* Clock.currentTimeMillis) - processingStartedAtMs)),
          );
          yield* Metric.update(
            Metric.withAttributes(
              orchestrationCommandsTotal,
              metricAttributes({
                ...baseMetricAttributes,
                outcome,
              }),
            ),
            1,
          );

          if (Exit.isSuccess(exit)) {
            yield* Deferred.succeed(envelope.result, exit.value);
            return;
          }

          const error = Cause.squash(exit.cause) as OrchestrationDispatchError;
          if (
            !isOrchestrationCommandPreviouslyRejectedError(error) &&
            !isOrchestrationCommandIdConflictError(error) &&
            !existingReceiptFound
          ) {
            yield* reconcileReadModelAfterDispatchFailure.pipe(
              Effect.catch(() =>
                Effect.logWarning(
                  "failed to reconcile orchestration read model after dispatch failure",
                ).pipe(
                  Effect.annotateLogs({
                    commandId: envelope.command.commandId,
                    snapshotSequence: commandReadModel.snapshotSequence,
                  }),
                ),
              ),
            );

            if (
              isOrchestrationCommandRejection(error) &&
              !(error instanceof CoordinationError && error.code === "busy") &&
              !(isWorkerOperationError(error) && (error.code === "busy" || error.code === "limit"))
            ) {
              yield* commandReceiptRepository
                .upsert({
                  commandId: envelope.command.commandId,
                  aggregateKind: aggregateRef.aggregateKind,
                  aggregateId: aggregateRef.aggregateId,
                  acceptedAt: yield* nowIso,
                  resultSequence: commandReadModel.snapshotSequence,
                  status: "rejected",
                  error: error.message,
                })
                .pipe(Effect.ignore);
            }
          }

          yield* Deferred.fail(envelope.result, error);
        }),
      ),
    );
  };

  yield* projectionPipeline.bootstrap;
  commandReadModel = yield* projectionSnapshotQuery.getCommandReadModel();

  const worker = Effect.forever(Queue.take(commandQueue).pipe(Effect.flatMap(processEnvelope)));
  yield* Effect.forkScoped(worker);
  yield* Effect.logDebug("orchestration engine started").pipe(
    Effect.annotateLogs({ sequence: commandReadModel.snapshotSequence }),
  );

  const readEvents: OrchestrationEngineShape["readEvents"] = (fromSequenceExclusive, limit) =>
    eventStore.readFromSequence(fromSequenceExclusive, limit);

  const readThreadEvents: OrchestrationEngineShape["readThreadEvents"] = ({ threadId, ...range }) =>
    eventStore.readAggregateRange({ ...range, aggregateKind: "thread", aggregateId: threadId });

  const getThreadReplayStats: OrchestrationEngineShape["getThreadReplayStats"] = ({
    threadId,
    ...range
  }) =>
    eventStore.getAggregateReplayStats({
      ...range,
      aggregateKind: "thread",
      aggregateId: threadId,
    });

  const dispatch: OrchestrationEngineShape["dispatch"] = (command, options) =>
    Effect.gen(function* () {
      const result = yield* Deferred.make<{ sequence: number }, OrchestrationDispatchError>();
      yield* Queue.offer(commandQueue, {
        command,
        origin: options?.origin,
        result,
        startedAtMs: yield* Clock.currentTimeMillis,
      });
      return yield* Deferred.await(result);
    });

  return {
    readEvents,
    readThreadEvents,
    getThreadReplayStats,
    dispatch,
    subscribeDomainEvents: PubSub.subscribe(eventPubSub).pipe(Effect.map(Stream.fromSubscription)),
    // Each access creates a fresh PubSub subscription so that multiple
    // consumers (wsServer, ProviderRuntimeIngestion, CheckpointReactor, etc.)
    // each independently receive all domain events.
    get streamDomainEvents(): OrchestrationEngineShape["streamDomainEvents"] {
      return Stream.fromPubSub(eventPubSub);
    },
    // The command read model's snapshotSequence tracks the latest committed
    // event sequence (updated on the worker fiber). A plain property read is a
    // consistent, committed value — reassignment of `commandReadModel` is
    // atomic on the single-threaded event loop.
    latestSequence: Effect.sync(() => commandReadModel.snapshotSequence),
  } satisfies OrchestrationEngineShape;
});

export const OrchestrationEngineLive = Layer.effect(
  OrchestrationEngineService,
  makeOrchestrationEngine,
);
