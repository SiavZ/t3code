import {
  CommandId,
  CoordinationError,
  WorkerOperationError,
  type ThreadId,
} from "@t3tools/contracts";
import {
  ScheduledWorkError,
  type ScheduledWorkRecord,
} from "../../../../packages/contracts/src/scheduledWork.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "./Services/OrchestrationEngine.ts";
import * as Receipts from "../persistence/Services/OrchestrationCommandReceipts.ts";
import * as OwnedWorkers from "./OwnedWorkers.ts";
import * as UnattendedGrants from "./UnattendedGrants.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { ScheduledWorkActivation } from "./ScheduledWork.ts";

const fail = (code: ScheduledWorkError["code"], detail: string) =>
  new ScheduledWorkError({ code, detail });
const make = Effect.gen(function* () {
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const sql = yield* SqlClient.SqlClient;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const receipts = yield* Receipts.OrchestrationCommandReceiptRepository;
  const grants = yield* UnattendedGrants.UnattendedGrants;
  const workers = yield* OwnedWorkers.OwnedWorkers;
  const capabilities = yield* McpInvocationContext.makeThreadMcpCapabilities;
  const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        Schema.is(ScheduledWorkError)(cause)
          ? cause
          : Schema.is(WorkerOperationError)(cause) &&
              (cause.code === "busy" || cause.code === "limit")
            ? fail("busy", cause.detail)
            : Schema.is(CoordinationError)(cause) && cause.code === "busy"
              ? fail("busy", cause.detail)
              : new ScheduledWorkError({
                  code: "internal",
                  detail: "Scheduled activation failed.",
                  cause,
                }),
      ),
    );
  const read = (id: ThreadId) =>
    wrap(
      Effect.gen(function* () {
        const state = yield* snapshots.getWorkerState(id);
        if (Option.isNone(state))
          return yield* fail("not-found", "Scheduled target thread is unavailable.");
        const project = yield* snapshots.getProjectShellById(state.value.thread.projectId);
        if (Option.isNone(project))
          return yield* fail("not-found", "Scheduled project is unavailable.");
        return state.value;
      }),
    );
  const authorize = (caller: ThreadId, grantId: string) =>
    wrap(
      Effect.gen(function* () {
        const grant = yield* grants.get({ callerThreadId: caller, id: grantId });
        if (grant.revoked)
          return yield* fail("forbidden", "Explicit unattended grant was revoked.");
        const state = yield* read(caller);
        if (
          grant.projectId !== state.thread.projectId ||
          state.thread.worker?.stopRequestedAt != null
        )
          return yield* fail("forbidden", "Grant owner is no longer eligible.");
        const current = {
          runtimeMode: state.thread.runtimeMode,
          mcpCapabilities: [...((yield* capabilities(caller)) ?? [])],
        };
        return {
          projectId: grant.projectId,
          grantRevision: grant.revision,
          ceiling: UnattendedGrants.intersectCeiling(grant.ceiling, current),
        };
      }),
    );
  const validate = (record: ScheduledWorkRecord) =>
    wrap(
      Effect.gen(function* () {
        const authority = yield* authorize(record.ownerThreadId, record.grantId);
        if (
          authority.grantRevision !== record.grantRevision ||
          authority.projectId !== record.projectId
        )
          return yield* fail("forbidden", "Scheduled grant revision changed.");
        if (record.prompt.trimStart().startsWith("/"))
          return yield* fail("invalid", "Provider-native commands cannot run unattended.");
        if (record.target.type === "ambient") {
          const ambient = yield* sql<{
            document_json: string;
          }>`SELECT document_json FROM ambient_work WHERE owner_thread_id = ${record.ownerThreadId}`;
          const configured: unknown = ambient[0] ? JSON.parse(ambient[0].document_json) : null;
          const enabled = Schema.decodeUnknownOption(
            Schema.Struct({
              config: Schema.Struct({ enabled: Schema.Boolean, grantId: Schema.String }),
            }),
          )(configured);
          if (
            Option.isNone(enabled) ||
            !enabled.value.config.enabled ||
            enabled.value.config.grantId !== record.grantId
          )
            return yield* fail(
              "forbidden",
              "Ambient work is disabled or lacks its configured grant.",
            );
        }
        if (record.target.type === "spawn") {
          if (!authority.ceiling.mcpCapabilities.includes("workers"))
            return yield* fail("forbidden", "Worker spawning is outside the current grant.");
          return;
        }
        let current = yield* read(record.target.threadId);
        if (current.thread.projectId !== record.projectId)
          return yield* fail("forbidden", "Target is outside the grant project.");
        const seen = new Set<ThreadId>();
        while (current.thread.id !== record.ownerThreadId) {
          if (!current.thread.worker || seen.has(current.thread.id) || seen.size >= 2)
            return yield* fail("forbidden", "Scheduled target is not an owned descendant.");
          seen.add(current.thread.id);
          current = yield* read(current.thread.worker.ownerThreadId);
        }
      }),
    );
  const dispatch = (record: ScheduledWorkRecord) =>
    wrap(
      Effect.gen(function* () {
        yield* validate(record);
        const authority = yield* authorize(record.ownerThreadId, record.grantId);
        const ceiling = UnattendedGrants.intersectCeiling(record.ceiling, authority.ceiling);
        const unattendedAuthority = {
          grantId: record.grantId,
          grantRevision: record.grantRevision,
          ownerThreadId: record.ownerThreadId,
          runtimeModeCeiling: ceiling.runtimeMode,
          mcpCapabilityCeiling: [...ceiling.mcpCapabilities],
        };
        if (record.target.type === "spawn") {
          const result = yield* workers.spawn(
            {
              commandId: record.commandId,
              callerThreadId: record.ownerThreadId,
              label: record.target.label,
              prompt: record.prompt,
              modelSelection: record.target.modelSelection,
            },
            {
              mcpCapabilityCeiling: [...ceiling.mcpCapabilities],
              runtimeModeCeiling: ceiling.runtimeMode,
              unattendedAuthority,
            },
          );
          return { sequence: result.sequence, threadId: result.workerThreadId };
        }
        const state = yield* read(record.target.threadId);
        const receipt = yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: record.commandId,
          threadId: record.target.threadId,
          expectedIdle: true,
          unattendedAuthority: {
            grantId: record.grantId,
            grantRevision: record.grantRevision,
            ownerThreadId: record.ownerThreadId,
            runtimeModeCeiling: ceiling.runtimeMode,
            mcpCapabilityCeiling: [...ceiling.mcpCapabilities],
          },
          message: {
            messageId: record.messageId,
            role: "user",
            text: record.prompt,
            attachments: [],
          },
          runtimeMode: ceiling.runtimeMode,
          interactionMode: state.thread.interactionMode,
          createdAt: record.createdAt,
        });
        return { sequence: receipt.sequence, threadId: record.target.threadId };
      }),
    );
  const reconcile = (record: ScheduledWorkRecord) =>
    wrap(
      Effect.gen(function* () {
        const receipt = yield* receipts.getByCommandId({ commandId: record.commandId });
        if (Option.isNone(receipt)) return null;
        if (receipt.value.status !== "accepted")
          return {
            state: "failed" as const,
            sequence: receipt.value.resultSequence,
            threadId: null,
            reason: receipt.value.error ?? "Activation was rejected.",
          };
        const threadId = receipt.value.aggregateId as ThreadId;
        const loaded = yield* snapshots.getWorkerState(threadId);
        if (Option.isNone(loaded))
          return {
            state: "interrupted" as const,
            sequence: receipt.value.resultSequence,
            threadId,
            reason: "Accepted target was removed.",
          };
        const turns = yield* sql<{
          state: string;
          turn_id: string | null;
        }>`SELECT state,turn_id FROM projection_turns WHERE thread_id = ${threadId} AND pending_message_id = ${record.messageId} ORDER BY row_id DESC LIMIT 1`;
        const turn = turns[0];
        const barriers =
          yield* sql`SELECT message_id FROM projection_turn_cancellations WHERE thread_id = ${threadId} AND message_id = ${record.messageId}`;
        const currentActivations = yield* sql<{
          message_id: string;
        }>`SELECT message_id FROM projection_thread_activation_authorities WHERE thread_id = ${threadId}`;
        const isCurrentActivation = currentActivations[0]?.message_id === record.messageId;
        const nativeLive =
          isCurrentActivation &&
          (loaded.value.thread.session?.status === "starting" ||
            loaded.value.thread.session?.activeTurnId != null ||
            loaded.value.thread.latestTurn?.state === "running" ||
            loaded.value.thread.hasPendingApprovals ||
            loaded.value.thread.hasPendingUserInput ||
            loaded.value.thread.backgroundLiveness != null);
        if (!isCurrentActivation && (!turn || turn.state === "pending" || turn.state === "running"))
          return {
            state: record.cancelRequested ? ("cancelled" as const) : ("interrupted" as const),
            sequence: receipt.value.resultSequence,
            threadId,
            reason: "Activation was superseded by a later accepted turn.",
          };
        if (record.cancelRequested && barriers.length > 0 && !turn && !nativeLive)
          return {
            state: "cancelled" as const,
            sequence: receipt.value.resultSequence,
            threadId,
            reason: null,
          };
        if (
          nativeLive &&
          ((turn && ["completed", "error", "interrupted"].includes(turn.state)) ||
            ["error", "interrupted", "stopped"].includes(loaded.value.thread.session?.status ?? ""))
        )
          return {
            state: record.cancelRequested ? ("stopping" as const) : ("running" as const),
            sequence: receipt.value.resultSequence,
            threadId,
            reason: null,
          };
        const state: ScheduledWorkRecord["state"] =
          turn?.state === "pending"
            ? record.cancelRequested
              ? "stopping"
              : "accepted"
            : turn?.state === "running"
              ? record.cancelRequested
                ? "stopping"
                : "running"
              : turn?.state === "completed"
                ? nativeLive
                  ? "running"
                  : "completed"
                : turn?.state === "error" || loaded.value.thread.session?.status === "error"
                  ? "failed"
                  : turn?.state === "interrupted" ||
                      loaded.value.thread.session?.status === "interrupted" ||
                      loaded.value.thread.session?.status === "stopped"
                    ? record.cancelRequested
                      ? "cancelled"
                      : "interrupted"
                    : "accepted";
        return {
          state,
          sequence: receipt.value.resultSequence,
          threadId,
          reason: state === "failed" ? "Provider execution failed after acceptance." : null,
        };
      }),
    );
  const cancel = (record: ScheduledWorkRecord) =>
    wrap(
      Effect.gen(function* () {
        const resolved = yield* reconcile(record);
        const threadId = resolved?.threadId ?? record.executionThreadId;
        if (!threadId) return "cancelled" as const;
        if (
          resolved &&
          ["completed", "failed", "interrupted", "cancelled"].includes(resolved.state)
        )
          return "cancelled" as const;
        yield* read(threadId);
        const activationTurns = yield* sql<{
          turn_id: string | null;
        }>`SELECT turn_id FROM projection_turns WHERE thread_id = ${threadId} AND pending_message_id = ${record.messageId} ORDER BY row_id DESC LIMIT 1`;
        // Interrupt and stop are guarded against this exact accepted activation.
        yield* engine.dispatch({
          type: "thread.turn.interrupt",
          commandId: CommandId.make(`scheduled-cancel:${record.id}`),
          threadId,
          expectedMessageId: record.messageId,
          ...(activationTurns[0]?.turn_id
            ? { expectedTurnId: activationTurns[0].turn_id as import("@t3tools/contracts").TurnId }
            : {}),
          createdAt: record.updatedAt,
        });
        return "stopping" as const;
      }),
    );
  return ScheduledWorkActivation.of({ authorize, validate, dispatch, reconcile, cancel });
});
export const layer = Layer.effect(ScheduledWorkActivation, make);
