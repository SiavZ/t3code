import { CommandId, EventId, type ThreadId } from "@t3tools/contracts";
import { BackgroundJobError } from "../../../../packages/contracts/src/backgroundJobs.ts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as UnattendedGrants from "../orchestration/UnattendedGrants.ts";
import * as ScheduledWork from "../orchestration/ScheduledWork.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { BackgroundJobAuthority } from "./BackgroundJobs.ts";

const make = Effect.gen(function* () {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const paths = yield* WorkspacePaths.WorkspacePaths;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const grants = yield* UnattendedGrants.UnattendedGrants;
  const scheduled = yield* ScheduledWork.ScheduledWork;
  const capabilities = yield* McpInvocationContext.makeThreadMcpCapabilities;
  const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        Schema.is(BackgroundJobError)(cause)
          ? cause
          : new BackgroundJobError({
              code: "internal",
              detail: "Host-job authority operation failed.",
              cause,
            }),
      ),
    );
  const findGrant = (caller: ThreadId) =>
    wrap(
      Effect.gen(function* () {
        const invocation = yield* Effect.serviceOption(McpInvocationContext.McpInvocationContext);
        const persisted = yield* snapshots.getThreadActivationAuthority(caller);
        const retained =
          (Option.isSome(invocation) ? invocation.value.unattendedAuthority : undefined) ??
          Option.getOrUndefined(persisted);
        const available = yield* grants.list({ callerThreadId: caller });
        const grant = available.find(
          (entry) =>
            !entry.revoked &&
            entry.hostJobs &&
            (!retained ||
              (entry.id === retained.grantId &&
                entry.revision === retained.grantRevision &&
                retained.ownerThreadId === caller &&
                retained.mcpCapabilityCeiling.includes("background-jobs"))),
        );
        if (!grant)
          return yield* new BackgroundJobError({
            code: "forbidden",
            detail: "Explicit client host-job consent is required.",
          });
        return grant;
      }),
    );
  const authorize = (caller: ThreadId) =>
    wrap(
      Effect.gen(function* () {
        const state = yield* snapshots.getWorkerState(caller);
        if (Option.isNone(state))
          return yield* new BackgroundJobError({
            code: "not-found",
            detail: "Host-job owner is unavailable.",
          });
        if (state.value.thread.worker?.stopRequestedAt != null)
          return yield* new BackgroundJobError({
            code: "forbidden",
            detail: "Stopped workers cannot execute host jobs.",
          });
        const access = yield* capabilities(caller);
        if (!access?.has("background-jobs"))
          return yield* new BackgroundJobError({
            code: "forbidden",
            detail: "Host-job capability is disabled for this project or worker.",
          });
        const grant = yield* findGrant(caller);
        if (grant.projectId !== state.value.thread.projectId)
          return yield* new BackgroundJobError({
            code: "forbidden",
            detail: "Host-job consent belongs to another project.",
          });
        const project = yield* snapshots.getProjectShellById(state.value.thread.projectId);
        if (Option.isNone(project))
          return yield* new BackgroundJobError({
            code: "not-found",
            detail: "Host-job project is unavailable.",
          });
        const cwd = yield* paths.normalizeWorkspaceRoot(
          state.value.thread.worktreePath ?? project.value.workspaceRoot,
        );
        return { projectId: project.value.id, cwd };
      }),
    );
  return BackgroundJobAuthority.of({
    authorize,
    notify: (record, subscription) =>
      wrap(
        Effect.gen(function* () {
          const key = `job-terminal:${record.id}:${subscription.callerThreadId}`;
          if (subscription.notify)
            yield* engine.dispatch({
              type: "thread.activity.append",
              commandId: CommandId.make(key),
              threadId: subscription.callerThreadId,
              activity: {
                id: EventId.make(key),
                tone: record.state === "completed" ? "info" : "error",
                kind: "background-job.terminal",
                summary: `Background job ${record.id}: ${record.state}`,
                payload: { jobId: record.id, state: record.state, exitCode: record.exitCode },
                turnId: null,
                createdAt: record.updatedAt,
              },
              createdAt: record.updatedAt,
            });
          if (subscription.wake) {
            yield* authorize(subscription.callerThreadId);
            const grant = yield* findGrant(subscription.callerThreadId);
            yield* scheduled.create({
              id: key,
              callerThreadId: subscription.callerThreadId,
              target: { type: "resume", threadId: subscription.callerThreadId },
              prompt: `Background job ${record.id} finished with state ${record.state}. Retrieve its bounded output if needed.`,
              delayMs: 0,
              onBusy: "wait",
              grantId: grant.id,
            });
          }
        }),
      ),
  });
});
export const layer = Layer.effect(BackgroundJobAuthority, make);
