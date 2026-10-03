import { CommandId, MessageId, type ThreadId } from "@t3tools/contracts";
import {
  UnattendedGrant,
  UnattendedGrantCreateInput,
  UnattendedGrantReadInput,
} from "../../../../packages/contracts/src/unattendedGrants.ts";
import {
  ScheduledWorkError,
  type UnattendedCeiling,
} from "../../../../packages/contracts/src/scheduledWork.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";

export class UnattendedGrants extends Context.Service<
  UnattendedGrants,
  {
    readonly create: (
      input: UnattendedGrantCreateInput,
      approval: { readonly source: "client" },
    ) => Effect.Effect<UnattendedGrant, ScheduledWorkError>;
    readonly list: (input: {
      callerThreadId: ThreadId;
    }) => Effect.Effect<ReadonlyArray<UnattendedGrant>, ScheduledWorkError>;
    readonly get: (
      input: UnattendedGrantReadInput,
    ) => Effect.Effect<UnattendedGrant, ScheduledWorkError>;
    readonly revoke: (
      input: UnattendedGrantReadInput,
      approval: { readonly source: "client" },
    ) => Effect.Effect<UnattendedGrant, ScheduledWorkError>;
  }
>()("t3/orchestration/UnattendedGrants") {}
const error = (code: ScheduledWorkError["code"], detail: string) =>
  new ScheduledWorkError({ code, detail });
/** Runtime modes have only two current levels. Never turn approval-required into full-access. */
export const intersectCeiling = (
  left: UnattendedCeiling,
  right: UnattendedCeiling,
): UnattendedCeiling => ({
  runtimeMode:
    left.runtimeMode === "full-access" && right.runtimeMode === "full-access"
      ? "full-access"
      : "approval-required",
  mcpCapabilities: left.mcpCapabilities.filter((capability) =>
    right.mcpCapabilities.includes(capability),
  ),
});
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const engine = yield* Effect.serviceOption(OrchestrationEngineService);
  const capabilities = yield* McpInvocationContext.makeThreadMcpCapabilities;
  const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        Schema.is(ScheduledWorkError)(cause)
          ? cause
          : new ScheduledWorkError({ code: "internal", detail: "Grant operation failed.", cause }),
      ),
    );
  type Row = {
    grant_id: string;
    owner_thread_id: string;
    project_id: string;
    revision: number;
    revoked: number;
    ceiling_json: string;
    host_jobs: number;
    created_at: string;
  };
  const decode = (row: Row) =>
    Schema.decodeUnknownEffect(UnattendedGrant)({
      id: row.grant_id,
      ownerThreadId: row.owner_thread_id,
      projectId: row.project_id,
      revision: row.revision,
      revoked: row.revoked === 1,
      ceiling: JSON.parse(row.ceiling_json),
      hostJobs: row.host_jobs === 1,
      createdAt: row.created_at,
    });
  const list = (input: { callerThreadId: ThreadId }) =>
    wrap(
      Effect.gen(function* () {
        const rows =
          yield* sql<Row>`SELECT * FROM unattended_grants WHERE owner_thread_id = ${input.callerThreadId} ORDER BY grant_id LIMIT 100`;
        return yield* Effect.forEach(rows, decode);
      }),
    );
  const get = (input: UnattendedGrantReadInput) =>
    wrap(
      Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(UnattendedGrantReadInput)(input);
        const rows =
          yield* sql<Row>`SELECT * FROM unattended_grants WHERE grant_id = ${decoded.id}`;
        if (!rows[0]) return yield* error("not-found", "Explicit unattended grant not found.");
        const grant = yield* decode(rows[0]);
        if (grant.ownerThreadId !== decoded.callerThreadId)
          return yield* error("forbidden", "Grant belongs to another thread.");
        return grant;
      }),
    );
  const create = (raw: UnattendedGrantCreateInput, approval: { source: "client" }) =>
    wrap(
      Effect.gen(function* () {
        if (approval?.source !== "client")
          return yield* error("forbidden", "Only explicit client consent can mint a grant.");
        const input = yield* Schema.decodeUnknownEffect(UnattendedGrantCreateInput)(raw);
        const state = yield* snapshots.getWorkerState(input.callerThreadId);
        if (Option.isNone(state)) return yield* error("not-found", "Grant owner thread not found.");
        if (state.value.thread.worker)
          return yield* error("forbidden", "Workers cannot grant themselves unattended authority.");
        const current = {
          runtimeMode: state.value.thread.runtimeMode,
          mcpCapabilities: [...((yield* capabilities(input.callerThreadId)) ?? [])],
        };
        const ceiling = intersectCeiling(input.ceiling, current);
        const now = new Date(yield* Clock.currentTimeMillis).toISOString();
        const grant: UnattendedGrant = {
          id: input.id,
          ownerThreadId: input.callerThreadId,
          projectId: state.value.thread.projectId,
          revision: 1,
          revoked: false,
          ceiling,
          hostJobs: input.hostJobs,
          createdAt: now,
        };
        const existing =
          yield* sql<Row>`SELECT * FROM unattended_grants WHERE grant_id = ${input.id}`;
        if (existing[0]) {
          const previous = yield* decode(existing[0]);
          if (
            previous.ownerThreadId !== input.callerThreadId ||
            previous.revoked ||
            JSON.stringify(previous.ceiling) !== JSON.stringify(ceiling) ||
            previous.hostJobs !== input.hostJobs
          )
            return yield* error("conflict", "Grant identifier is already used.");
          return previous;
        }
        yield* sql`INSERT INTO unattended_grants(grant_id,owner_thread_id,project_id,revision,revoked,ceiling_json,host_jobs,created_at) VALUES(${grant.id},${grant.ownerThreadId},${grant.projectId},1,0,${JSON.stringify(ceiling)},${grant.hostJobs ? 1 : 0},${now})`;
        return grant;
      }),
    );
  const revoke = (input: UnattendedGrantReadInput, approval: { source: "client" }) =>
    wrap(
      Effect.gen(function* () {
        if (approval?.source !== "client")
          return yield* error("forbidden", "Only the client can revoke unattended grants.");
        const previous = yield* get(input);
        if (!previous.revoked)
          yield* sql`UPDATE unattended_grants SET revoked = 1, revision = revision + 1 WHERE grant_id = ${previous.id} AND revoked = 0`;
        if (Option.isSome(engine)) {
          const activations = yield* sql<{
            thread_id: string;
            message_id: string;
          }>`SELECT thread_id,message_id FROM projection_thread_activation_authorities WHERE json_extract(authority_json, '$.grantId') = ${previous.id}`;
          const now = new Date(yield* Clock.currentTimeMillis).toISOString();
          for (const activation of activations) {
            yield* engine.value
              .dispatch({
                type: "thread.turn.interrupt",
                commandId: CommandId.make(`grant-revoke:${previous.id}:${activation.message_id}`),
                threadId: activation.thread_id as ThreadId,
                expectedMessageId: MessageId.make(activation.message_id),
                createdAt: now,
              })
              .pipe(Effect.catch(() => Effect.void));
          }
        }
        return yield* get(input);
      }),
    );
  return UnattendedGrants.of({ create, list, get, revoke });
});
export const layer = Layer.effect(UnattendedGrants, make);
