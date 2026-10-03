import type {
  MessageId,
  RuntimeMode,
  ThreadId,
  ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import {
  ThreadUnattendedAuthority as AuthoritySchema,
  isWorkerRuntimeModeAllowed,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import type * as SqlClient from "effect/unstable/sql/SqlClient";

/** Trusted engine-event origin, carried internally across asynchronous setup without wire fields. */
export class NativeUnattendedActivation extends Context.Service<
  NativeUnattendedActivation,
  {
    readonly messageId: MessageId;
    readonly sequence: number;
    readonly authority: ThreadUnattendedAuthority;
  }
>()("t3/orchestration/NativeUnattendedActivation") {}

export const captureNativeUnattendedActivation = (sql: SqlClient.SqlClient, threadId: ThreadId) =>
  Effect.gen(function* () {
    const rows = yield* sql<{
      message_id: string;
      event_sequence: number;
      authority_json: string | null;
    }>`SELECT message_id,event_sequence,authority_json FROM projection_thread_activation_authorities WHERE thread_id = ${threadId}`;
    if (!rows[0]?.authority_json) return undefined;
    const authority = yield* Schema.decodeUnknownEffect(AuthoritySchema)(
      JSON.parse(rows[0].authority_json),
    );
    return {
      messageId: rows[0].message_id as MessageId,
      sequence: rows[0].event_sequence,
      authority,
    };
  });

/** Check immediately before a captured provider adapter call, including recovery and resend. */
export const validateNativeUnattendedAuthority = (
  sql: SqlClient.SqlClient,
  threadId: ThreadId,
  runtimeMode?: RuntimeMode,
  expectedActivation?: NativeUnattendedActivation["Service"],
) =>
  Effect.gen(function* () {
    const authorities = yield* sql<{
      authority_json: string | null;
      message_id: string;
      event_sequence: number;
    }>`
    SELECT authority_json,message_id,event_sequence FROM projection_thread_activation_authorities WHERE thread_id = ${threadId}
  `;
    if (
      expectedActivation &&
      (authorities[0]?.message_id !== expectedActivation.messageId ||
        authorities[0]?.event_sequence !== expectedActivation.sequence ||
        authorities[0]?.authority_json === null)
    )
      return false;
    if (authorities.length === 0 || authorities[0]?.authority_json === null) return true;
    const rows = yield* sql<{
      authority_json: string;
      ceiling_json: string;
      owner_mode: RuntimeMode;
      target_mode: RuntimeMode;
    }>`
    SELECT a.authority_json,g.ceiling_json,owner.runtime_mode AS owner_mode,target.runtime_mode AS target_mode FROM projection_thread_activation_authorities a
    JOIN unattended_grants g ON g.grant_id = json_extract(a.authority_json, '$.grantId')
    JOIN projection_threads owner ON owner.thread_id = g.owner_thread_id
    JOIN projection_threads target ON target.thread_id = a.thread_id
    WHERE a.thread_id = ${threadId} AND g.revoked = 0
      AND g.revision = json_extract(a.authority_json, '$.grantRevision')
      AND g.owner_thread_id = json_extract(a.authority_json, '$.ownerThreadId')
      AND g.project_id = target.project_id AND owner.project_id = target.project_id
      AND owner.deleted_at IS NULL AND target.deleted_at IS NULL
      AND (${runtimeMode ?? null} IS NULL OR ${runtimeMode ?? null} != 'full-access'
        OR (json_extract(a.authority_json, '$.runtimeModeCeiling') = 'full-access'
          AND json_extract(g.ceiling_json, '$.runtimeMode') = 'full-access'
          AND owner.runtime_mode = 'full-access' AND target.runtime_mode = 'full-access'))
      AND NOT EXISTS (
        SELECT 1 FROM json_each(a.authority_json, '$.mcpCapabilityCeiling') cap
        WHERE cap.value NOT IN (SELECT value FROM json_each(g.ceiling_json, '$.mcpCapabilities'))
      )
  `;
    if (rows.length !== 1) return false;
    if (!runtimeMode) return true;
    const authority = yield* Schema.decodeUnknownEffect(AuthoritySchema)(
      JSON.parse(rows[0]!.authority_json),
    );
    const grant = yield* Schema.decodeUnknownEffect(
      Schema.Struct({
        runtimeMode: Schema.Literals([
          "approval-required",
          "auto-accept-edits",
          "auto",
          "full-access",
        ]),
      }),
    )(JSON.parse(rows[0]!.ceiling_json));
    return [
      authority.runtimeModeCeiling,
      grant.runtimeMode,
      rows[0]!.owner_mode,
      rows[0]!.target_mode,
    ].every((ceiling) => isWorkerRuntimeModeAllowed(runtimeMode, ceiling));
  });
