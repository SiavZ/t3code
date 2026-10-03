import type { MessageId, ThreadId, TurnId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export class NativeActivationCancellation extends Context.Service<
  NativeActivationCancellation,
  {
    readonly threadId: ThreadId;
    readonly sequence: number;
    readonly expectedMessageId?: MessageId | undefined;
    readonly expectedTurnId?: TurnId | undefined;
  }
>()("t3/orchestration/NativeActivationCancellation") {}

/** Durable fences survive pending-row removal and reactor queueing across later user sends. */
export const makeActivationCancellationFence = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const cancelled = (threadId: ThreadId, messageId: MessageId) =>
    sql<{ event_sequence: number }>`
      SELECT event_sequence FROM projection_turn_cancellations
      WHERE thread_id = ${threadId} AND message_id = ${messageId}
    `.pipe(Effect.map((rows) => rows.length > 0));
  const authorized = (threadId: ThreadId, messageId: MessageId) =>
    Effect.gen(function* () {
      const schedules = yield* sql<{
        grant_id: string;
        grant_revision: number;
        owner_thread_id: string;
        project_id: string;
        cancel_requested: number;
      }>`
      SELECT json_extract(document_json, '$.grantId') AS grant_id,
        json_extract(document_json, '$.grantRevision') AS grant_revision,
        owner_thread_id, json_extract(document_json, '$.projectId') AS project_id,
        json_extract(document_json, '$.cancelRequested') AS cancel_requested
      FROM scheduled_work WHERE json_extract(document_json, '$.messageId') = ${messageId}
        AND (json_extract(document_json, '$.target.threadId') = ${threadId}
          OR json_extract(document_json, '$.executionThreadId') = ${threadId}
          OR json_extract(document_json, '$.target.type') = 'spawn')
    `;
      if (schedules.length === 0) return true;
      const schedule = schedules[0]!;
      if (schedule.cancel_requested) return false;
      const activations = yield* sql<{
        message_id: string;
      }>`SELECT message_id FROM projection_thread_activation_authorities WHERE thread_id = ${threadId}`;
      if (activations[0]?.message_id !== messageId) return false;
      const grants = yield* sql`
      SELECT grant_id FROM unattended_grants WHERE grant_id = ${schedule.grant_id}
        AND revision = ${schedule.grant_revision} AND revoked = 0
        AND owner_thread_id = ${schedule.owner_thread_id} AND project_id = ${schedule.project_id}
    `;
      return grants.length > 0;
    });
  const canCancel = (input: {
    threadId: ThreadId;
    sequence: number;
    expectedMessageId?: MessageId | undefined;
    expectedTurnId?: TurnId | undefined;
  }) =>
    Effect.gen(function* () {
      if (input.expectedMessageId === undefined && input.expectedTurnId === undefined) return true;
      const later = yield* sql`
      SELECT sequence FROM orchestration_events
      WHERE stream_id = ${input.threadId} AND event_type = 'thread.turn-start-requested'
        AND sequence > ${input.sequence} LIMIT 1
    `;
      if (later.length > 0) return false;
      if (input.expectedMessageId !== undefined) {
        const barrier = yield* sql`
        SELECT event_sequence FROM projection_turn_cancellations
        WHERE thread_id = ${input.threadId} AND message_id = ${input.expectedMessageId}
          AND event_sequence = ${input.sequence}
      `;
        if (barrier.length > 0) return true;
        const activation = yield* sql`
        SELECT row_id FROM projection_turns
        WHERE thread_id = ${input.threadId} AND pending_message_id = ${input.expectedMessageId}
          AND (${input.expectedTurnId ?? null} IS NULL OR turn_id = ${input.expectedTurnId ?? null})
      `;
        return activation.length > 0;
      }
      const activation = yield* sql`
      SELECT row_id FROM projection_turns WHERE thread_id = ${input.threadId}
        AND turn_id = ${input.expectedTurnId ?? null} AND state IN ('pending', 'running')
    `;
      return activation.length > 0;
    });
  return { cancelled, authorized, canCancel };
});
