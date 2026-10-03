import {
  CoordinationError,
  CoordinationPlan,
  type CoordinationReadInput,
  type CoordinationWriteInput,
  type ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as OrchestrationEngine from "./Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";
import { CoordinationPlanStore } from "./CoordinationPlans.ts";
import {
  CoordinationMailbox,
  type CoordinationMailboxReadInput,
  type CoordinationMailboxWriteInput,
} from "@t3tools/contracts";

const decodeMailbox = Schema.decodeUnknownEffect(CoordinationMailbox);
const decodePlan = Schema.decodeUnknownEffect(CoordinationPlan);
const storageFailure = () =>
  new CoordinationError({ code: "invalid", detail: "Unable to read coordination state." });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const query = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const mailboxRead = Effect.fn("CoordinationPlanStore.mailboxRead")(function* (
    input: CoordinationMailboxReadInput,
  ) {
    const root = yield* query
      .getThreadShellById(input.rootThreadId)
      .pipe(Effect.mapError(storageFailure));
    const caller = yield* query
      .getThreadShellById(input.callerThreadId)
      .pipe(Effect.mapError(storageFailure));
    if (Option.isNone(root) || Option.isNone(caller))
      return yield* new CoordinationError({
        code: "notFound",
        detail: "Root or caller is unavailable.",
      });
    if (
      root.value.worker ||
      (caller.value.id !== root.value.id && caller.value.worker?.rootThreadId !== root.value.id)
    )
      return yield* new CoordinationError({
        code: "forbidden",
        detail: "Caller is outside the persisted root lineage.",
      });
    const rows = yield* sql<{
      document_json: string;
    }>`SELECT document_json FROM projection_coordination_mailboxes WHERE root_thread_id = ${input.rootThreadId}`.pipe(
      Effect.mapError(storageFailure),
    );
    const raw = yield* Effect.try({
      try: () =>
        rows[0]
          ? JSON.parse(rows[0].document_json)
          : {
              rootThreadId: input.rootThreadId,
              revision: 0,
              channels: [],
              context: [],
              envelopes: [],
            },
      catch: storageFailure,
    });
    const mailbox = yield* decodeMailbox(raw).pipe(Effect.mapError(storageFailure));
    return {
      ...mailbox,
      envelopes: mailbox.envelopes.filter(
        (envelope) =>
          envelope.recipientThreadId === input.callerThreadId ||
          input.callerThreadId === input.rootThreadId,
      ),
      channels: mailbox.channels.filter(
        (channel) =>
          channel.members.includes(input.callerThreadId) ||
          input.callerThreadId === input.rootThreadId,
      ),
    };
  });
  const read = Effect.fn("CoordinationPlanStore.read")(function* (input: CoordinationReadInput) {
    const root = yield* query
      .getThreadShellById(input.rootThreadId)
      .pipe(Effect.mapError(storageFailure));
    const caller = yield* query
      .getThreadShellById(input.callerThreadId)
      .pipe(Effect.mapError(storageFailure));
    if (Option.isNone(root) || Option.isNone(caller))
      return yield* new CoordinationError({
        code: "notFound",
        detail: "Root or caller is unavailable.",
      });
    if (
      root.value.worker ||
      (caller.value.id !== root.value.id && caller.value.worker?.rootThreadId !== root.value.id)
    )
      return yield* new CoordinationError({
        code: "forbidden",
        detail: "Caller is outside the persisted root lineage.",
      });
    const rows = yield* sql<{
      document_json: string;
    }>`SELECT document_json FROM projection_coordination_plans WHERE plan_id = ${input.planId} AND root_thread_id = ${input.rootThreadId}`.pipe(
      Effect.mapError(storageFailure),
    );
    if (!rows[0])
      return yield* new CoordinationError({ code: "notFound", detail: "Plan not found." });
    const raw = yield* Effect.try({
      try: () => JSON.parse(rows[0]!.document_json),
      catch: storageFailure,
    });
    return yield* decodePlan(raw).pipe(Effect.mapError(storageFailure));
  });
  return CoordinationPlanStore.of({
    mailboxRead,
    mailboxWrite: Effect.fn("CoordinationPlanStore.mailboxWrite")(function* (
      input: CoordinationMailboxWriteInput,
    ) {
      yield* engine
        .dispatch({
          type: "coordination.mailbox.write",
          commandId: input.commandId,
          threadId: input.rootThreadId,
          input,
          createdAt: DateTime.formatIso(yield* DateTime.now),
        })
        .pipe(
          Effect.mapError((cause) =>
            cause instanceof CoordinationError
              ? cause
              : new CoordinationError({ code: "conflict", detail: "Mailbox command rejected." }),
          ),
        );
      return yield* mailboxRead(input);
    }),
    read,
    execute: Effect.fn("CoordinationPlanStore.execute")(function* (
      input: CoordinationWriteInput,
      trustedAuthority?: ThreadUnattendedAuthority,
    ) {
      const createdAt = DateTime.formatIso(yield* DateTime.now);
      yield* engine
        .dispatch({
          type: "coordination.plan.write",
          ...(trustedAuthority !== undefined ? { executionAuthority: trustedAuthority } : {}),
          commandId: input.commandId,
          threadId: input.rootThreadId,
          input,
          createdAt,
        })
        .pipe(
          Effect.mapError((cause) =>
            cause instanceof CoordinationError
              ? cause
              : new CoordinationError({
                  code: "conflict",
                  detail: "Coordination command was rejected.",
                }),
          ),
        );
      return yield* read(input);
    }),
  });
});
export const layer = Layer.effect(CoordinationPlanStore, make);
