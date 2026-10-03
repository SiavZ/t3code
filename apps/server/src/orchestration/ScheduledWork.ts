import { CommandId, MessageId, type ProjectId, type ThreadId } from "@t3tools/contracts";
import {
  ScheduledWorkCreateInput,
  ScheduledWorkReadInput,
  ScheduledWorkListInput,
  ScheduledWorkRecord,
  ScheduledWorkError,
  type UnattendedCeiling,
} from "../../../../packages/contracts/src/scheduledWork.ts";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { isWorkerRuntimeModeAllowed } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as OrchestrationEngine from "./Services/OrchestrationEngine.ts";

/** Implemented at the engine boundary, where idle and exact cancellation guards
 * are serialized with foreground commands. Authority is never caller JSON. */
export class ScheduledWorkActivation extends Context.Service<
  ScheduledWorkActivation,
  {
    readonly authorize: (
      caller: ThreadId,
      grantId: string,
    ) => Effect.Effect<
      { projectId: ProjectId; ceiling: UnattendedCeiling; grantRevision: number },
      ScheduledWorkError
    >;
    readonly validate: (record: ScheduledWorkRecord) => Effect.Effect<void, ScheduledWorkError>;
    readonly dispatch: (
      record: ScheduledWorkRecord,
    ) => Effect.Effect<{ sequence: number; threadId: ThreadId }, ScheduledWorkError>;
    readonly reconcile: (record: ScheduledWorkRecord) => Effect.Effect<
      {
        state: ScheduledWorkRecord["state"];
        sequence: number | null;
        threadId: ThreadId | null;
        reason: string | null;
      } | null,
      ScheduledWorkError
    >;
    readonly cancel: (
      record: ScheduledWorkRecord,
    ) => Effect.Effect<"stopping" | "cancelled", ScheduledWorkError>;
  }
>()("t3/orchestration/ScheduledWorkActivation") {}

export class ScheduledWork extends Context.Service<
  ScheduledWork,
  {
    readonly create: (
      input: ScheduledWorkCreateInput,
      authority?: { readonly mcpCapabilityCeiling: UnattendedCeiling["mcpCapabilities"] },
    ) => Effect.Effect<ScheduledWorkRecord, ScheduledWorkError>;
    readonly list: (
      input: ScheduledWorkListInput,
    ) => Effect.Effect<ReadonlyArray<ScheduledWorkRecord>, ScheduledWorkError>;
    readonly get: (
      input: ScheduledWorkReadInput,
    ) => Effect.Effect<ScheduledWorkRecord, ScheduledWorkError>;
    readonly cancel: (
      input: ScheduledWorkReadInput,
    ) => Effect.Effect<ScheduledWorkRecord, ScheduledWorkError>;
    readonly drainDue: Effect.Effect<void, ScheduledWorkError>;
    readonly reconcile: Effect.Effect<void, ScheduledWorkError>;
    readonly start: Effect.Effect<void, ScheduledWorkError, Scope.Scope>;
  }
>()("t3/orchestration/ScheduledWork") {}

const terminal = (state: ScheduledWorkRecord["state"]) =>
  ["completed", "failed", "interrupted", "cancelled"].includes(state);
const failure = (code: ScheduledWorkError["code"], detail: string, cause?: unknown) =>
  new ScheduledWorkError({ code, detail, ...(cause === undefined ? {} : { cause }) });
const decodeScheduledWorkRecord = Schema.decodeUnknownEffect(ScheduledWorkRecord);
const decodeScheduledWorkReadInput = Schema.decodeUnknownEffect(ScheduledWorkReadInput);
const decodeScheduledWorkListInput = Schema.decodeUnknownEffect(ScheduledWorkListInput);
const decodeScheduledWorkCreateInput = Schema.decodeUnknownEffect(ScheduledWorkCreateInput);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const activation = yield* ScheduledWorkActivation;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const lock = yield* Semaphore.make(1);
  const wake = yield* Queue.sliding<void>(1);
  let started = false;
  const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        Schema.is(ScheduledWorkError)(cause)
          ? cause
          : failure("internal", "Scheduled work persistence failed.", cause),
      ),
    );
  const read = (id: string) =>
    wrap(
      Effect.gen(function* () {
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM scheduled_work WHERE id = ${id}`;
        if (!rows[0]) return null;
        return yield* decodeScheduledWorkRecord(JSON.parse(rows[0].document_json));
      }),
    );
  const save = (record: ScheduledWorkRecord) =>
    wrap(
      sql`UPDATE scheduled_work SET state = ${record.state}, due_at = ${record.dueAt}, document_json = ${JSON.stringify(record)} WHERE id = ${record.id}`,
    ).pipe(Effect.asVoid);
  const update = (record: ScheduledWorkRecord, change: Partial<ScheduledWorkRecord>) =>
    Effect.gen(function* () {
      const next = {
        ...record,
        ...change,
        updatedAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      };
      yield* save(next);
      return next;
    });
  const get = (input: ScheduledWorkReadInput) =>
    Effect.gen(function* () {
      const decoded = yield* decodeScheduledWorkReadInput(input).pipe(
        Effect.mapError(() => failure("invalid", "Invalid scheduled work identifier.")),
      );
      const record = yield* read(decoded.id);
      if (!record) return yield* failure("not-found", "Scheduled work not found.");
      if (record.ownerThreadId !== decoded.callerThreadId)
        return yield* failure("forbidden", "Scheduled work belongs to another thread.");
      return record;
    });
  const list = (input: ScheduledWorkListInput) =>
    wrap(
      Effect.gen(function* () {
        const decoded = yield* decodeScheduledWorkListInput(input);
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM scheduled_work WHERE owner_thread_id = ${decoded.callerThreadId} ORDER BY due_at, id LIMIT 200`;
        return yield* Effect.forEach(rows, (row) =>
          decodeScheduledWorkRecord(JSON.parse(row.document_json)),
        );
      }),
    );
  const create = (
    raw: ScheduledWorkCreateInput,
    callerAuthority?: { readonly mcpCapabilityCeiling: UnattendedCeiling["mcpCapabilities"] },
  ) =>
    lock.withPermit(
      Effect.gen(function* () {
        const input = yield* decodeScheduledWorkCreateInput(raw).pipe(
          Effect.mapError(() => failure("invalid", "Invalid bounded schedule input.")),
        );
        if ((input.dueAt === undefined) === (input.delayMs === undefined))
          return yield* failure("invalid", "Specify exactly one dueAt or delayMs.");
        const invocation = yield* Effect.serviceOption(McpInvocationContext.McpInvocationContext);
        const retained = Option.isSome(invocation)
          ? invocation.value.unattendedAuthority
          : undefined;
        if (
          retained &&
          (input.grantId !== retained.grantId || input.callerThreadId !== retained.ownerThreadId)
        )
          return yield* failure(
            "forbidden",
            "Nested schedules must retain their credential-bound unattended grant.",
          );
        const authority = yield* activation.authorize(input.callerThreadId, input.grantId);
        if (retained && retained.grantRevision !== authority.grantRevision)
          return yield* failure(
            "forbidden",
            "Credential-bound unattended grant was revoked or changed.",
          );
        const requestJson = JSON.stringify({
          target: input.target,
          prompt: input.prompt,
          dueAt: input.dueAt ?? null,
          delayMs: input.delayMs ?? null,
          latestStartAt: input.latestStartAt ?? null,
          onBusy: input.onBusy,
          grantId: input.grantId,
        });
        const existing = yield* read(input.id);
        if (existing) {
          const requests = yield* wrap(
            sql<{
              request_json: string;
            }>`SELECT request_json FROM scheduled_work WHERE id = ${input.id}`,
          );
          if (requests[0]?.request_json !== requestJson)
            return yield* failure(
              "conflict",
              "Schedule identifier already has different timing or execution parameters.",
            );
          if (
            existing.ownerThreadId !== input.callerThreadId ||
            existing.prompt !== input.prompt ||
            JSON.stringify(existing.target) !== JSON.stringify(input.target) ||
            existing.grantId !== input.grantId ||
            existing.onBusy !== input.onBusy ||
            (input.dueAt !== undefined && existing.dueAt !== input.dueAt)
          )
            return yield* failure(
              "conflict",
              "Schedule identifier already has a different request.",
            );
          return existing;
        }
        const now = yield* Clock.currentTimeMillis;
        const dueAt = input.dueAt ?? new Date(now + input.delayMs!).toISOString();
        if (input.latestStartAt && Date.parse(input.latestStartAt) < Date.parse(dueAt))
          return yield* failure("invalid", "Latest start precedes due time.");
        const record: ScheduledWorkRecord = {
          id: input.id,
          ownerThreadId: input.callerThreadId,
          projectId: authority.projectId,
          target: input.target,
          prompt: input.prompt,
          dueAt,
          latestStartAt: input.latestStartAt ?? null,
          onBusy: input.onBusy,
          grantId: input.grantId,
          grantRevision: authority.grantRevision,
          ceiling: {
            ...authority.ceiling,
            runtimeMode:
              retained &&
              !isWorkerRuntimeModeAllowed(
                authority.ceiling.runtimeMode,
                retained.runtimeModeCeiling,
              )
                ? retained.runtimeModeCeiling
                : authority.ceiling.runtimeMode,
            mcpCapabilities: authority.ceiling.mcpCapabilities.filter(
              (capability) =>
                (!retained || retained.mcpCapabilityCeiling.includes(capability)) &&
                (callerAuthority === undefined ||
                  callerAuthority.mcpCapabilityCeiling.includes(capability)),
            ),
          },
          commandId: CommandId.make(`scheduled:${input.id}`),
          messageId: MessageId.make(
            input.target.type === "spawn"
              ? `worker-message:scheduled:${input.id}`
              : `scheduled:${input.id}`,
          ),
          state: "queued",
          reason: null,
          cancelRequested: false,
          acceptedSequence: null,
          executionThreadId: null,
          createdAt: new Date(now).toISOString(),
          updatedAt: new Date(now).toISOString(),
        };
        yield* activation.validate(record);
        yield* wrap(
          sql`INSERT INTO scheduled_work (id, owner_thread_id, due_at, state, request_json, document_json) VALUES (${record.id}, ${record.ownerThreadId}, ${record.dueAt}, ${record.state}, ${requestJson}, ${JSON.stringify(record)})`,
        );
        yield* Queue.offer(wake, undefined);
        return record;
      }),
    );
  const reconcileOne = (record: ScheduledWorkRecord) =>
    Effect.gen(function* () {
      const receipt = yield* activation.reconcile(record);
      if (receipt)
        return yield* update(record, {
          state: receipt.state,
          acceptedSequence: receipt.sequence,
          executionThreadId: receipt.threadId,
          reason: receipt.reason,
        });
      return record;
    });
  const allActive = wrap(
    Effect.gen(function* () {
      const rows = yield* sql<{
        document_json: string;
      }>`SELECT document_json FROM scheduled_work WHERE state NOT IN ('completed','failed','interrupted','cancelled') ORDER BY due_at,id`;
      return yield* Effect.forEach(rows, (row) =>
        decodeScheduledWorkRecord(JSON.parse(row.document_json)),
      );
    }),
  );
  const process = (original: ScheduledWorkRecord) =>
    Effect.gen(function* () {
      let record = original;
      if (["dispatching", "accepted", "running", "stopping"].includes(record.state))
        record = yield* reconcileOne(record);
      if (terminal(record.state)) return;
      if (record.cancelRequested) {
        const state =
          record.acceptedSequence !== null ? yield* activation.cancel(record) : "cancelled";
        yield* update(record, { state });
        return;
      }
      if (!["queued", "blocked", "dispatching"].includes(record.state)) return;
      const now = yield* Clock.currentTimeMillis;
      if (Date.parse(record.dueAt) > now) return;
      if (record.latestStartAt && Date.parse(record.latestStartAt) < now) {
        yield* update(record, { state: "failed", reason: "Latest start time expired." });
        return;
      }
      const validated = yield* activation.validate(record).pipe(Effect.result);
      if (validated._tag === "Failure") {
        yield* update(record, { state: "blocked", reason: validated.failure.detail });
        return;
      }
      record = yield* update(record, { state: "dispatching", reason: null });
      // Persisted deterministic IDs recover accepted-before-outbox-update crashes.
      const dispatched = yield* activation.dispatch(record).pipe(Effect.result);
      if (dispatched._tag === "Failure") {
        const error = dispatched.failure;
        yield* update(record, {
          state: error.code === "busy" && record.onBusy === "wait" ? "blocked" : "failed",
          reason: error.detail,
        });
        return;
      }
      yield* update(record, {
        state: "accepted",
        acceptedSequence: dispatched.success.sequence,
        executionThreadId: dispatched.success.threadId,
      });
    });
  const drainDue = lock.withPermit(
    Effect.gen(function* () {
      for (const record of yield* allActive) yield* process(record);
    }),
  );
  const reconcile = lock.withPermit(
    Effect.gen(function* () {
      for (const record of yield* allActive)
        if (["dispatching", "accepted", "running", "stopping"].includes(record.state))
          yield* reconcileOne(record);
    }),
  );
  const cancel = (input: ScheduledWorkReadInput) =>
    lock.withPermit(
      Effect.gen(function* () {
        let record = yield* get(input);
        if (terminal(record.state)) return record;
        record = yield* update(record, { cancelRequested: true });
        // Reconcile before cancel: acceptance can precede the durable outbox update.
        if (record.state === "dispatching") record = yield* reconcileOne(record);
        const state =
          record.acceptedSequence !== null ? yield* activation.cancel(record) : "cancelled";
        const result = yield* update(record, { state });
        yield* Queue.offer(wake, undefined);
        return result;
      }),
    );
  const start = Effect.gen(function* () {
    if (started) return;
    started = true;
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        started = false;
      }),
    );
    const events = yield* engine.subscribeDomainEvents;
    yield* events.pipe(
      Stream.runForEach(() => Queue.offer(wake, undefined)),
      Effect.forkScoped,
    );
    yield* reconcile;
    yield* drainDue;
    yield* Effect.gen(function* () {
      while (true) {
        const rows = yield* wrap(
          sql<{
            due_at: string;
          }>`SELECT due_at FROM scheduled_work WHERE state = 'queued' ORDER BY due_at LIMIT 1`,
        );
        const delay = rows[0]
          ? Math.max(0, Date.parse(rows[0].due_at) - (yield* Clock.currentTimeMillis))
          : null;
        if (delay === null) yield* Queue.take(wake);
        else yield* Effect.raceFirst(Queue.take(wake), Effect.sleep(delay));
        yield* drainDue.pipe(Effect.catch((error) => Effect.logError(error)));
      }
    }).pipe(Effect.forkScoped);
  });
  return ScheduledWork.of({ create, list, get, cancel, drainDue, reconcile, start });
});
export const layer = Layer.effect(ScheduledWork, make);
