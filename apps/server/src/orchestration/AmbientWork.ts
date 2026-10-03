import { type ThreadId } from "@t3tools/contracts";
import {
  AmbientWorkConfigureInput,
  AmbientWorkRecord,
  ScheduledWorkError,
} from "../../../../packages/contracts/src/scheduledWork.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Option from "effect/Option";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import * as ScheduledWork from "./ScheduledWork.ts";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";
import * as OrchestrationEngine from "./Services/OrchestrationEngine.ts";

export class AmbientWork extends Context.Service<
  AmbientWork,
  {
    readonly configure: (
      input: AmbientWorkConfigureInput,
      approval: { source: "client" },
    ) => Effect.Effect<AmbientWorkRecord, ScheduledWorkError>;
    readonly get: (input: {
      callerThreadId: ThreadId;
    }) => Effect.Effect<AmbientWorkRecord | null, ScheduledWorkError>;
    readonly stop: (input: {
      callerThreadId: ThreadId;
    }) => Effect.Effect<AmbientWorkRecord | null, ScheduledWorkError>;
    readonly drain: Effect.Effect<void, ScheduledWorkError>;
    readonly start: Effect.Effect<void, ScheduledWorkError, Scope.Scope>;
  }
>()("t3/orchestration/AmbientWork") {}
const fail = (code: ScheduledWorkError["code"], detail: string) =>
  new ScheduledWorkError({ code, detail });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const policy = yield* BackgroundPolicy.BackgroundPolicy;
  const scheduled = yield* ScheduledWork.ScheduledWork;
  const activation = yield* ScheduledWork.ScheduledWorkActivation;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const lock = yield* Semaphore.make(1);
  const wake = yield* Queue.sliding<void>(1);
  let started = false;
  const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        Schema.is(ScheduledWorkError)(cause)
          ? cause
          : new ScheduledWorkError({
              code: "internal",
              detail: "Ambient work operation failed.",
              cause,
            }),
      ),
    );
  const save = (record: AmbientWorkRecord) =>
    wrap(
      sql`INSERT INTO ambient_work(owner_thread_id,document_json) VALUES(${record.config.callerThreadId},${JSON.stringify(record)}) ON CONFLICT(owner_thread_id) DO UPDATE SET document_json=excluded.document_json`,
    ).pipe(Effect.asVoid);
  const get = (input: { callerThreadId: ThreadId }) =>
    wrap(
      Effect.gen(function* () {
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM ambient_work WHERE owner_thread_id = ${input.callerThreadId}`;
        return rows[0]
          ? yield* Schema.decodeUnknownEffect(AmbientWorkRecord)(JSON.parse(rows[0].document_json))
          : null;
      }),
    );
  const stop = (input: { callerThreadId: ThreadId }) =>
    lock.withPermit(
      Effect.gen(function* () {
        const previous = yield* get(input);
        if (!previous) return null;
        const next = {
          ...previous,
          config: { ...previous.config, enabled: false },
          reason: "Disabled by owner.",
        };
        yield* save(next);
        if (next.activeScheduleId)
          yield* scheduled.cancel({
            callerThreadId: input.callerThreadId,
            id: next.activeScheduleId,
          });
        yield* Queue.offer(wake, undefined);
        return next;
      }),
    );
  const configure = (raw: AmbientWorkConfigureInput, approval: { source: "client" }) =>
    lock.withPermit(
      Effect.gen(function* () {
        if (approval?.source !== "client")
          return yield* fail("forbidden", "Ambient admission requires explicit client consent.");
        const config = yield* Schema.decodeUnknownEffect(AmbientWorkConfigureInput)(raw).pipe(
          Effect.mapError(() => fail("invalid", "Invalid ambient configuration.")),
        );
        yield* Effect.try({
          try: () =>
            new Intl.DateTimeFormat("en-CA", { timeZone: config.timezone }).format(new Date()),
          catch: () => fail("invalid", "Unknown ambient timezone."),
        });
        if (config.allowedHourStart >= config.allowedHourEnd)
          return yield* fail("invalid", "Allowed hours must be one nonempty daytime interval.");
        if (config.enabled) yield* activation.authorize(config.callerThreadId, config.grantId);
        const previous = yield* get(config);
        const next: AmbientWorkRecord = previous
          ? { ...previous, config }
          : {
              config,
              lastInteractionAt: yield* Clock.currentTimeMillis,
              lastCycleAt: null,
              day: "",
              cycles: 0,
              activeScheduleId: null,
              reason: "Waiting for idle admission.",
            };
        yield* save(next);
        if (!config.enabled && next.activeScheduleId)
          yield* scheduled.cancel({
            callerThreadId: config.callerThreadId,
            id: next.activeScheduleId,
          });
        yield* Queue.offer(wake, undefined);
        return next;
      }),
    );
  const drain = lock.withPermit(
    wrap(
      Effect.gen(function* () {
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM ambient_work ORDER BY owner_thread_id`;
        const host = yield* policy.snapshot;
        const now = yield* Clock.currentTimeMillis;
        for (const row of rows) {
          let record = yield* Schema.decodeUnknownEffect(AmbientWorkRecord)(
            JSON.parse(row.document_json),
          );
          const config = record.config;
          if (!config.enabled) continue;
          const date = new Date(now);
          const parts = new Intl.DateTimeFormat("en-CA", {
            timeZone: config.timezone,
            year: "numeric",
            month: "2-digit",
            day: "2-digit",
            hour: "2-digit",
            hourCycle: "h23",
          }).formatToParts(date);
          const value = (type: Intl.DateTimeFormatPartTypes) =>
            parts.find((part) => part.type === type)?.value ?? "";
          const day = `${value("year")}-${value("month")}-${value("day")}`;
          if (record.day !== day) record = { ...record, day, cycles: 0 };
          if (host.activeForegroundLeaseCount > 0) record = { ...record, lastInteractionAt: now };
          let reason: string | null = null;
          if (record.activeScheduleId) {
            const previous = yield* scheduled.get({
              callerThreadId: config.callerThreadId,
              id: record.activeScheduleId,
            });
            if (!["completed", "failed", "interrupted", "cancelled"].includes(previous.state))
              reason = "An ambient cycle is already active.";
            else record = { ...record, activeScheduleId: null };
          }
          if (!reason && host.activeForegroundLeaseCount > 0)
            reason = "Foreground interaction is active.";
          if (
            !reason &&
            (host.hostPower.stale ||
              host.hostPower.source === "unknown" ||
              host.hostPower.idle === "unknown")
          )
            reason = "Host idle telemetry is unavailable.";
          if (
            !reason &&
            (host.hostPower.suspended ||
              host.hostPower.lowPowerMode === "true" ||
              ["serious", "critical"].includes(host.hostPower.thermalState))
          )
            reason = "Host power state does not permit ambient work.";
          if (
            !reason &&
            (host.hostPower.idle !== "true" ||
              (host.hostPower.idleSeconds ?? 0) * 1000 < config.idleDelayMs ||
              now - record.lastInteractionAt < config.idleDelayMs)
          )
            reason = "Waiting for idle delay.";
          if (!reason && !config.allowUnknownQuota)
            reason =
              "Provider quota telemetry is unavailable. Explicitly allow unknown quota to proceed.";
          const hour = Number(value("hour"));
          if (!reason && (hour < config.allowedHourStart || hour >= config.allowedHourEnd))
            reason = "Outside configured hours.";
          if (!reason && record.cycles >= config.maxCyclesPerDay)
            reason = "Daily cycle limit reached.";
          if (
            !reason &&
            record.lastCycleAt !== null &&
            now - record.lastCycleAt < config.minimumCycleMs
          )
            reason = "Waiting for cycle interval.";
          const owner = yield* snapshots.getWorkerState(config.callerThreadId);
          if (!reason && Option.isNone(owner)) reason = "Ambient owner was removed.";
          if (!reason && Option.isSome(owner)) {
            const shell = yield* snapshots.getShellSnapshot();
            if (
              shell.threads.some(
                (thread) =>
                  thread.projectId === owner.value.thread.projectId &&
                  (thread.latestTurn?.state === "running" ||
                    thread.session?.status === "starting" ||
                    thread.session?.activeTurnId != null ||
                    thread.hasPendingApprovals ||
                    thread.hasPendingUserInput ||
                    thread.backgroundLiveness != null),
              )
            )
              reason = "Project execution or user input is active.";
          }
          const grant = yield* activation
            .authorize(config.callerThreadId, config.grantId)
            .pipe(Effect.result);
          if (!reason && grant._tag === "Failure") reason = grant.failure.detail;
          if (reason) {
            yield* save({ ...record, reason });
            continue;
          }
          const id = `ambient:${config.callerThreadId}:${day}:${record.cycles + 1}`;
          // Deterministic cycle IDs close crash-after-create-before-cadence-save window.
          yield* scheduled.create({
            id,
            callerThreadId: config.callerThreadId,
            target: { type: "ambient", threadId: config.callerThreadId },
            prompt: config.prompt,
            delayMs: 0,
            onBusy: "wait",
            grantId: config.grantId,
          });
          yield* save({
            ...record,
            activeScheduleId: id,
            lastCycleAt: now,
            cycles: record.cycles + 1,
            reason: null,
          });
        }
      }),
    ),
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
    const changes = yield* policy.subscribe;
    yield* events.pipe(
      Stream.runForEach(() => Queue.offer(wake, undefined)),
      Effect.forkScoped,
    );
    yield* changes.changes.pipe(
      Stream.runForEach(() => Queue.offer(wake, undefined)),
      Effect.forkScoped,
    );
    yield* drain;
    yield* Effect.gen(function* () {
      while (true) {
        yield* Effect.raceFirst(Queue.take(wake), Effect.sleep(60_000));
        yield* drain.pipe(Effect.catch((error) => Effect.logError(error)));
      }
    }).pipe(Effect.forkScoped);
  });
  return AmbientWork.of({ configure, get, stop, drain, start });
});
export const layer = Layer.effect(AmbientWork, make);
