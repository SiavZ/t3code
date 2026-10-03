import { type ProjectId, type ThreadId } from "@t3tools/contracts";
import {
  BackgroundJobStartInput,
  BackgroundJobReadInput,
  BackgroundJobOutputInput,
  BackgroundJobWaitInput,
  BackgroundJobRecord,
  BackgroundJobError,
  type BackgroundJobOutput,
} from "../../../../packages/contracts/src/backgroundJobs.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Clock from "effect/Clock";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Semaphore from "effect/Semaphore";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ChildProcess from "effect/unstable/process/ChildProcess";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";

/** Resolves the authenticated owner's workspace and explicit host-job permission.
 * Notification delivery must deduplicate by job ID and subscriber thread ID. */
export class BackgroundJobAuthority extends Context.Service<
  BackgroundJobAuthority,
  {
    readonly authorize: (
      caller: ThreadId,
    ) => Effect.Effect<{ projectId: ProjectId; cwd: string }, BackgroundJobError>;
    readonly notify: (
      record: BackgroundJobRecord,
      subscription: { callerThreadId: ThreadId; notify: boolean; wake: boolean },
    ) => Effect.Effect<void, BackgroundJobError>;
  }
>()("t3/background/BackgroundJobAuthority") {}
export class BackgroundJobs extends Context.Service<
  BackgroundJobs,
  {
    readonly start: (
      input: BackgroundJobStartInput,
    ) => Effect.Effect<BackgroundJobRecord, BackgroundJobError>;
    readonly list: (input: {
      callerThreadId: ThreadId;
    }) => Effect.Effect<ReadonlyArray<BackgroundJobRecord>, BackgroundJobError>;
    readonly get: (
      input: BackgroundJobReadInput,
    ) => Effect.Effect<BackgroundJobRecord, BackgroundJobError>;
    readonly output: (
      input: BackgroundJobOutputInput,
    ) => Effect.Effect<BackgroundJobOutput, BackgroundJobError>;
    readonly cancel: (
      input: BackgroundJobReadInput,
    ) => Effect.Effect<BackgroundJobRecord, BackgroundJobError>;
    readonly wait: (
      input: BackgroundJobWaitInput,
    ) => Effect.Effect<{ timedOut: boolean; job: BackgroundJobRecord }, BackgroundJobError>;
    readonly subscribe: (
      input: BackgroundJobReadInput & { notify: boolean; wake: boolean },
    ) => Effect.Effect<BackgroundJobRecord, BackgroundJobError>;
    readonly cleanup: (input: BackgroundJobReadInput) => Effect.Effect<void, BackgroundJobError>;
    readonly reconcile: Effect.Effect<void, BackgroundJobError>;
  }
>()("t3/background/BackgroundJobs") {}
const terminal = (state: BackgroundJobRecord["state"]) =>
  ["completed", "failed", "cancelled", "interrupted"].includes(state);
const failure = (code: BackgroundJobError["code"], detail: string, cause?: unknown) =>
  new BackgroundJobError({ code, detail, ...(cause === undefined ? {} : { cause }) });
const decodeBackgroundJobRecord = Schema.decodeUnknownEffect(BackgroundJobRecord);
const decodeBackgroundJobReadInput = Schema.decodeUnknownEffect(BackgroundJobReadInput);
const decodeBackgroundJobStartInput = Schema.decodeUnknownEffect(BackgroundJobStartInput);
const decodeBackgroundJobOutputInput = Schema.decodeUnknownEffect(BackgroundJobOutputInput);
const decodeBackgroundJobWaitInput = Schema.decodeUnknownEffect(BackgroundJobWaitInput);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const authority = yield* BackgroundJobAuthority;
  const scope = yield* Scope.Scope;
  const lock = yield* Semaphore.make(1);
  const changes = yield* PubSub.sliding<string>(64);
  const outputChunks = new Map<string, number>();
  const decoders = new Map<string, InstanceType<typeof TextDecoder>>();
  const progressLines = new Map<string, string>();
  const handles = new Map<string, ChildProcessSpawner.ChildProcessHandle>();
  const wrap = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.mapError((cause) =>
        Schema.is(BackgroundJobError)(cause)
          ? cause
          : failure("internal", "Background job operation failed.", cause),
      ),
    );
  const read = (id: string) =>
    wrap(
      Effect.gen(function* () {
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM background_jobs WHERE id = ${id}`;
        if (!rows[0]) return null;
        return yield* decodeBackgroundJobRecord(JSON.parse(rows[0].document_json));
      }),
    );
  const save = (record: BackgroundJobRecord) =>
    wrap(
      Effect.gen(function* () {
        yield* sql`UPDATE background_jobs SET state = ${record.state}, document_json = ${JSON.stringify(record)} WHERE id = ${record.id}`;
        yield* PubSub.publish(changes, record.id);
      }),
    );
  const update = (record: BackgroundJobRecord, change: Partial<BackgroundJobRecord>) =>
    Effect.gen(function* () {
      const next = {
        ...record,
        ...change,
        updatedAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      };
      yield* save(next);
      return next;
    });
  const get = (raw: BackgroundJobReadInput) =>
    Effect.gen(function* () {
      const input = yield* decodeBackgroundJobReadInput(raw).pipe(
        Effect.mapError(() => failure("invalid", "Invalid job identifier.")),
      );
      const job = yield* read(input.id);
      if (!job) return yield* failure("not-found", "Background job not found.");
      if (job.ownerThreadId !== input.callerThreadId)
        return yield* failure("forbidden", "Job belongs to another thread.");
      return job;
    });
  const deliver = (record: BackgroundJobRecord) =>
    wrap(
      Effect.gen(function* () {
        if (!terminal(record.state)) return;
        const rows = yield* sql<{
          subscriber_thread_id: string;
          notify: number;
          wake: number;
        }>`SELECT subscriber_thread_id, notify, wake FROM background_job_notifications WHERE job_id = ${record.id} AND delivered = 0`;
        for (const row of rows) {
          const delivery = yield* authority
            .notify(record, {
              callerThreadId: Schema.decodeUnknownSync(Schema.String)(
                row.subscriber_thread_id,
              ) as ThreadId,
              notify: row.notify === 1,
              wake: row.wake === 1,
            })
            .pipe(Effect.result);
          if (delivery._tag === "Failure") {
            if (delivery.failure.code === "forbidden" || delivery.failure.code === "not-found") {
              yield* sql`UPDATE background_job_notifications SET delivered = 2, suppressed_reason = ${delivery.failure.detail} WHERE job_id = ${record.id} AND subscriber_thread_id = ${row.subscriber_thread_id}`;
              continue;
            }
            yield* Effect.logWarning("Background notification delivery deferred", {
              jobId: record.id,
              error: delivery.failure.detail,
            });
            continue;
          }
          yield* sql`UPDATE background_job_notifications SET delivered = 1 WHERE job_id = ${record.id} AND subscriber_thread_id = ${row.subscriber_thread_id}`;
        }
      }),
    );
  const append = (id: string, stream: "stdout" | "stderr", bytes: Uint8Array) =>
    lock.withPermit(
      wrap(
        Effect.gen(function* () {
          const job = yield* read(id);
          if (!job) return;
          const chunkCount = outputChunks.get(id) ?? 0;
          const remaining =
            chunkCount >= 4096 ? 0 : Math.max(0, job.maxOutputBytes - job.outputBytes);
          const decoderKey = `${id}:${stream}`;
          const decoder = decoders.get(decoderKey) ?? new TextDecoder();
          decoders.set(decoderKey, decoder);
          const decoded = decoder.decode(bytes, { stream: true });
          let text = decoded;
          if (new TextEncoder().encode(text).byteLength > remaining) {
            let low = 0;
            let high = text.length;
            while (low < high) {
              const middle = Math.ceil((low + high) / 2);
              if (new TextEncoder().encode(text.slice(0, middle)).byteLength <= remaining)
                low = middle;
              else high = middle - 1;
            }
            text = text.slice(0, low);
            if (text.length > 0 && /[\uD800-\uDBFF]$/.test(text)) text = text.slice(0, -1);
          }
          // Output remains ordinary text. A bounded, valid line may additionally update progress.
          let progress = job.progress;
          if (stream === "stdout") {
            const pending = (progressLines.get(id) ?? "") + decoded;
            const lines = pending.split("\n");
            const unfinished = lines.pop() ?? "";
            progressLines.set(id, unfinished.length <= 8192 ? unfinished : "");
            for (const line of lines) {
              if (line.length > 8192 || !line.startsWith("T3_PROGRESS ")) continue;
              try {
                const parsed: unknown = JSON.parse(line.slice(12));
                progress = Schema.decodeUnknownSync(BackgroundJobRecord.fields.progress)(parsed);
              } catch {
                /* Malformed progress is retained as ordinary stdout. */
              }
            }
          }
          const size = new TextEncoder().encode(text).byteLength;
          if (size > 0) {
            yield* sql`INSERT INTO background_job_output (job_id,cursor,stream,text) VALUES (${id},${job.outputBytes},${stream},${text})`;
            outputChunks.set(id, chunkCount + 1);
          }
          yield* update(job, {
            outputBytes: job.outputBytes + size,
            truncated: job.truncated || bytes.byteLength > remaining,
            progress,
          });
        }),
      ),
    );
  const run = (input: BackgroundJobStartInput, job: BackgroundJobRecord) =>
    Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* lock.withPermit(
          Effect.gen(function* () {
            const latest = yield* read(job.id);
            if (!latest || terminal(latest.state) || latest.state === "cancelling") return null;
            yield* authority.authorize(job.ownerThreadId);
            const captured = yield* wrap(
              spawner.spawn(
                ChildProcess.make(input.command, input.args, {
                  cwd: job.cwd,
                  stdin: "ignore",
                  stdout: "pipe",
                  stderr: "pipe",
                  killSignal: "SIGKILL",
                  forceKillAfter: 1000,
                }),
              ),
            );
            handles.set(job.id, captured);
            yield* update(latest, { state: "running" });
            return captured;
          }),
        );
        if (!handle) return;
        yield* Effect.addFinalizer(() =>
          Effect.gen(function* () {
            // Keep the captured handle until native termination is acknowledged.
            yield* handle.kill({ killSignal: "SIGKILL" }).pipe(Effect.catch(() => Effect.void));
            yield* handle.exitCode.pipe(Effect.catch(() => Effect.void));
            handles.delete(job.id);
            outputChunks.delete(job.id);
            decoders.delete(`${job.id}:stdout`);
            decoders.delete(`${job.id}:stderr`);
            progressLines.delete(job.id);
            yield* lock
              .withPermit(
                Effect.gen(function* () {
                  const latest = yield* read(job.id);
                  if (latest && !terminal(latest.state))
                    yield* update(latest, {
                      state: "interrupted",
                      reason: "Host supervisor stopped. Job was not rerun.",
                    });
                }),
              )
              .pipe(Effect.catch(() => Effect.void));
          }),
        );

        const result = yield* Effect.all(
          [
            handle.stdout.pipe(Stream.runForEach((chunk) => append(job.id, "stdout", chunk))),
            handle.stderr.pipe(Stream.runForEach((chunk) => append(job.id, "stderr", chunk))),
            handle.exitCode,
          ],
          { concurrency: "unbounded" },
        ).pipe(Effect.timeoutOption(input.timeoutMs), Effect.result);
        if (result._tag === "Failure" || Option.isNone(result.success)) {
          yield* handle.kill({ killSignal: "SIGKILL" }).pipe(Effect.catch(() => Effect.void));
          yield* handle.exitCode.pipe(Effect.catch(() => Effect.void));
        }
        const completed = yield* lock.withPermit(
          Effect.gen(function* () {
            const latest = yield* read(job.id);
            if (!latest) return null;
            const timedOut = result._tag === "Success" && Option.isNone(result.success);
            const exitCode =
              result._tag === "Success" && Option.isSome(result.success)
                ? result.success.value[2]
                : null;
            return yield* update(latest, {
              state:
                latest.state === "cancelling"
                  ? "cancelled"
                  : timedOut || result._tag === "Failure" || exitCode !== 0
                    ? "failed"
                    : "completed",
              exitCode,
              reason:
                latest.state === "cancelling"
                  ? "Cancelled by owner."
                  : timedOut
                    ? "Process timeout. Partial output retained."
                    : result._tag === "Failure"
                      ? "Process stream or execution failed."
                      : null,
            });
          }),
        );
        if (completed) yield* deliver(completed);
      }),
    ).pipe(
      Effect.catch((error) =>
        lock.withPermit(
          Effect.gen(function* () {
            const latest = yield* read(job.id);
            if (latest && !terminal(latest.state)) {
              const failed = yield* update(latest, { state: "failed", reason: error.detail });
              yield* deliver(failed);
            }
          }),
        ),
      ),
    );
  const start = (raw: BackgroundJobStartInput) =>
    lock.withPermit(
      Effect.gen(function* () {
        const input = yield* decodeBackgroundJobStartInput(raw).pipe(
          Effect.mapError(() => failure("invalid", "Invalid bounded process input.")),
        );
        const permission = yield* authority.authorize(input.callerThreadId);
        const requestJson = JSON.stringify({
          command: input.command,
          args: input.args,
          timeoutMs: input.timeoutMs,
          maxOutputBytes: input.maxOutputBytes,
        });
        const existing = yield* read(input.id);
        if (existing) {
          const requests = yield* wrap(
            sql<{
              request_json: string;
            }>`SELECT request_json FROM background_jobs WHERE id = ${input.id}`,
          );
          if (requests[0]?.request_json !== requestJson)
            return yield* failure(
              "conflict",
              "Job identifier already has different execution bounds.",
            );
          if (
            existing.ownerThreadId !== input.callerThreadId ||
            existing.command !== input.command ||
            JSON.stringify(existing.args) !== JSON.stringify(input.args)
          )
            return yield* failure("conflict", "Job identifier already has a different request.");
          return existing;
        }
        const retained = yield* wrap(
          sql<{ count: number }>`SELECT COUNT(*) AS count FROM background_jobs`,
        );
        if ((retained[0]?.count ?? 0) >= 256)
          return yield* failure(
            "running",
            "Retained job limit reached. Clean up terminal jobs before starting another.",
          );
        const active = yield* wrap(
          sql<{
            count: number;
          }>`SELECT COUNT(*) AS count FROM background_jobs WHERE state IN ('pending','running','cancelling')`,
        );
        if ((active[0]?.count ?? 0) >= 16)
          return yield* failure("running", "Host job concurrency limit reached.");
        const now = new Date(yield* Clock.currentTimeMillis).toISOString();
        const job: BackgroundJobRecord = {
          id: input.id,
          ownerThreadId: input.callerThreadId,
          projectId: permission.projectId,
          command: input.command,
          args: input.args,
          cwd: permission.cwd,
          state: "pending",
          createdAt: now,
          updatedAt: now,
          exitCode: null,
          reason: null,
          maxOutputBytes: input.maxOutputBytes,
          outputBytes: 0,
          truncated: false,
          progress: null,
        };
        yield* wrap(
          sql`INSERT INTO background_jobs(id,owner_thread_id,state,request_json,document_json) VALUES(${job.id},${job.ownerThreadId},${job.state},${requestJson},${JSON.stringify(job)})`,
        );
        yield* run(input, job).pipe(Effect.forkIn(scope));
        return job;
      }),
    );
  const list = (input: { callerThreadId: ThreadId }) =>
    wrap(
      Effect.gen(function* () {
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM background_jobs WHERE owner_thread_id = ${input.callerThreadId} ORDER BY id LIMIT 200`;
        return yield* Effect.forEach(rows, (row) =>
          decodeBackgroundJobRecord(JSON.parse(row.document_json)),
        );
      }),
    );
  const output = (raw: BackgroundJobOutputInput) =>
    wrap(
      Effect.gen(function* () {
        const input = yield* decodeBackgroundJobOutputInput(raw);
        const job = yield* get(input);
        if (input.cursor > job.outputBytes)
          return yield* failure("invalid", "Output cursor exceeds retained output.");
        const rows = yield* sql<{
          cursor: number;
          stream: "stdout" | "stderr";
          text: string;
        }>`SELECT cursor,stream,text FROM background_job_output WHERE job_id = ${input.id} ORDER BY cursor`;
        const chunks: Array<BackgroundJobOutput["chunks"][number]> = [];
        let remaining = input.limitBytes;
        let nextCursor = input.cursor;
        for (const row of rows) {
          const bytes = new TextEncoder().encode(row.text);
          if (row.cursor + bytes.length <= input.cursor) continue;
          const start = Math.max(0, input.cursor - row.cursor);
          const piece = bytes.subarray(start, start + remaining);
          if (piece.length === 0) break;
          chunks.push({
            cursor: row.cursor + start,
            stream: row.stream,
            text: new TextDecoder().decode(piece),
          });
          nextCursor = row.cursor + start + piece.length;
          remaining -= piece.length;
          if (remaining === 0) break;
        }
        return { chunks, nextCursor, truncated: job.truncated };
      }),
    );
  const cancel = (input: BackgroundJobReadInput) =>
    Effect.gen(function* () {
      const job = yield* lock.withPermit(
        Effect.gen(function* () {
          const current = yield* get(input);
          if (terminal(current.state)) return current;
          return yield* update(current, { state: "cancelling" });
        }),
      );
      if (!terminal(job.state)) {
        const handle = handles.get(job.id);
        if (handle) yield* wrap(handle.kill({ killSignal: "SIGKILL" }));
        else
          return yield* lock.withPermit(
            update(job, { state: "cancelled", reason: "Cancelled before process activation." }),
          );
      }
      return yield* get(input);
    });
  const wait = (raw: BackgroundJobWaitInput) =>
    Effect.scoped(
      Effect.gen(function* () {
        const input = yield* decodeBackgroundJobWaitInput(raw).pipe(
          Effect.mapError(() => failure("invalid", "Invalid wait input.")),
        );
        const subscription = yield* PubSub.subscribe(changes);
        const initial = yield* get(input);
        if (terminal(initial.state)) return { timedOut: false, job: initial };
        const settled = yield* Stream.fromSubscription(subscription).pipe(
          Stream.mapEffect(() => get(input)),
          Stream.filter((job) => terminal(job.state)),
          Stream.runHead,
          Effect.timeoutOption(input.timeoutMs),
        );
        if (Option.isSome(settled) && Option.isSome(settled.value))
          return { timedOut: false, job: settled.value.value };
        return { timedOut: true, job: yield* get(input) };
      }),
    );
  const subscribe = (input: BackgroundJobReadInput & { notify: boolean; wake: boolean }) =>
    lock.withPermit(
      wrap(
        Effect.gen(function* () {
          const job = yield* get(input);
          if (input.wake) yield* authority.authorize(input.callerThreadId);
          yield* sql`INSERT INTO background_job_notifications(job_id,subscriber_thread_id,notify,wake) VALUES(${job.id},${input.callerThreadId},${input.notify ? 1 : 0},${input.wake ? 1 : 0}) ON CONFLICT(job_id,subscriber_thread_id) DO UPDATE SET notify=excluded.notify,wake=excluded.wake`;
          yield* deliver(job);
          return job;
        }),
      ),
    );
  const cleanup = (input: BackgroundJobReadInput) =>
    lock.withPermit(
      wrap(
        Effect.gen(function* () {
          const job = yield* get(input);
          if (!terminal(job.state) || handles.has(job.id))
            return yield* failure("running", "Cannot clean up a live job.");
          yield* sql.withTransaction(
            Effect.gen(function* () {
              yield* sql`DELETE FROM background_job_output WHERE job_id = ${job.id}`;
              yield* sql`DELETE FROM background_job_notifications WHERE job_id = ${job.id}`;
              yield* sql`DELETE FROM background_jobs WHERE id = ${job.id}`;
            }),
          );
        }),
      ),
    );
  const reconcile = lock.withPermit(
    wrap(
      Effect.gen(function* () {
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM background_jobs WHERE state IN ('pending','running','cancelling')`;
        for (const row of rows) {
          const job = yield* decodeBackgroundJobRecord(JSON.parse(row.document_json));
          if (handles.has(job.id)) continue;
          const interrupted = yield* update(job, {
            state: "interrupted",
            reason: "Host restarted. Captured process cannot be reattached and was not rerun.",
          });
          yield* deliver(interrupted);
        }
        const pendingNotifications = yield* sql<{
          document_json: string;
        }>`SELECT DISTINCT jobs.document_json FROM background_jobs jobs JOIN background_job_notifications subscriptions ON subscriptions.job_id = jobs.id WHERE subscriptions.delivered = 0 AND jobs.state IN ('completed','failed','cancelled','interrupted')`;
        for (const row of pendingNotifications)
          yield* deliver(yield* decodeBackgroundJobRecord(JSON.parse(row.document_json)));
      }),
    ),
  );
  return BackgroundJobs.of({
    start,
    list,
    get,
    output,
    cancel,
    wait,
    subscribe,
    cleanup,
    reconcile,
  });
});
export const layer = Layer.effect(BackgroundJobs, make);
