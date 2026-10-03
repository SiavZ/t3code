import * as Deferred from "effect/Deferred";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import { BackgroundJobError } from "@t3tools/contracts";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import * as BackgroundJobs from "./BackgroundJobs.ts";
import migrate from "../persistence/Migrations/061_BackgroundJobs.ts";

const owner = ThreadId.make("job-owner");
const authority = Layer.succeed(BackgroundJobs.BackgroundJobAuthority, {
  authorize: () => Effect.succeed({ projectId: ProjectId.make("project"), cwd: process.cwd() }),
  notify: () => Effect.void,
});
const dependencies = Layer.mergeAll(
  NodeSqliteClient.layer({ filename: ":memory:" }),
  NodeServices.layer,
  authority,
);
const live = BackgroundJobs.layer.pipe(Layer.provideMerge(dependencies));
const input = (id: string, source: string) => ({
  id,
  callerThreadId: owner,
  command: process.execPath,
  args: ["-e", source],
  timeoutMs: 10_000,
  maxOutputBytes: 1024,
});

it.live("streams real stdout/stderr/progress, cursor slices and bounded truncation", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* migrate;
      const jobs = yield* BackgroundJobs.BackgroundJobs;
      yield* jobs.start(
        input(
          "short",
          'process.stdout.write("hello\\nT3_PROGRESS {\\"value\\":0.5,\\"label\\":\\"half\\"}\\n"); process.stderr.write("warning\\n")',
        ),
      );
      const result = yield* jobs.wait({ callerThreadId: owner, id: "short", timeoutMs: 10_000 });
      expect(result.timedOut).toBe(false);
      expect(result.job.state).toBe("completed");
      expect(result.job.progress).toEqual({ value: 0.5, label: "half" });
      const output = yield* jobs.output({
        callerThreadId: owner,
        id: "short",
        cursor: 0,
        limitBytes: 65_536,
      });
      expect(
        output.chunks
          .filter((chunk) => chunk.stream === "stdout")
          .map((chunk) => chunk.text)
          .join(""),
      ).toContain("hello");
      expect(
        output.chunks
          .filter((chunk) => chunk.stream === "stderr")
          .map((chunk) => chunk.text)
          .join(""),
      ).toContain("warning");
      const first = yield* jobs.output({
        callerThreadId: owner,
        id: "short",
        cursor: 0,
        limitBytes: 2,
      });
      expect(first.nextCursor).toBe(2);
      yield* jobs.start(input("large", 'process.stdout.write("x".repeat(4096))'));
      const large = yield* jobs.wait({ callerThreadId: owner, id: "large", timeoutMs: 10_000 });
      expect(large.job.outputBytes).toBe(1024);
      expect(large.job.truncated).toBe(true);
    }).pipe(Effect.provide(live)),
  ),
);

it.live("cancels only its captured process, rejects foreign owners and refuses live cleanup", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* migrate;
      const jobs = yield* BackgroundJobs.BackgroundJobs;
      yield* jobs.start(
        input("owned", 'process.stdout.write("ready\\n"); setInterval(() => {}, 1000)'),
      );
      const foreign = yield* jobs
        .get({ callerThreadId: ThreadId.make("foreign"), id: "owned" })
        .pipe(Effect.result);
      expect(foreign._tag).toBe("Failure");
      const cleanup = yield* jobs
        .cleanup({ callerThreadId: owner, id: "owned" })
        .pipe(Effect.result);
      expect(cleanup._tag).toBe("Failure");
      yield* jobs.cancel({ callerThreadId: owner, id: "owned" });
      const result = yield* jobs.wait({ callerThreadId: owner, id: "owned", timeoutMs: 10_000 });
      expect(result.timedOut).toBe(false);
      expect(result.job.state).toBe("cancelled");
    }).pipe(Effect.provide(live)),
  ),
);

it.live("rejects job retries with changed timeout or output bounds", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* migrate;
      const jobs = yield* BackgroundJobs.BackgroundJobs;
      const request = input("retry-job", "process.stdout.write('done')");
      yield* jobs.start(request);
      yield* jobs.wait({ callerThreadId: owner, id: request.id, timeoutMs: 10_000 });
      expect((yield* jobs.start(request)).id).toBe(request.id);
      expect((yield* jobs.start({ ...request, timeoutMs: 2000 }).pipe(Effect.result))._tag).toBe(
        "Failure",
      );
      expect(
        (yield* jobs.start({ ...request, maxOutputBytes: 2048 }).pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(live)),
  ),
);

it.live("persists denied terminal wake suppression and recovery remains ready", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* migrate;
      const jobs = yield* BackgroundJobs.BackgroundJobs;
      yield* jobs.start(input("revoked-notification", "process.stdout.write('done')"));
      yield* jobs.wait({ callerThreadId: owner, id: "revoked-notification", timeoutMs: 10_000 });
      yield* jobs.subscribe({
        callerThreadId: owner,
        id: "revoked-notification",
        notify: true,
        wake: true,
      });
      yield* jobs.reconcile;
      yield* jobs.reconcile;
      const sql = yield* SqlClient.SqlClient;
      const rows = yield* sql<{
        delivered: number;
        suppressed_reason: string;
      }>`SELECT delivered,suppressed_reason FROM background_job_notifications WHERE job_id = 'revoked-notification'`;
      expect(rows[0]?.delivered).toBe(2);
      expect(rows[0]?.suppressed_reason).toContain("revoked");
    }).pipe(
      Effect.provide(
        BackgroundJobs.layer.pipe(
          Layer.provideMerge(
            Layer.mergeAll(
              NodeSqliteClient.layer({ filename: ":memory:" }),
              NodeServices.layer,
              Layer.succeed(BackgroundJobs.BackgroundJobAuthority, {
                authorize: () =>
                  Effect.succeed({ projectId: ProjectId.make("project"), cwd: process.cwd() }),
                notify: () =>
                  Effect.fail(
                    new BackgroundJobError({ code: "forbidden", detail: "Grant revoked." }),
                  ),
              }),
            ),
          ),
        ),
      ),
    ),
  ),
);

it.live(
  "scope shutdown kills its SIGTERM-ignoring captured child before persisting interruption",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* migrate;
        const sql = yield* SqlClient.SqlClient;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const ready = yield* Deferred.make<void>();
        let launches = 0;
        const capturedSpawner = {
          ...spawner,
          spawn: (command: Parameters<typeof spawner.spawn>[0]) =>
            spawner.spawn(command).pipe(
              Effect.map((handle) => {
                launches++;
                return {
                  ...handle,
                  stdout: handle.stdout.pipe(Stream.tap(() => Deferred.succeed(ready, undefined))),
                };
              }),
            ),
        };
        yield* Effect.scoped(
          Effect.gen(function* () {
            const jobs = yield* BackgroundJobs.BackgroundJobs;
            yield* jobs.start(
              input(
                "shutdown-owned",
                "process.on('SIGTERM', () => {}); process.stdout.write('ready'); setInterval(() => {}, 1000)",
              ),
            );
            yield* Deferred.await(ready);
          }).pipe(
            Effect.provide(BackgroundJobs.layer),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, capturedSpawner),
          ),
        );
        const rows = yield* sql<{
          document_json: string;
        }>`SELECT document_json FROM background_jobs WHERE id = 'shutdown-owned'`;
        expect(JSON.parse(rows[0]!.document_json).state).toBe("interrupted");
        yield* Effect.scoped(
          Effect.gen(function* () {
            const jobs = yield* BackgroundJobs.BackgroundJobs;
            yield* jobs.reconcile;
          }).pipe(
            Effect.provide(BackgroundJobs.layer),
            Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, capturedSpawner),
          ),
        );
        expect(launches).toBe(1);
      }).pipe(Effect.provide(dependencies)),
    ),
);

it.live(
  "retains partial output on timeout and does not rerun a persisted interrupted process",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* migrate;
        const jobs = yield* BackgroundJobs.BackgroundJobs;
        yield* jobs.start({
          ...input("timeout", 'process.stdout.write("partial\\n"); setInterval(() => {}, 1000)'),
          timeoutMs: 1000,
        });
        const result = yield* jobs.wait({
          callerThreadId: owner,
          id: "timeout",
          timeoutMs: 10_000,
        });
        expect(result.job.state).toBe("failed");
        expect(result.job.reason).toContain("timeout");
        const output = yield* jobs.output({
          callerThreadId: owner,
          id: "timeout",
          cursor: 0,
          limitBytes: 65_536,
        });
        expect(output.chunks.map((chunk) => chunk.text).join("")).toContain("partial");
      }).pipe(Effect.provide(live)),
    ),
);
