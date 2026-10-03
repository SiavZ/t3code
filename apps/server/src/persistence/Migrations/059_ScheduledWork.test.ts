import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import scheduled from "./059_ScheduledWork.ts";
import jobs from "./060_BackgroundJobs.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("automation migrations", (it) => {
  it.effect("are repeatable and retain durable schedule, output and notification records", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* scheduled;
      yield* jobs;
      yield* sql`INSERT INTO scheduled_work(id,owner_thread_id,due_at,state,document_json) VALUES('schedule','owner','2026-10-03T00:00:00.000Z','queued','{}')`;
      yield* sql`INSERT INTO background_jobs(id,owner_thread_id,state,document_json) VALUES('job','owner','completed','{}')`;
      yield* sql`INSERT INTO background_job_output(job_id,cursor,stream,text) VALUES('job',0,'stderr','retained')`;
      yield* sql`INSERT INTO background_job_notifications(job_id,subscriber_thread_id,notify,wake) VALUES('job','owner',1,0)`;
      yield* scheduled;
      yield* jobs;
      expect(
        (yield* sql<{
          text: string;
        }>`SELECT text FROM background_job_output WHERE job_id = 'job'`)[0]?.text,
      ).toBe("retained");
      expect(
        (yield* sql<{ count: number }>`SELECT count(*) AS count FROM scheduled_work`)[0]?.count,
      ).toBe(1);
      const indexes = yield* sql<{ name: string }>`PRAGMA index_list(scheduled_work)`;
      expect(indexes.some((row) => row.name === "idx_scheduled_work_due")).toBe(true);
      const invalid =
        yield* sql`INSERT INTO background_job_output(job_id,cursor,stream,text) VALUES('job',1,'bogus','bad')`.pipe(
          Effect.result,
        );
      expect(invalid._tag).toBe("Failure");
    }),
  );
});
