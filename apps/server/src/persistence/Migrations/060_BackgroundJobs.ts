import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS background_jobs (
    id TEXT PRIMARY KEY NOT NULL, owner_thread_id TEXT NOT NULL,
    state TEXT NOT NULL, request_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(request_json)),
    document_json TEXT NOT NULL CHECK(json_valid(document_json))
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_background_jobs_owner ON background_jobs(owner_thread_id, id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS background_job_output (
    job_id TEXT NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
    cursor INTEGER NOT NULL, stream TEXT NOT NULL CHECK(stream IN ('stdout','stderr')),
    text TEXT NOT NULL, PRIMARY KEY(job_id, cursor)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS background_job_notifications (
    job_id TEXT NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
    subscriber_thread_id TEXT NOT NULL, notify INTEGER NOT NULL CHECK(notify IN (0,1)),
    wake INTEGER NOT NULL CHECK(wake IN (0,1)), delivered INTEGER NOT NULL DEFAULT 0,
    suppressed_reason TEXT,
    PRIMARY KEY(job_id, subscriber_thread_id)
  )`;
});
