import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_thread_activation_authorities (
    thread_id TEXT PRIMARY KEY NOT NULL,
    message_id TEXT NOT NULL,
    event_sequence INTEGER NOT NULL,
    authority_json TEXT CHECK(authority_json IS NULL OR json_valid(authority_json))
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS unattended_grants (
    grant_id TEXT PRIMARY KEY NOT NULL, owner_thread_id TEXT NOT NULL,
    project_id TEXT NOT NULL, revision INTEGER NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0 CHECK (revoked IN (0,1)),
    ceiling_json TEXT NOT NULL CHECK(json_valid(ceiling_json)),
    host_jobs INTEGER NOT NULL DEFAULT 0 CHECK(host_jobs IN (0,1)),
    created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS scheduled_work (
    id TEXT PRIMARY KEY NOT NULL, owner_thread_id TEXT NOT NULL,
    due_at TEXT NOT NULL, state TEXT NOT NULL,
    request_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(request_json)),
    document_json TEXT NOT NULL CHECK(json_valid(document_json))
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_scheduled_work_message ON scheduled_work(json_extract(document_json, '$.messageId'))`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_scheduled_work_due ON scheduled_work(state, due_at, id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_scheduled_work_owner ON scheduled_work(owner_thread_id, id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS ambient_work (
    owner_thread_id TEXT PRIMARY KEY NOT NULL,
    document_json TEXT NOT NULL CHECK(json_valid(document_json))
  )`;
});
