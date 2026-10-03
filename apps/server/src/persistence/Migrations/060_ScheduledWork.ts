import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
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
