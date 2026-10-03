import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_turn_cancellations (
    thread_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    event_sequence INTEGER NOT NULL,
    expected_turn_id TEXT,
    PRIMARY KEY (thread_id, message_id)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_coordination_mailboxes (
    root_thread_id TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    document_json TEXT NOT NULL CHECK (json_valid(document_json))
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_coordination_plans (
    plan_id TEXT PRIMARY KEY NOT NULL,
    root_thread_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision >= 0),
    document_json TEXT NOT NULL CHECK (json_valid(document_json))
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_coordination_plans_root ON projection_coordination_plans(root_thread_id, plan_id)`;
});
