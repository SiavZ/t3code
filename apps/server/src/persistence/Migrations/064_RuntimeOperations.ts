import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS runtime_seed_reservations (thread_id TEXT NOT NULL, epoch_id TEXT NOT NULL, turn_key TEXT NOT NULL, PRIMARY KEY(thread_id,epoch_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS runtime_seed_deliveries (thread_id TEXT NOT NULL, epoch_id TEXT NOT NULL, turn_key TEXT NOT NULL, PRIMARY KEY(thread_id,epoch_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_runtime_metadata (thread_id TEXT PRIMARY KEY, metadata_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS runtime_operations (operation_id TEXT PRIMARY KEY, thread_id TEXT NOT NULL, kind TEXT NOT NULL, input_json TEXT NOT NULL, receipt_json TEXT NOT NULL)`;
});
