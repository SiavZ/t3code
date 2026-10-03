import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS quality_records (thread_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, record_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS quality_operations (thread_id TEXT NOT NULL, operation_id TEXT NOT NULL, revision INTEGER NOT NULL, request_fingerprint TEXT NOT NULL CHECK(length(request_fingerprint)=64), PRIMARY KEY(thread_id,operation_id))`;
});
