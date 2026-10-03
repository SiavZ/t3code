import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS provider_diagnostics (run_id TEXT PRIMARY KEY, input_json TEXT NOT NULL, result_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS provider_diagnostic_consents (run_id TEXT PRIMARY KEY, approval_id TEXT NOT NULL UNIQUE, human_session_id TEXT NOT NULL, review_digest TEXT NOT NULL, expires_at INTEGER NOT NULL)`;
});
