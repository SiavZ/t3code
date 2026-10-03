import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS runtime_hooks (project_id TEXT NOT NULL, hook_id TEXT NOT NULL, config_json TEXT NOT NULL, PRIMARY KEY(project_id,hook_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS runtime_hook_receipts (project_id TEXT NOT NULL, hook_id TEXT NOT NULL, receipt_id TEXT NOT NULL, result_json TEXT NOT NULL, PRIMARY KEY(project_id,hook_id,receipt_id))`;
});
