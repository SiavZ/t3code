import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE integration_approvals (approval_id TEXT PRIMARY KEY, human_session_id TEXT NOT NULL, operation TEXT NOT NULL, review_digest TEXT NOT NULL, expires_at INTEGER NOT NULL)`;
  yield* sql`CREATE TABLE integration_selections (selection_id TEXT PRIMARY KEY, selection_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE integration_attempts (operation_id TEXT PRIMARY KEY, operation TEXT NOT NULL, review_digest TEXT NOT NULL, state TEXT NOT NULL)`;
});
