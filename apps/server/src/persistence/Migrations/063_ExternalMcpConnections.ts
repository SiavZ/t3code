import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS external_mcp_connections (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    config_json TEXT NOT NULL,
    approved INTEGER NOT NULL DEFAULT 0 CHECK (approved IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 1,
    updated_at TEXT NOT NULL
  )`;
});
