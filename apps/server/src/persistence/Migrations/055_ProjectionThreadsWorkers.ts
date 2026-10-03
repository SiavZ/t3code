import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
  if (!columns.some((column) => column.name === "worker_json")) {
    yield* sql`ALTER TABLE projection_threads ADD COLUMN worker_json TEXT`;
  }
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_threads_worker_root
    ON projection_threads(json_extract(worker_json, '$.rootThreadId'))
    WHERE worker_json IS NOT NULL AND deleted_at IS NULL
  `;
});
