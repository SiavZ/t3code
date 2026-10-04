import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Agent-written project notes. Entries are immutable: agents remember and
  // forget, users list and delete. No foreign key to projects, so removing a
  // project leaves its notes for MemoryService to ignore rather than failing.
  yield* sql`
    CREATE TABLE IF NOT EXISTS agent_memory_entries (
      id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      category TEXT NOT NULL,
      content TEXT NOT NULL,
      source_thread_id TEXT,
      created_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_agent_memory_entries_project_created
    ON agent_memory_entries(project_id, created_at DESC, id)
  `;
});
