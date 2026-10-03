import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS memory_entries (id TEXT PRIMARY KEY, scope TEXT NOT NULL CHECK(scope IN ('project','global')), project_id TEXT, category TEXT NOT NULL, content TEXT NOT NULL, tags_json TEXT NOT NULL, source_thread_id TEXT, source_message_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL, revision INTEGER NOT NULL, CHECK((scope='global' AND project_id IS NULL) OR (scope='project' AND project_id IS NOT NULL)))`;
  yield* sql`CREATE INDEX IF NOT EXISTS memory_entries_scope ON memory_entries(project_id, scope, updated_at, id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS memory_links (from_id TEXT NOT NULL REFERENCES memory_entries(id) ON DELETE CASCADE, to_id TEXT NOT NULL REFERENCES memory_entries(id) ON DELETE CASCADE, relation TEXT NOT NULL, PRIMARY KEY(from_id,to_id,relation))`;
  // Receipts retain a one-way request hash, never a serialized body.
  yield* sql`CREATE TABLE IF NOT EXISTS memory_operations (authority_key TEXT NOT NULL, operation_id TEXT NOT NULL, entry_id TEXT NOT NULL, revision INTEGER NOT NULL, status TEXT NOT NULL, request_fingerprint TEXT NOT NULL CHECK(length(request_fingerprint)=64), PRIMARY KEY(authority_key,operation_id))`;
  yield* sql`CREATE INDEX IF NOT EXISTS memory_operations_entry ON memory_operations(entry_id,status)`;
});
