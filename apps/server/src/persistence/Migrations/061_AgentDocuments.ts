import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE agent_document_assets (asset_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, owner_thread_id TEXT NOT NULL, byte_length INTEGER NOT NULL)`;
  yield* sql`CREATE INDEX agent_document_assets_scope ON agent_document_assets(project_id, owner_thread_id)`;
  // mounted_after_sequence is the last orchestration event committed before the mount.
  // A turn-end event at or before it belongs to an earlier turn and must not close the document.
  yield* sql`CREATE TABLE agent_documents (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, owner_thread_id TEXT NOT NULL, revision INTEGER NOT NULL, snapshot TEXT NOT NULL, mounted_after_sequence INTEGER NOT NULL DEFAULT 0)`;
  yield* sql`CREATE INDEX agent_documents_scope ON agent_documents(project_id, owner_thread_id)`;
  yield* sql`CREATE TABLE agent_document_operations (document_id TEXT NOT NULL, operation_id TEXT NOT NULL, fingerprint TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(document_id, operation_id))`;
  yield* sql`CREATE TABLE agent_document_actions (sequence INTEGER PRIMARY KEY AUTOINCREMENT, document_id TEXT NOT NULL, action_id TEXT NOT NULL, fingerprint TEXT NOT NULL, snapshot TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0, UNIQUE(document_id, action_id))`;
});
