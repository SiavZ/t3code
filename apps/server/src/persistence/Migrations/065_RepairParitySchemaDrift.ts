import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import Migration0056 from "./056_CoordinationPlans.ts";
import Migration0057 from "./057_Memory.ts";
import Migration0058 from "./058_QualityRecords.ts";
import Migration0059 from "./059_ScheduledWork.ts";
import Migration0060 from "./060_BackgroundJobs.ts";
import Migration0062 from "./062_ExternalMcpConnections.ts";
import Migration0063 from "./063_RuntimeOperations.ts";

/**
 * Databases that recorded migrations 056-064 while those migrations were still
 * changing kept the earlier shape, because the migrator never reruns a recorded
 * id. Re-apply the idempotent ones and add the tables and columns the
 * non-idempotent ones introduced later. Every statement is a no-op on a
 * database that already has the final shape.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* Migration0056;
  yield* Migration0057;
  yield* Migration0058;
  yield* Migration0059;
  yield* Migration0060;
  yield* Migration0062;
  yield* Migration0063;

  yield* sql`CREATE TABLE IF NOT EXISTS agent_document_assets (asset_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, owner_thread_id TEXT NOT NULL, byte_length INTEGER NOT NULL)`;
  yield* sql`CREATE INDEX IF NOT EXISTS agent_document_assets_scope ON agent_document_assets(project_id, owner_thread_id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_documents (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, owner_thread_id TEXT NOT NULL, revision INTEGER NOT NULL, snapshot TEXT NOT NULL, mounted_after_sequence INTEGER NOT NULL DEFAULT 0)`;
  yield* sql`CREATE INDEX IF NOT EXISTS agent_documents_scope ON agent_documents(project_id, owner_thread_id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_document_operations (document_id TEXT NOT NULL, operation_id TEXT NOT NULL, fingerprint TEXT NOT NULL, result TEXT NOT NULL, PRIMARY KEY(document_id, operation_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS agent_document_actions (sequence INTEGER PRIMARY KEY AUTOINCREMENT, document_id TEXT NOT NULL, action_id TEXT NOT NULL, fingerprint TEXT NOT NULL, snapshot TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0, UNIQUE(document_id, action_id))`;
  yield* sql`CREATE TABLE IF NOT EXISTS integration_approvals (approval_id TEXT PRIMARY KEY, human_session_id TEXT NOT NULL, operation TEXT NOT NULL, review_digest TEXT NOT NULL, expires_at INTEGER NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS integration_selections (selection_id TEXT PRIMARY KEY, selection_json TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS integration_attempts (operation_id TEXT PRIMARY KEY, operation TEXT NOT NULL, review_digest TEXT NOT NULL, state TEXT NOT NULL)`;

  const addColumn = (table: string, column: string, definition: string) =>
    Effect.gen(function* () {
      const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(${sql(table)})`;
      if (!columns.some((entry) => entry.name === column))
        yield* sql.unsafe(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    });
  // Older rows predate request fingerprints. An empty value never matches a
  // retry's 64-character hash, so those operation ids are simply not deduplicated.
  yield* addColumn("memory_operations", "request_fingerprint", "TEXT NOT NULL DEFAULT ''");
  yield* addColumn("quality_operations", "request_fingerprint", "TEXT NOT NULL DEFAULT ''");
  yield* addColumn(
    "scheduled_work",
    "request_json",
    "TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(request_json))",
  );
  yield* addColumn(
    "background_jobs",
    "request_json",
    "TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(request_json))",
  );
  yield* addColumn("background_job_notifications", "suppressed_reason", "TEXT");
  yield* addColumn("agent_documents", "mounted_after_sequence", "INTEGER NOT NULL DEFAULT 0");
});
