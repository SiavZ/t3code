import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import repair from "./065_RepairParitySchemaDrift.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("065_RepairParitySchemaDrift", (it) => {
  it.effect("completes a database that recorded early drafts of the parity migrations", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // The shape a development database kept after recording 056-064 early.
      yield* sql`CREATE TABLE memory_operations (authority_key TEXT NOT NULL, operation_id TEXT NOT NULL, entry_id TEXT NOT NULL, revision INTEGER NOT NULL, status TEXT NOT NULL, PRIMARY KEY(authority_key,operation_id))`;
      yield* sql`CREATE TABLE quality_operations (thread_id TEXT NOT NULL, operation_id TEXT NOT NULL, revision INTEGER NOT NULL, PRIMARY KEY(thread_id,operation_id))`;
      yield* sql`CREATE TABLE scheduled_work (id TEXT PRIMARY KEY NOT NULL, owner_thread_id TEXT NOT NULL, due_at TEXT NOT NULL, state TEXT NOT NULL, document_json TEXT NOT NULL)`;
      yield* sql`CREATE TABLE background_jobs (id TEXT PRIMARY KEY NOT NULL, owner_thread_id TEXT NOT NULL, state TEXT NOT NULL, document_json TEXT NOT NULL)`;
      yield* sql`CREATE TABLE background_job_notifications (job_id TEXT NOT NULL, subscriber_thread_id TEXT NOT NULL, notify INTEGER NOT NULL, wake INTEGER NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(job_id, subscriber_thread_id))`;
      yield* sql`CREATE TABLE agent_documents (id TEXT PRIMARY KEY, project_id TEXT NOT NULL, owner_thread_id TEXT NOT NULL, revision INTEGER NOT NULL, snapshot TEXT NOT NULL)`;
      yield* sql`INSERT INTO memory_operations VALUES('authority','old-op','entry',1,'stored')`;
      yield* sql`INSERT INTO scheduled_work VALUES('work','thread','2026-10-03T00:00:00Z','pending','{}')`;

      yield* repair;
      yield* repair;

      // The startup recovery query that failed on the stale database.
      const pending =
        yield* sql`SELECT thread_id,metadata_json FROM projection_runtime_metadata WHERE json_extract(metadata_json,'$.runtimeHandoff.status')='pending'`;
      assert.equal(pending.length, 0);
      for (const table of [
        "projection_turn_cancellations",
        "projection_coordination_mailboxes",
        "projection_thread_activation_authorities",
        "agent_document_assets",
        "runtime_seed_reservations",
        "runtime_seed_deliveries",
        "provider_diagnostic_consents",
        "integration_attempts",
      ]) {
        const rows =
          yield* sql`SELECT name FROM sqlite_master WHERE type='table' AND name=${table}`;
        assert.equal(rows.length, 1, table);
      }
      const columns = (table: string) =>
        sql<{ readonly name: string }>`PRAGMA table_info(${sql(table)})`.pipe(
          Effect.map((rows) => rows.map((row) => row.name)),
        );
      assert.include(yield* columns("memory_operations"), "request_fingerprint");
      assert.include(yield* columns("quality_operations"), "request_fingerprint");
      assert.include(yield* columns("scheduled_work"), "request_json");
      assert.include(yield* columns("background_jobs"), "request_json");
      assert.include(yield* columns("background_job_notifications"), "suppressed_reason");
      assert.include(yield* columns("agent_documents"), "mounted_after_sequence");
      // Existing rows survive and take the column defaults.
      const work = yield* sql<{
        readonly request_json: string;
      }>`SELECT request_json FROM scheduled_work`;
      assert.deepEqual(
        work.map((row) => row.request_json),
        ["{}"],
      );
      const operations = yield* sql<{
        readonly request_fingerprint: string;
      }>`SELECT request_fingerprint FROM memory_operations`;
      assert.deepEqual(
        operations.map((row) => row.request_fingerprint),
        [""],
      );
    }),
  );
});
