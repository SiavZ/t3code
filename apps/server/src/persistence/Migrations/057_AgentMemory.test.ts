import { assert, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { runMigrations } from "../Migrations.ts";

const layer = it.layer(Layer.mergeAll(NodeSqliteClient.layer({ filename: ":memory:" })));

layer("057_AgentMemory", (it) => {
  it.effect("creates the memory table and its project index, and is safe to rerun", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 56 });
      yield* runMigrations({ toMigrationInclusive: 57 });

      const columns = yield* sql<{ readonly name: string }>`
        SELECT name FROM pragma_table_info('agent_memory_entries') ORDER BY cid
      `;
      assert.deepEqual(
        columns.map((column) => column.name),
        ["id", "project_id", "category", "content", "source_thread_id", "created_at"],
      );
      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'index' AND tbl_name = 'agent_memory_entries' AND name LIKE 'idx_%'
      `;
      assert.deepEqual(
        indexes.map((index) => index.name),
        ["idx_agent_memory_entries_project_created"],
      );

      // A database that already has the table (for example from a preview build) still migrates.
      yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id = 57`;
      yield* runMigrations({ toMigrationInclusive: 57 });
    }),
  );
});
