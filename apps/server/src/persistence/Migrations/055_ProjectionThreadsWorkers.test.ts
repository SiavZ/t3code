import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { runMigrations } from "../Migrations.ts";
import migrateWorkers from "./055_ProjectionThreadsWorkers.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("055_ProjectionThreadsWorkers", (it) => {
  it.effect(
    "keeps ordinary threads unowned, preserves worker metadata on rerun and adds the root lookup index",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 54 });
        const now = "2026-10-02T17:00:00.000Z";
        yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at)
        VALUES ('thread', 'project', 'Existing thread', '{"instanceId":"codex","model":"gpt-5.4"}', 'approval-required', ${now}, ${now})`;
        yield* runMigrations({ toMigrationInclusive: 55 });
        const ordinary = yield* sql<{
          worker: string | null;
        }>`SELECT worker_json AS worker FROM projection_threads WHERE thread_id = 'thread'`;
        assert.deepEqual(ordinary, [{ worker: null }]);
        const metadata = JSON.stringify({ ownerThreadId: "root", rootThreadId: "root", depth: 1 });
        yield* sql`UPDATE projection_threads SET worker_json = ${metadata} WHERE thread_id = 'thread'`;
        yield* migrateWorkers;
        const worker = yield* sql<{
          worker: string | null;
        }>`SELECT worker_json AS worker FROM projection_threads WHERE thread_id = 'thread'`;
        assert.deepEqual(worker, [{ worker: metadata }]);
        const indexes = yield* sql<{ name: string }>`PRAGMA index_list(projection_threads)`;
        assert.isTrue(indexes.some(({ name }) => name === "idx_projection_threads_worker_root"));
      }),
  );
});
