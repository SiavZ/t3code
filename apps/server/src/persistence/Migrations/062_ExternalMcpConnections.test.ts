import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migrate from "./062_ExternalMcpConnections.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("062_ExternalMcpConnections", (it) => {
  it.effect(
    "retains explicit approval and configuration on rerun without durable live handles",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* migrate;
        yield* sql`INSERT INTO external_mcp_connections (id,name,config_json,approved,revision,updated_at) VALUES ('fixture','Fixture','{"transport":"http","url":"https://example.invalid/mcp"}',1,2,'2026-10-03T00:00:00Z')`;
        yield* migrate;
        const rows = yield* sql<{
          id: string;
          approved: number;
          revision: number;
        }>`SELECT id, approved, revision FROM external_mcp_connections`;
        expect(rows).toEqual([{ id: "fixture", approved: 1, revision: 2 }]);
        const columns = yield* sql<{ name: string }>`PRAGMA table_info(external_mcp_connections)`;
        expect(columns.map((column) => column.name)).not.toContain("pid");
        expect(columns.map((column) => column.name)).not.toContain("session_id");
      }),
  );
});
