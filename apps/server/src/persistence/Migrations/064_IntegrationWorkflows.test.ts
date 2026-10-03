import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { testDatabase } from "../../integrations/integrationTestSupport.ts";

it.effect("creates isolated approval, provenance and attempt tables with unique identities", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const tables = yield* sql<{
      name: string;
    }>`SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'integration_%' ORDER BY name`;
    expect(tables.map((row) => row.name)).toEqual([
      "integration_approvals",
      "integration_attempts",
      "integration_selections",
    ]);
    yield* sql`INSERT INTO integration_attempts (operation_id, operation, review_digest, state) VALUES ('fixture', 'image.create', 'digest', 'unknown')`;
    const duplicate =
      yield* sql`INSERT INTO integration_attempts (operation_id, operation, review_digest, state) VALUES ('fixture', 'image.create', 'digest', 'completed')`.pipe(
        Effect.result,
      );
    expect(duplicate._tag).toBe("Failure");
    const rows = yield* sql<{
      state: string;
    }>`SELECT state FROM integration_attempts WHERE operation_id = 'fixture'`;
    expect(rows).toEqual([{ state: "unknown" }]);
  }).pipe(Effect.provide(testDatabase())),
);
