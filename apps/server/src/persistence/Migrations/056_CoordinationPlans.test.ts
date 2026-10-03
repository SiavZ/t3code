import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migration from "./056_CoordinationPlans.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("056_CoordinationPlans", (it) => {
  it.effect("preserves attempts on rerun and enforces valid documents and revisions", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* migration;
      const document = JSON.stringify({
        attempts: [{ number: 1, artifact: { summary: "retained" } }],
      });
      yield* sql`INSERT INTO projection_coordination_plans VALUES ('plan', 'root', 1, ${document})`;
      yield* migration;
      const rows = yield* sql<{
        document_json: string;
      }>`SELECT document_json FROM projection_coordination_plans WHERE root_thread_id = 'root'`;
      assert.deepEqual(rows, [{ document_json: document }]);
      const invalid =
        yield* sql`INSERT INTO projection_coordination_plans VALUES ('bad', 'root', -1, '{}')`.pipe(
          Effect.exit,
        );
      assert.isTrue(invalid._tag === "Failure");
      const malformed =
        yield* sql`INSERT INTO projection_coordination_plans VALUES ('bad', 'root', 0, 'broken')`.pipe(
          Effect.exit,
        );
      assert.isTrue(malformed._tag === "Failure");
      const indexes = yield* sql<{
        name: string;
      }>`PRAGMA index_list(projection_coordination_plans)`;
      assert.isTrue(indexes.some((index) => index.name === "idx_coordination_plans_root"));
    }),
  );
});
