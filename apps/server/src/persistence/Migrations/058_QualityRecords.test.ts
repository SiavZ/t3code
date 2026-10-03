import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migration from "./058_QualityRecords.ts";
it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("058_QualityRecords", (it) => {
  it.effect("is repeatable without overwriting durable records or operation identities", () =>
    Effect.gen(function* () {
      yield* migration;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO quality_records VALUES('thread',1,'{}')`;
      yield* sql`INSERT INTO quality_operations VALUES('thread','operation',1,${"a".repeat(64)})`;
      yield* migration;
      const rows = yield* sql<{ revision: number }>`SELECT revision FROM quality_records`;
      assert.equal(rows[0]?.revision, 1);
      const duplicate =
        yield* sql`INSERT INTO quality_operations VALUES('thread','operation',2,${"b".repeat(64)})`.pipe(
          Effect.result,
        );
      assert.equal(duplicate._tag, "Failure");
    }),
  );
});
