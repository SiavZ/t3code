import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migration from "./058_Memory.ts";
it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("058_Memory", (it) => {
  it.effect(
    "is repeatable and enforces project/global store identity with content-free receipts",
    () =>
      Effect.gen(function* () {
        yield* migration;
        yield* migration;
        const sql = yield* SqlClient.SqlClient;
        const invalid =
          yield* sql`INSERT INTO memory_entries VALUES('invalid','global','project','fact','body','[]',NULL,NULL,'now','now',1)`.pipe(
            Effect.result,
          );
        assert.equal(invalid._tag, "Failure");
        const columns = yield* sql<{ name: string }>`PRAGMA table_info(memory_operations)`;
        assert.deepEqual(
          columns.map((column) => column.name),
          [
            "authority_key",
            "operation_id",
            "entry_id",
            "revision",
            "status",
            "request_fingerprint",
          ],
        );
      }),
  );
});
