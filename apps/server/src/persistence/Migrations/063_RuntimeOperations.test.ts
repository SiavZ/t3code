import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migration from "./063_RuntimeOperations.ts";
it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("runtime migration", (it) => {
  it.effect("replays safely and isolates seed delivery by thread and native epoch", () =>
    Effect.gen(function* () {
      yield* migration;
      yield* migration;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`INSERT INTO runtime_seed_reservations VALUES('thread','old','turn')`;
      yield* sql`INSERT INTO runtime_seed_reservations VALUES('thread','new','turn')`;
      yield* sql`INSERT INTO runtime_seed_deliveries VALUES('thread','old','turn')`;
      const rows = yield* sql<{
        epoch_id: string;
      }>`SELECT epoch_id FROM runtime_seed_reservations ORDER BY epoch_id`;
      assert.deepEqual(
        rows.map((row) => row.epoch_id),
        ["new", "old"],
      );
      const delivered =
        yield* sql`SELECT 1 FROM runtime_seed_deliveries WHERE thread_id='thread' AND epoch_id='new'`;
      assert.equal(delivered.length, 0);
      const duplicate =
        yield* sql`INSERT INTO runtime_seed_deliveries VALUES('thread','old','different-turn')`.pipe(
          Effect.result,
        );
      assert.equal(duplicate._tag, "Failure");
    }),
  );
});
