import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/058_Memory.ts";
import * as Memory from "./Memory.ts";
const db = NodeSqliteClient.layer({ filename: ":memory:" });
const services = Memory.layer.pipe(Layer.provideMerge(db));
const authority = { projectId: ProjectId.make("project-a"), allowGlobal: false };
const remember = {
  id: "m1",
  operationId: "r1",
  scope: "project" as const,
  category: "fact" as const,
  content: "SQLite memory persists",
  tags: [" SQL ", "sql"],
};
it.layer(services)("Memory", (it) => {
  it.effect(
    "persists explicit memories, dedupes retries, normalizes tags and forgets bodies and links",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const memory = yield* Memory.MemoryService;
        const sql = yield* SqlClient.SqlClient;
        yield* sql`DELETE FROM memory_links`;
        yield* sql`DELETE FROM memory_entries`;
        yield* sql`DELETE FROM memory_operations`;
        const saved = yield* memory.remember(remember, authority);
        assert.deepEqual(yield* memory.remember(remember, authority), saved);
        yield* memory.remember(
          { ...remember, id: "m2", operationId: "r2", content: "Related sqlite fact" },
          authority,
        );
        yield* memory.link(
          { id: "m1", toId: "m2", operationId: "l1", expectedRevision: 1, relation: "related" },
          authority,
        );
        assert.equal((yield* memory.related({ id: "m1" }, authority)).entries.length, 1);
        const recalled = yield* memory.recall({ query: "sqlite" }, authority);
        assert.equal(recalled.entries.length, 2);
        assert.deepEqual(recalled.entries[0]?.tags, ["sql"]);
        yield* memory.forget({ id: "m1", operationId: "f1", expectedRevision: 2 }, authority);
        assert.equal((yield* memory.related({ id: "m2" }, authority)).entries.length, 0);
        assert.equal((yield* memory.search({ query: "" }, authority)).entries.length, 1);
        const receipts = yield* sql`SELECT * FROM memory_operations`;
        assert.ok(!JSON.stringify(receipts).includes("SQLite memory persists"));
        yield* memory.remember(remember, authority);
        assert.equal((yield* memory.search({ query: "" }, authority)).entries.length, 1);
      }),
  );
  it.effect("denies foreign project and ungranted global operations", () =>
    Effect.gen(function* () {
      yield* migrate;
      const memory = yield* Memory.MemoryService;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM memory_links`;
      yield* sql`DELETE FROM memory_entries`;
      yield* sql`DELETE FROM memory_operations`;
      yield* memory.remember(remember, authority);
      const foreign = { ...authority, projectId: ProjectId.make("other") };
      const denied = yield* memory
        .tag({ id: "m1", operationId: "tag", expectedRevision: 1, tags: [] }, foreign)
        .pipe(Effect.flip);
      assert.equal(denied.code, "forbidden");
      assert.equal(
        (yield* memory
          .remember({ ...remember, id: "g", scope: "global", operationId: "g" }, authority)
          .pipe(Effect.flip)).code,
        "forbidden",
      );
      assert.equal(
        (yield* memory.search({ query: "", scope: "global" }, authority).pipe(Effect.flip)).code,
        "forbidden",
      );
    }),
  );
  it.effect("CAS rejects stale concurrent updates and contextual recall respects budget", () =>
    Effect.gen(function* () {
      yield* migrate;
      const memory = yield* Memory.MemoryService;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`DELETE FROM memory_links`;
      yield* sql`DELETE FROM memory_entries`;
      yield* sql`DELETE FROM memory_operations`;
      yield* memory.remember(remember, authority);
      const results = yield* Effect.all(
        [
          memory
            .tag({ id: "m1", operationId: "a", expectedRevision: 1, tags: ["a"] }, authority)
            .pipe(Effect.result),
          memory
            .tag({ id: "m1", operationId: "b", expectedRevision: 1, tags: ["b"] }, authority)
            .pipe(Effect.result),
        ],
        { concurrency: 2 },
      );
      assert.equal(results.filter((r) => r._tag === "Success").length, 1);
      const recalled = yield* memory.recallForTurn({ query: "sqlite", budget: 1 }, authority);
      assert.equal(recalled.text, "");
      assert.equal(recalled.mode, "lexical-context");
    }),
  );
});
