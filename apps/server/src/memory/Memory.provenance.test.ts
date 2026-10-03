import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/057_Memory.ts";
import migrateProjections from "../persistence/Migrations/005_Projections.ts";
import * as Memory from "./Memory.ts";
const services = Memory.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
it.layer(services)("Memory provenance", (it) => {
  it.effect("validates explicit message ownership without harvesting transcript bodies", () =>
    Effect.gen(function* () {
      yield* migrate;
      yield* migrateProjections;
      const memory = yield* Memory.MemoryService;
      const sql = yield* SqlClient.SqlClient;
      const authority = { projectId: ProjectId.make("project"), allowGlobal: false };
      const threadId = ThreadId.make("provenance-thread");
      yield* sql`INSERT INTO projection_thread_messages VALUES('source-message',${threadId},NULL,'user','User-authored source',0,'2026-10-03T00:00:00.000Z','2026-10-03T00:00:00.000Z')`;
      const input = {
        id: "provenance",
        operationId: "provenance",
        scope: "project" as const,
        category: "fact" as const,
        content: "Explicit memory",
        tags: [],
        sourceMessageId: "source-message",
      };
      assert.equal((yield* memory.remember(input, authority).pipe(Effect.flip)).code, "forbidden");
      assert.equal(
        (yield* memory
          .remember(input, { ...authority, threadId: ThreadId.make("foreign-thread") })
          .pipe(Effect.flip)).code,
        "invalid",
      );
      yield* memory.remember(input, { ...authority, threadId });
      const recalled = yield* memory.search({ query: "memory" }, { ...authority, threadId });
      assert.equal(recalled.entries[0]?.sourceMessageId, "source-message");
      assert.equal(recalled.entries[0]?.content, "Explicit memory");
    }),
  );
});
