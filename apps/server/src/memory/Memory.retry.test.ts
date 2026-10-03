import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/057_Memory.ts";
import * as Memory from "./Memory.ts";
const services = Memory.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const authority = { projectId: ProjectId.make("retry-project"), allowGlobal: false };
it.layer(services)("Memory exact retry", (it) => {
  it.effect("rejects changed request bodies and retains only hashes after forget", () =>
    Effect.gen(function* () {
      yield* migrate;
      const memory = yield* Memory.MemoryService;
      const sql = yield* SqlClient.SqlClient;
      const input = {
        id: "exact",
        operationId: "remember-exact",
        scope: "project" as const,
        category: "fact" as const,
        content: "private forgotten body",
        tags: ["original"],
      };
      yield* memory.remember(input, authority);
      // Field order is not part of request identity.
      yield* memory.remember(
        {
          tags: input.tags,
          content: input.content,
          category: input.category,
          scope: input.scope,
          operationId: input.operationId,
          id: input.id,
        },
        authority,
      );
      for (const changed of [
        { ...input, content: "changed body" },
        { ...input, category: "correction" as const },
        { ...input, tags: ["changed"] },
      ]) {
        assert.equal(
          (yield* memory.remember(changed, authority).pipe(Effect.flip)).code,
          "conflict",
        );
      }
      for (const id of ["target-a", "target-b"])
        yield* memory.remember({ ...input, id, operationId: id }, authority);
      const tagged = { id: input.id, operationId: "tag-exact", expectedRevision: 1, tags: ["new"] };
      yield* memory.tag(tagged, authority);
      yield* memory.tag(tagged, authority);
      assert.equal(
        (yield* memory.tag({ ...tagged, tags: ["other"] }, authority).pipe(Effect.flip)).code,
        "conflict",
      );
      const linked = {
        id: input.id,
        operationId: "link-exact",
        expectedRevision: 2,
        toId: "target-a",
        relation: "related" as const,
      };
      yield* memory.link(linked, authority);
      yield* memory.link(linked, authority);
      assert.equal(
        (yield* memory.link({ ...linked, toId: "target-b" }, authority).pipe(Effect.flip)).code,
        "conflict",
      );
      assert.equal(
        (yield* memory.link({ ...linked, relation: "supports" }, authority).pipe(Effect.flip)).code,
        "conflict",
      );
      assert.equal(
        (yield* memory
          .forget({ id: input.id, operationId: linked.operationId, expectedRevision: 2 }, authority)
          .pipe(Effect.flip)).code,
        "conflict",
      );
      yield* memory.forget(
        { id: input.id, operationId: "forget-exact", expectedRevision: 3 },
        authority,
      );
      yield* memory.remember(input, authority);
      yield* memory.tag(tagged, authority);
      yield* memory.link(linked, authority);
      assert.equal(
        (yield* memory.remember({ ...input, content: "resurrect" }, authority).pipe(Effect.flip))
          .code,
        "conflict",
      );
      const entries = yield* sql`SELECT id FROM memory_entries WHERE id=${input.id}`;
      assert.equal(entries.length, 0);
      const receipts = yield* sql<{
        request_fingerprint: string;
      }>`SELECT * FROM memory_operations WHERE entry_id=${input.id}`;
      assert.equal(receipts.length, 4);
      assert.ok(receipts.every((receipt) => /^[a-f0-9]{64}$/.test(receipt.request_fingerprint)));
      assert.ok(!JSON.stringify(receipts).includes(input.content));
      assert.ok(!JSON.stringify(receipts).includes("original"));
      // Missing thread and a real thread named client are distinct trusted authorities.
      assert.equal(
        (yield* memory
          .remember(input, { ...authority, threadId: ThreadId.make("client") })
          .pipe(Effect.flip)).code,
        "conflict",
      );
    }),
  );
});
