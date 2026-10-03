import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/058_Memory.ts";
import * as Memory from "./Memory.ts";
const services = Memory.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const a = {
  projectId: ProjectId.make("global-project-a"),
  threadId: ThreadId.make("global-thread-a"),
  allowGlobal: true,
};
const b = {
  projectId: ProjectId.make("global-project-b"),
  threadId: ThreadId.make("global-thread-b"),
  allowGlobal: true,
};
it.layer(services)("Memory global authority", (it) => {
  it.effect(
    "shares global namespace only under explicit authority and binds retries to the original authority",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const memory = yield* Memory.MemoryService;
        const input = {
          id: "shared-global",
          operationId: "remember-global",
          scope: "global" as const,
          category: "fact" as const,
          content: "Sharedglobalsecret reference",
          tags: [],
        };
        const remembered = yield* memory.remember(input, a);
        assert.deepEqual(yield* memory.remember(input, a), remembered);
        const shared = yield* memory.recall({ query: "Sharedglobalsecret", scope: "global" }, b);
        assert.equal(shared.entries[0]?.id, input.id);
        assert.equal(shared.entries[0]?.projectId, null);
        assert.equal(
          (yield* memory.recall({ query: "Sharedglobalsecret" }, { ...b, allowGlobal: false }))
            .entries.length,
          0,
        );
        assert.equal(
          (yield* memory.recall({ query: "Sharedglobalsecret", scope: "project" }, b)).entries
            .length,
          0,
        );
        assert.equal(
          (yield* memory.recallForTurn({ query: "Sharedglobalsecret", scope: "project" }, b)).text,
          "",
        );
        assert.equal(
          (yield* memory
            .recall({ query: "Sharedglobalsecret", scope: "global" }, { ...b, allowGlobal: false })
            .pipe(Effect.flip)).code,
          "forbidden",
        );
        assert.equal(
          (yield* memory.remember(input, { ...a, allowGlobal: false }).pipe(Effect.flip)).code,
          "forbidden",
        );
        assert.equal((yield* memory.remember(input, b).pipe(Effect.flip)).code, "conflict");
        assert.equal(
          (yield* memory.remember({ ...input, scope: "project" }, a).pipe(Effect.flip)).code,
          "conflict",
        );
        const tag = {
          id: input.id,
          operationId: "global-tag",
          expectedRevision: 1,
          tags: ["shared"],
        };
        yield* memory.tag(tag, a);
        assert.equal(
          (yield* memory.tag(tag, { ...a, allowGlobal: false }).pipe(Effect.flip)).code,
          "forbidden",
        );
        assert.equal((yield* memory.tag(tag, b).pipe(Effect.flip)).code, "conflict");
        const retag = yield* memory.tag(
          { ...tag, operationId: "global-tag-b", expectedRevision: 2, tags: ["from-b"] },
          b,
        );
        assert.equal(retag.revision, 3);
        assert.deepEqual(
          (yield* memory.recall({ query: "Sharedglobalsecret", scope: "global" }, a)).entries[0]
            ?.tags,
          ["from-b"],
        );
        const local = {
          ...input,
          id: "local-a",
          operationId: "local-a",
          scope: "project" as const,
          content: "Localprojectsecret reference",
        };
        yield* memory.remember(local, a);
        assert.equal((yield* memory.recall({ query: "Localprojectsecret" }, b)).entries.length, 0);
        assert.equal(
          (yield* memory
            .tag({ id: local.id, operationId: "foreign-local", expectedRevision: 1, tags: [] }, b)
            .pipe(Effect.flip)).code,
          "forbidden",
        );
        yield* memory.forget(
          { id: input.id, operationId: "forget-global-b", expectedRevision: 3 },
          b,
        );
        assert.equal((yield* memory.recall({ query: "Sharedglobalsecret" }, a)).entries.length, 0);
        assert.deepEqual(yield* memory.remember(input, a), remembered);
        assert.equal((yield* memory.recall({ query: "Sharedglobalsecret" }, b)).entries.length, 0);
      }),
  );
});
