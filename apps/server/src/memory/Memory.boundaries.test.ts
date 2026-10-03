import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId } from "@t3tools/contracts";
import migrate from "../persistence/Migrations/058_Memory.ts";
import * as Memory from "./Memory.ts";
const services = Memory.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const project = { projectId: ProjectId.make("bounded-project"), allowGlobal: false };
const global = { ...project, allowGlobal: true };
it.layer(services)("Memory boundaries", (it) => {
  it.effect(
    "bounded results and links never expose global entries after authority revocation",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const memory = yield* Memory.MemoryService;
        yield* memory.remember(
          {
            id: "global",
            operationId: "global",
            scope: "global",
            category: "fact",
            content: "global approved memory",
            tags: [],
          },
          global,
        );
        for (let n = 0; n < 25; n++)
          yield* memory.remember(
            {
              id: `p${n}`,
              operationId: `r${n}`,
              scope: "project",
              category: "fact",
              content: "project approved memory",
              tags: [],
            },
            project,
          );
        const page = yield* memory.search({ query: "approved", limit: 20 }, global);
        assert.equal(page.entries.length, 20);
        assert.equal(page.truncated, true);
        const revoked = yield* memory.search({ query: "global" }, project);
        assert.equal(revoked.entries.length, 0);
        assert.equal(
          (yield* memory
            .tag(
              { id: "global", operationId: "revoke-tag", expectedRevision: 1, tags: [] },
              project,
            )
            .pipe(Effect.flip)).code,
          "forbidden",
        );
        assert.equal(
          (yield* memory
            .link(
              {
                id: "p0",
                toId: "global",
                operationId: "cross-store",
                expectedRevision: 1,
                relation: "related",
              },
              global,
            )
            .pipe(Effect.flip)).code,
          "invalid",
        );
        assert.equal((yield* memory.related({ id: "p0", limit: 20 }, project)).entries.length, 0);
        assert.equal(
          (yield* memory.search({ query: "", limit: 21 }, project).pipe(Effect.flip)).code,
          "invalid",
        );
        assert.equal(
          (yield* memory
            .recallForTurn({ query: "approved", budget: Number.NaN }, project)
            .pipe(Effect.flip)).code,
          "invalid",
        );
        assert.equal(
          (yield* memory.recallForTurn({ query: "!!!", budget: 16000 }, global)).entries.length,
          0,
        );
        const context = yield* memory.recallForTurn({ query: "approved", budget: 200 }, project);
        assert.ok(context.text.length <= 200);
        assert.ok(context.text.includes("lexical-context"));
      }),
  );
  it.effect(
    "idempotency metadata does not allow unrelated mutations or forgotten content resurrection",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const memory = yield* Memory.MemoryService;
        yield* memory.remember(
          {
            id: "identity",
            operationId: "remember-identity",
            scope: "project",
            category: "correction",
            content: "Explicit correction",
            tags: [],
          },
          project,
        );
        yield* memory.tag(
          { id: "identity", operationId: "tag-identity", expectedRevision: 1, tags: ["corrected"] },
          project,
        );
        assert.equal(
          (yield* memory
            .forget({ id: "identity", operationId: "tag-identity", expectedRevision: 1 }, project)
            .pipe(Effect.flip)).code,
          "conflict",
        );
        yield* memory.forget(
          { id: "identity", operationId: "forget-identity", expectedRevision: 2 },
          project,
        );
        yield* memory.remember(
          {
            id: "identity",
            operationId: "remember-identity",
            scope: "project",
            category: "correction",
            content: "Explicit correction",
            tags: [],
          },
          project,
        );
        assert.equal((yield* memory.search({ query: "correction" }, project)).entries.length, 0);
      }),
  );
});
