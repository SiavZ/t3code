import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  MEMORY_RESULT_CHARACTER_BUDGET,
  MemoryEntryId,
  ProjectId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import * as MemoryService from "./MemoryService.ts";

const projectA = { projectId: ProjectId.make("project-a"), threadId: ThreadId.make("thread-a") };
const projectB = { projectId: ProjectId.make("project-b") };

const layer = it.layer(
  MemoryService.layer.pipe(
    Layer.provideMerge(SqlitePersistenceMemory),
    Layer.provideMerge(NodeServices.layer),
  ),
);

layer("MemoryService", (it) => {
  it.effect("creates its own table outside the migration ledger, and restarts cleanly", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const tables = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master WHERE name = 'agent_memory_entries'
      `;
      assert.strictEqual(tables.length, 1);
      const ledger = yield* sql<{ readonly name: string }>`
        SELECT name FROM effect_sql_migrations WHERE name LIKE '%Memory%'
      `;
      assert.deepEqual(ledger, []);
      // A second service start against the same database keeps existing entries.
      const project = { projectId: ProjectId.make("project-restart") };
      yield* MemoryService.MemoryService.pipe(
        Effect.flatMap((memory) =>
          memory.remember({ category: "fact", content: "survives restart" }, project),
        ),
      );
      const restarted = yield* Layer.build(MemoryService.layer).pipe(
        Effect.map((context) => Context.get(context, MemoryService.MemoryService)),
      );
      assert.strictEqual(
        (yield* restarted.search({ query: "survives restart" }, project)).entries.length,
        1,
      );
    }).pipe(Effect.scoped),
  );

  it.effect("remembers, searches, lists and forgets within one project", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const e2e = yield* memory.remember(
        { category: "fact", content: "Run the e2e suite with `vp run test:e2e --serial`." },
        projectA,
      );
      yield* memory.remember(
        { category: "preference", content: "The user wants commits split by concern." },
        projectA,
      );
      assert.strictEqual(e2e.projectId, projectA.projectId);
      assert.strictEqual(e2e.sourceThreadId, projectA.threadId);

      const found = yield* memory.search({ query: "how do I run E2E tests" }, projectA);
      assert.deepEqual(
        found.entries.map((entry) => entry.id),
        [e2e.id],
      );

      const preferences = yield* memory.search(
        { query: "e2e commits", category: "preference" },
        projectA,
      );
      assert.deepEqual(
        preferences.entries.map((entry) => entry.category),
        ["preference"],
      );

      assert.strictEqual((yield* memory.list(projectA)).entries.length, 2);
      assert.isTrue(yield* memory.forget({ id: e2e.id }, projectA));
      assert.deepEqual((yield* memory.search({ query: "e2e" }, projectA)).entries, []);
      assert.isFalse(yield* memory.forget({ id: e2e.id }, projectA));
    }),
  );

  it.effect("never lets one project read or delete another project's entries", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const secret = yield* memory.remember(
        { category: "decision", content: "isolation-marker: project A chose Postgres." },
        projectA,
      );
      assert.deepEqual((yield* memory.search({ query: "isolation-marker" }, projectB)).entries, []);
      assert.isFalse(
        (yield* memory.list(projectB)).entries.some((entry) => entry.id === secret.id),
      );
      assert.isFalse(yield* memory.forget({ id: secret.id }, projectB));
      assert.strictEqual(
        (yield* memory.search({ query: "isolation-marker" }, projectA)).entries.length,
        1,
      );
    }),
  );

  it.effect("ranks by matched terms and keeps results within the limit and character budget", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const project = { projectId: ProjectId.make("project-budget") };
      const both = yield* memory.remember(
        { category: "fact", content: "budget-alpha budget-beta together" },
        project,
      );
      yield* memory.remember({ category: "fact", content: "budget-alpha only" }, project);
      const ranked = yield* memory.search({ query: "budget-alpha budget-beta" }, project);
      assert.strictEqual(ranked.entries[0]?.id, both.id);

      const limited = yield* memory.search({ query: "budget-alpha", limit: 1 }, project);
      assert.strictEqual(limited.entries.length, 1);
      assert.isTrue(limited.truncated);

      const large = "budget-large ".repeat(300).trim();
      for (let index = 0; index < 6; index++) {
        yield* memory.remember({ category: "fact", content: large }, project);
      }
      const budgeted = yield* memory.search({ query: "budget-large", limit: 20 }, project);
      const characters = budgeted.entries.reduce((sum, entry) => sum + entry.content.length, 0);
      assert.isAtMost(characters, MEMORY_RESULT_CHARACTER_BUDGET);
      assert.isBelow(budgeted.entries.length, 6);
      assert.isTrue(budgeted.truncated);
    }),
  );

  it.effect("refuses to grow a project past its entry cap", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const project = { projectId: ProjectId.make("project-full") };
      for (let index = 0; index < MemoryService.MAX_MEMORY_ENTRIES_PER_PROJECT; index++) {
        yield* memory.remember({ category: "fact", content: `note ${index}` }, project);
      }
      const full = yield* memory
        .remember({ category: "fact", content: "one too many" }, project)
        .pipe(Effect.flip);
      assert.strictEqual(full._tag, "MemoryProjectFullError");
      // Another project is unaffected.
      yield* memory.remember(
        { category: "fact", content: "still fine" },
        { projectId: ProjectId.make("project-other") },
      );
    }),
  );

  it.effect("returns nothing for a query without searchable words", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      yield* memory.remember({ category: "fact", content: "a b c" }, projectA);
      assert.deepEqual(yield* memory.search({ query: "? !" }, projectA), {
        entries: [],
        truncated: false,
      });
      assert.isFalse(yield* memory.forget({ id: MemoryEntryId.make("mem_missing") }, projectA));
    }),
  );

  it.effect("recalls a one-character name as a whole word, not inside other words", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const project = { projectId: ProjectId.make("project-short") };
      const r = yield* memory.remember(
        { category: "fact", content: "Analysis scripts are written in R" },
        project,
      );
      yield* memory.remember({ category: "fact", content: "Every error is logged" }, project);
      const found = yield* memory.search({ query: "R" }, project);
      assert.deepEqual(
        found.entries.map((entry) => entry.id),
        [r.id],
      );
    }),
  );
});
