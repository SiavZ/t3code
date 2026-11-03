import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
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
import * as SqlClient from "effect/sql/SqlClient";

import * as Sqlite from "../persistence/Sqlite.ts";
import * as MemoryService from "./MemoryService.ts";

const projectA = { projectId: ProjectId.make("project-a"), threadId: ThreadId.make("thread-a") };
const projectB = { projectId: ProjectId.make("project-b") };

const layer = it.layer(
  MemoryService.layer.pipe(
    Layer.provideMerge(Sqlite.layerMemory),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  ),
);

layer("MemoryService", (it) => {
  it.effect(
    "remembers, searches, lists and forgets within one project, including after restart",
    () =>
      Effect.gen(function* () {
        const memory = yield* MemoryService.MemoryService;
        const note = yield* memory.remember(
          { category: "fact", content: "Run the e2e suite with vp run test:e2e." },
          projectA,
        );
        yield* memory.remember(
          { category: "preference", content: "The user wants commits split by concern." },
          projectA,
        );
        assert.strictEqual(note.projectId, projectA.projectId);
        assert.strictEqual(note.sourceThreadId, projectA.threadId);
        assert.deepEqual(
          (yield* memory.search({ query: "how do I run E2E tests" }, projectA)).entries.map(
            (entry) => entry.id,
          ),
          [note.id],
        );
        assert.deepEqual(
          (yield* memory.search(
            { query: "e2e commits", category: "preference" },
            projectA,
          )).entries.map((entry) => entry.category),
          ["preference"],
        );
        const restarted = Context.get(
          yield* Layer.build(MemoryService.layer),
          MemoryService.MemoryService,
        );
        assert.strictEqual((yield* restarted.list(projectA)).entries.length, 2);
        assert.isTrue(yield* restarted.forget({ id: note.id }, projectA));
        assert.deepEqual((yield* memory.search({ query: "e2e" }, projectA)).entries, []);
        assert.isFalse(yield* memory.forget({ id: note.id }, projectA));
      }).pipe(Effect.scoped),
  );

  it.effect("never lets one project read or delete another project's entries", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const note = yield* memory.remember(
        { category: "decision", content: "isolation-marker: project A chose Postgres." },
        projectA,
      );
      assert.deepEqual((yield* memory.search({ query: "isolation-marker" }, projectB)).entries, []);
      assert.deepEqual((yield* memory.list(projectB)).entries, []);
      assert.isFalse(yield* memory.forget({ id: note.id }, projectB));
      assert.strictEqual(
        (yield* memory.search({ query: "isolation-marker" }, projectA)).entries.length,
        1,
      );
    }),
  );

  it.effect("ranks by matched terms and bounds results by limit and character budget", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const project = { projectId: ProjectId.make("project-budget") };
      const both = yield* memory.remember(
        { category: "fact", content: "budget-alpha budget-beta together" },
        project,
      );
      yield* memory.remember({ category: "fact", content: "budget-alpha only" }, project);
      assert.strictEqual(
        (yield* memory.search({ query: "budget-alpha budget-beta" }, project)).entries[0]?.id,
        both.id,
      );
      const limited = yield* memory.search({ query: "budget-alpha", limit: 1 }, project);
      assert.strictEqual(limited.entries.length, 1);
      assert.isTrue(limited.truncated);
      for (let index = 0; index < 6; index++) {
        yield* memory.remember(
          { category: "fact", content: "budget-large ".repeat(300).trim() },
          project,
        );
      }
      const budgeted = yield* memory.search({ query: "budget-large", limit: 20 }, project);
      assert.isAtMost(
        budgeted.entries.reduce((sum, entry) => sum + entry.content.length, 0),
        MEMORY_RESULT_CHARACTER_BUDGET,
      );
      assert.isBelow(budgeted.entries.length, 6);
      assert.isTrue(budgeted.truncated);
    }),
  );

  it.effect("refuses to grow a project past its cap, and forgetting makes room", () =>
    Effect.gen(function* () {
      const memory = yield* MemoryService.MemoryService;
      const project = { projectId: ProjectId.make("project-full") };
      let first = MemoryEntryId.make("missing");
      for (let index = 0; index < MemoryService.MAX_MEMORY_ENTRIES_PER_PROJECT; index++) {
        const entry = yield* memory.remember(
          { category: "fact", content: `note ${index}` },
          project,
        );
        if (index === 0) first = entry.id;
      }
      assert.strictEqual(
        (yield* memory
          .remember({ category: "fact", content: "one too many" }, project)
          .pipe(Effect.flip))._tag,
        "MemoryProjectFullError",
      );
      yield* memory.remember({ category: "fact", content: "still fine" }, projectB);
      yield* memory.forget({ id: first }, project);
      yield* memory.remember({ category: "fact", content: "replacement" }, project);
      assert.strictEqual(
        (yield* memory.list(project)).entries.length,
        MemoryService.MAX_MEMORY_ENTRIES_PER_PROJECT,
      );
    }),
  );

  it.effect(
    "recalls one-character names only as whole words and ignores punctuation-only queries",
    () =>
      Effect.gen(function* () {
        const memory = yield* MemoryService.MemoryService;
        const project = { projectId: ProjectId.make("project-short") };
        const r = yield* memory.remember(
          { category: "fact", content: "Analysis scripts are written in R" },
          project,
        );
        yield* memory.remember({ category: "fact", content: "Every error is logged" }, project);
        assert.deepEqual(
          (yield* memory.search({ query: "R" }, project)).entries.map((entry) => entry.id),
          [r.id],
        );
        assert.deepEqual(yield* memory.search({ query: "? !" }, project), {
          entries: [],
          truncated: false,
        });
        assert.isFalse(yield* memory.forget({ id: MemoryEntryId.make("mem_missing") }, project));
      }),
  );

  it.effect(
    "provisions its own table outside the migration ledger, so upstream ids stay free",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const tables = yield* sql<{
          readonly name: string;
        }>`SELECT name FROM sqlite_master WHERE name = 'agent_memory_entries'`;
        assert.strictEqual(tables.length, 1);
        const ledger = yield* sql<{
          readonly name: string;
        }>`SELECT name FROM effect_sql_migrations WHERE name LIKE '%Memory%'`;
        assert.deepEqual(ledger, []);
      }),
  );
});
