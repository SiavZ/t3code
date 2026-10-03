import { assert, it } from "@effect/vitest";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import migrateMemory from "../persistence/Migrations/057_Memory.ts";
import migrateQuality from "../persistence/Migrations/058_QualityRecords.ts";
import * as Memory from "./Memory.ts";
import * as Quality from "../orchestration/QualityRecords.ts";

it.effect(
  "memory and quality survive closing and reopening a real SQLite database without retaining forgotten bodies",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-memory-reopen-"))),
        (dir) => Effect.promise(() => NodeFSP.rm(dir, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      const services = () =>
        Layer.merge(Memory.layer, Quality.layer).pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename })),
        );
      const authority = { projectId: ProjectId.make("persistent-project"), allowGlobal: false };
      const threadId = ThreadId.make("persistent-thread");
      const qualityAuthority = { threadId, source: "agent-reported" as const };
      yield* Effect.gen(function* () {
        yield* migrateMemory;
        yield* migrateQuality;
        const memory = yield* Memory.MemoryService;
        const quality = yield* Quality.QualityRecords;
        yield* memory.remember(
          {
            id: "persistent",
            operationId: "persist",
            scope: "project",
            category: "preference",
            content: "Keep tests focused",
            tags: ["tests"],
          },
          authority,
        );
        yield* quality.update(
          {
            threadId,
            operationId: "quality",
            expectedRevision: 0,
            intention: "Durable quality",
            todos: [],
          },
          qualityAuthority,
        );
      }).pipe(Effect.provide(services()), Effect.scoped);
      yield* Effect.gen(function* () {
        const memory = yield* Memory.MemoryService;
        const quality = yield* Quality.QualityRecords;
        assert.equal(
          (yield* memory.recall({ query: "focused" }, authority)).entries[0]?.content,
          "Keep tests focused",
        );
        assert.equal(
          (yield* quality.get({ threadId }, qualityAuthority))?.intention,
          "Durable quality",
        );
        yield* memory.forget(
          { id: "persistent", operationId: "forget", expectedRevision: 1 },
          authority,
        );
      }).pipe(Effect.provide(services()), Effect.scoped);
      yield* Effect.gen(function* () {
        const memory = yield* Memory.MemoryService;
        const sql = yield* SqlClient.SqlClient;
        assert.equal((yield* memory.recall({ query: "focused" }, authority)).entries.length, 0);
        const retried = yield* memory.remember(
          {
            id: "persistent",
            operationId: "persist",
            scope: "project",
            category: "preference",
            content: "Keep tests focused",
            tags: ["tests"],
          },
          authority,
        );
        assert.equal(retried.status, "remembered");
        assert.equal((yield* memory.recall({ query: "focused" }, authority)).entries.length, 0);
        assert.equal(
          (yield* memory
            .remember(
              {
                id: "persistent",
                operationId: "persist",
                scope: "project",
                category: "preference",
                content: "Changed after reopen",
                tags: ["tests"],
              },
              authority,
            )
            .pipe(Effect.flip)).code,
          "conflict",
        );
        const rows = yield* sql`SELECT * FROM memory_operations`;
        assert.ok(!JSON.stringify(rows).includes("Keep tests focused"));
      }).pipe(Effect.provide(services()), Effect.scoped);
    }).pipe(Effect.scoped),
);
