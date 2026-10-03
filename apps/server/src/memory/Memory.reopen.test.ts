import { assert, it } from "@effect/vitest";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId } from "@t3tools/contracts";
import migrateMemory from "../persistence/Migrations/058_Memory.ts";
import * as Memory from "./Memory.ts";

it.effect(
  "memory survives closing and reopening a real SQLite database without retaining forgotten bodies",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "t3-memory-reopen-"))),
        (dir) => Effect.promise(() => NodeFSP.rm(dir, { recursive: true, force: true })),
      );
      const filename = NodePath.join(directory, "state.sqlite");
      const services = () =>
        Memory.layer.pipe(Layer.provideMerge(NodeSqliteClient.layer({ filename })));
      const authority = { projectId: ProjectId.make("persistent-project"), allowGlobal: false };
      yield* Effect.gen(function* () {
        yield* migrateMemory;
        const memory = yield* Memory.MemoryService;
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
      }).pipe(Effect.provide(services()), Effect.scoped);
      yield* Effect.gen(function* () {
        const memory = yield* Memory.MemoryService;
        assert.equal(
          (yield* memory.recall({ query: "focused" }, authority)).entries[0]?.content,
          "Keep tests focused",
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
