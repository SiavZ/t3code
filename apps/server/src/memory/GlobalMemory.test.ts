import { assert, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, ProjectId, type ServerSettings } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migrate from "../persistence/Migrations/058_Memory.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as Memory from "./Memory.ts";
import * as Global from "./GlobalMemory.ts";
const services = Memory.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const admin = { humanSessionId: "human-a", admin: true };
const input = {
  id: "global-live",
  operationId: "remember-live",
  category: "fact" as const,
  content: "Globaltoken explicit memory",
  tags: [],
};
const withSettings = (settings: Ref.Ref<ServerSettings>) =>
  Global.layer.pipe(
    Layer.provide(Layer.mock(ServerSettingsService)({ getSettings: Ref.get(settings) })),
  );
it.layer(services)("GlobalMemory", (it) => {
  it.effect(
    "fresh default-off/admin gates deny retries and reads immediately after revocation",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const settings = yield* Ref.make<ServerSettings>(DEFAULT_SERVER_SETTINGS);
        yield* Effect.gen(function* () {
          const global = yield* Global.GlobalMemory;
          const request = { operation: "remember" as const, input };
          assert.equal((yield* global.write(request, admin).pipe(Effect.flip)).code, "forbidden");
          yield* Ref.update(settings, (value) => ({ ...value, enableGlobalMemory: true }));
          assert.equal(
            (yield* global.write(request, { ...admin, admin: false }).pipe(Effect.flip)).code,
            "forbidden",
          );
          assert.equal(
            (yield* global.write(request, { ...admin, humanSessionId: "" }).pipe(Effect.flip)).code,
            "forbidden",
          );
          const saved = yield* global.write(request, admin);
          assert.deepEqual(yield* global.write(request, admin), saved);
          assert.equal(
            (yield* global
              .write({ ...request, input: { ...input, content: "changed" } }, admin)
              .pipe(Effect.flip)).code,
            "conflict",
          );
          assert.equal(
            (yield* global
              .write(request, { ...admin, humanSessionId: "human-b" })
              .pipe(Effect.flip)).code,
            "conflict",
          );
          const read = { operation: "recall" as const, input: { query: "Globaltoken" } };
          assert.equal(
            (yield* global.read(read, { ...admin, humanSessionId: "human-b" })).entries[0]?.scope,
            "global",
          );
          assert.equal(
            (yield* global.read(read, { ...admin, admin: false }).pipe(Effect.flip)).code,
            "forbidden",
          );
          yield* Ref.update(settings, (value) => ({ ...value, enableGlobalMemory: false }));
          assert.equal((yield* global.read(read, admin).pipe(Effect.flip)).code, "forbidden");
          assert.equal((yield* global.write(request, admin).pipe(Effect.flip)).code, "forbidden");
          assert.equal(
            (yield* global
              .write(
                {
                  operation: "forget",
                  input: { id: input.id, operationId: "revoked-forget", expectedRevision: 1 },
                },
                admin,
              )
              .pipe(Effect.flip)).code,
            "forbidden",
          );
        }).pipe(Effect.provide(withSettings(settings)));
      }),
  );
  it.effect(
    "forces global namespace and rejects local id operations and forged wire authority",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const memory = yield* Memory.MemoryService;
        const localAuthority = {
          projectId: ProjectId.make("environment-global-memory"),
          allowGlobal: false,
        };
        const local = {
          ...input,
          id: "local-global-anchor",
          operationId: "local-anchor",
          scope: "project" as const,
        };
        yield* memory.remember(local, localAuthority);
        const settings = yield* Ref.make<ServerSettings>({
          ...DEFAULT_SERVER_SETTINGS,
          enableGlobalMemory: true,
        });
        yield* Effect.gen(function* () {
          const global = yield* Global.GlobalMemory;
          const forged = {
            operation: "remember" as const,
            input: {
              ...input,
              id: "forced-global",
              operationId: "forced-global",
              scope: "project",
              sourceMessageId: "forged-provenance",
              allowGlobal: true,
            },
          };
          yield* global.write(forged, admin);
          const forgedRead = {
            operation: "search" as const,
            input: { query: "Globaltoken", scope: "project", limit: 20 },
          };
          const page = yield* global.read(forgedRead, admin);
          assert.ok(
            page.entries.every(
              (entry) =>
                entry.scope === "global" &&
                entry.projectId === null &&
                entry.sourceThreadId === null &&
                entry.sourceMessageId === null,
            ),
          );
          assert.ok(!page.entries.some((entry) => entry.id === local.id));
          assert.equal(
            (yield* memory.recall({ query: "Globaltoken" }, localAuthority)).entries.filter(
              (entry) => entry.scope === "global",
            ).length,
            0,
          );
          assert.equal(
            (yield* global
              .read({ operation: "related", input: { id: local.id } }, admin)
              .pipe(Effect.flip)).code,
            "forbidden",
          );
          assert.equal(
            (yield* global
              .write(
                {
                  operation: "tag",
                  input: {
                    id: local.id,
                    operationId: "global-local-tag",
                    expectedRevision: 1,
                    tags: [],
                  },
                },
                admin,
              )
              .pipe(Effect.flip)).code,
            "forbidden",
          );
          assert.equal(
            (yield* global
              .write(
                {
                  operation: "forget",
                  input: { id: local.id, operationId: "global-local-forget", expectedRevision: 1 },
                },
                admin,
              )
              .pipe(Effect.flip)).code,
            "forbidden",
          );
          assert.equal(
            (yield* global
              .write(
                {
                  operation: "link",
                  input: {
                    id: "forced-global",
                    operationId: "global-local-link",
                    expectedRevision: 1,
                    toId: local.id,
                    relation: "related",
                  },
                },
                admin,
              )
              .pipe(Effect.flip)).code,
            "forbidden",
          );
          assert.equal(
            (yield* global
              .read({ operation: "search", input: { query: "Globaltoken", limit: 21 } }, admin)
              .pipe(Effect.flip)).code,
            "invalid",
          );
          yield* global.write(
            {
              operation: "remember",
              input: { ...input, id: "global-target", operationId: "global-target" },
            },
            admin,
          );
          yield* global.write(
            {
              operation: "link",
              input: {
                id: "forced-global",
                operationId: "global-link",
                expectedRevision: 1,
                toId: "global-target",
                relation: "supports",
              },
            },
            admin,
          );
          assert.equal(
            (yield* global.read({ operation: "related", input: { id: "forced-global" } }, admin))
              .entries[0]?.id,
            "global-target",
          );
          yield* global.write(
            {
              operation: "tag",
              input: {
                id: "forced-global",
                operationId: "global-tag",
                expectedRevision: 2,
                tags: ["global"],
              },
            },
            admin,
          );
          yield* global.write(
            {
              operation: "forget",
              input: { id: "forced-global", operationId: "global-forget", expectedRevision: 3 },
            },
            admin,
          );
          yield* global.write(forged, admin);
          assert.ok(
            !(yield* global.read(
              { operation: "search", input: { query: "Globaltoken" } },
              admin,
            )).entries.some((entry) => entry.id === "forced-global"),
          );
        }).pipe(Effect.provide(withSettings(settings)));
      }),
  );
});
