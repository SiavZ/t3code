import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Catalog from "./IntegrationCatalog.ts";
import * as Http from "./CatalogHttp.ts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migration from "../persistence/Migrations/065_IntegrationWorkflows.ts";
import * as Attempts from "./WorkflowAttempts.ts";

const product = {
  productId: "mail",
  name: "Mail",
  publicUrl: "https://example.org/mail",
  capabilities: ["email"],
  setupInstructions: "Review configuration",
  authRequirements: ["OAuth"],
  revision: "r1",
};
const query = { query: "mail", category: "email", requirements: [] };
const fixture = (detail: unknown = product) => {
  const messages: { url: string; body: unknown; key?: string }[] = [];
  const transport = Layer.succeed(
    Http.CatalogHttpTransport,
    Http.CatalogHttpTransport.of({
      post: (url, body, key) =>
        Effect.sync(() => {
          messages.push({ url, body, ...(key ? { key } : {}) });
          return url.endsWith("search") ? [product] : url.endsWith("details") ? detail : {};
        }),
    }),
  );
  const config = Layer.succeed(Http.CatalogHttpConfiguration, {
    sourceId: "fixture",
    enabled: true,
    searchUrl: "https://catalog.test/search",
    detailsUrl: "https://catalog.test/details",
    suggestUrl: "https://catalog.test/suggest",
  });
  const approvals = Layer.succeed(Catalog.CatalogDisclosureApproval, {
    consume: (id: string, payload: string) =>
      id === "human" && payload === "preview"
        ? Effect.void
        : Effect.fail(new Catalog.CatalogError({ reason: "approval-required" })),
  });
  const source = Http.sourceLayer.pipe(Layer.provide(Layer.merge(config, transport)));
  const database = Layer.effectDiscard(migration).pipe(
    Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
  );
  const dependencies = Layer.mergeAll(
    source,
    approvals,
    Attempts.layer.pipe(Layer.provide(database)),
    database,
  );
  return { messages, layer: Catalog.layer.pipe(Layer.provide(dependencies)) };
};

describe("IntegrationCatalog", () => {
  it.effect(
    "dispatches configured HTTP messages and binds selection provenance without installation",
    () =>
      Effect.gen(function* () {
        const test = fixture();
        const result = yield* Effect.gen(function* () {
          const catalog = yield* Catalog.IntegrationCatalog;
          const found = yield* catalog.search(query);
          yield* catalog.details(found.requestId, "mail");
          const selection = yield* catalog.select(found.requestId, "mail", "Chosen for mail");
          yield* catalog.clearSelection(selection.selectionId);
          return { selection, remaining: yield* catalog.selections() };
        }).pipe(Effect.provide(test.layer));
        expect(result.selection).toMatchObject({
          sourceId: "fixture",
          productId: "mail",
          revision: "r1",
        });
        expect(result.remaining).toEqual([]);
        expect(test.messages).toEqual([
          { url: "https://catalog.test/search", body: query },
          { url: "https://catalog.test/details", body: { productId: "mail", revision: "r1" } },
        ]);
      }),
  );
  it.effect("rejects foreign requests and changed catalog detail provenance", () =>
    Effect.gen(function* () {
      const test = fixture({ ...product, revision: "r2" });
      const results = yield* Effect.gen(function* () {
        const catalog = yield* Catalog.IntegrationCatalog;
        const foreign = yield* catalog.details("foreign", "mail").pipe(Effect.result);
        const found = yield* catalog.search(query);
        const changed = yield* catalog.details(found.requestId, "mail").pipe(Effect.result);
        return { foreign, changed };
      }).pipe(Effect.provide(test.layer));
      expect(results.foreign._tag).toBe("Failure");
      expect(results.changed._tag).toBe("Failure");
      expect(test.messages).toHaveLength(2);
    }),
  );
  it.effect(
    "requires exact human disclosure approval and avoids duplicate outbound suggestion",
    () =>
      Effect.gen(function* () {
        const test = fixture();
        yield* Effect.gen(function* () {
          const catalog = yield* Catalog.IntegrationCatalog;
          const denied = yield* catalog
            .suggest("preview", "agent-confirmed", "key")
            .pipe(Effect.result);
          expect(denied._tag).toBe("Failure");
          yield* catalog.suggest("preview", "human", "key");
          const duplicate = yield* catalog.suggest("preview", "human", "key").pipe(Effect.result);
          expect(duplicate._tag).toBe("Failure");
        }).pipe(Effect.provide(test.layer));
        expect(test.messages).toEqual([
          { url: "https://catalog.test/suggest", body: { payload: "preview" }, key: "key" },
        ]);
      }),
  );
});
