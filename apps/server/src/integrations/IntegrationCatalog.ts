import { Context, Effect, Layer, Schema } from "effect";
import { makeCurrentCheck } from "./IntegrationConfigurationGuard.ts";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as Attempts from "./WorkflowAttempts.ts";
import * as NodeCrypto from "node:crypto";
const { randomUUID } = NodeCrypto;
import {
  CatalogProduct,
  CatalogSearchResult,
  CatalogSelection as CatalogSelectionSchema,
  type CatalogSearch,
  type CatalogSelection,
} from "../../../../packages/contracts/src/integrationWorkflows.ts";

export class CatalogError extends Schema.TaggedError<CatalogError>()("CatalogError", {
  reason: Schema.Literals([
    "unconfigured",
    "disabled",
    "expired-request",
    "invalid-response",
    "approval-required",
    "transport",
  ]),
  cause: Schema.optional(Schema.Defect()),
}) {}

/** A configured protocol adapter owns endpoint mapping. No catalog URL is assumed. */
export class CatalogSource extends Context.Service<
  CatalogSource,
  {
    readonly sourceId: string;
    readonly enabled: boolean;
    readonly search: (query: CatalogSearch) => Effect.Effect<unknown, CatalogError>;
    readonly details: (productId: string, revision: string) => Effect.Effect<unknown, CatalogError>;
    readonly suggest: (
      payload: string,
      idempotencyKey: string,
    ) => Effect.Effect<void, CatalogError>;
  }
>()("t3/integrations/CatalogSource") {}

/** Trusted UI grants are single use and bind the exact outbound payload. Never expose grant through MCP. */
export class CatalogDisclosureApproval extends Context.Service<
  CatalogDisclosureApproval,
  {
    readonly consume: (approvalId: string, payload: string) => Effect.Effect<void, CatalogError>;
  }
>()("t3/integrations/CatalogDisclosureApproval") {}

export class IntegrationCatalog extends Context.Service<
  IntegrationCatalog,
  {
    readonly status: () => Effect.Effect<{
      readonly sourceId: string;
      readonly state: "available" | "disabled" | "unconfigured";
    }>;
    readonly search: (
      query: CatalogSearch,
    ) => Effect.Effect<typeof CatalogSearchResult.Type, CatalogError>;
    readonly details: (
      requestId: string,
      productId: string,
    ) => Effect.Effect<typeof CatalogProduct.Type, CatalogError>;
    readonly select: (
      requestId: string,
      productId: string,
      reason: string,
    ) => Effect.Effect<CatalogSelection, CatalogError>;
    readonly selections: () => Effect.Effect<ReadonlyArray<CatalogSelection>, CatalogError>;
    readonly clearSelection: (selectionId: string) => Effect.Effect<void, CatalogError>;
    readonly selectOffCatalog: (
      productId: string,
      publicUrl: string,
      reason: string,
    ) => Effect.Effect<CatalogSelection, CatalogError>;
    readonly suggest: (
      payload: string,
      approvalId: string,
      idempotencyKey: string,
    ) => Effect.Effect<void, CatalogError>;
  }
>()("t3/integrations/IntegrationCatalog") {}

const make = Effect.gen(function* () {
  const source = yield* CatalogSource;
  const current = yield* makeCurrentCheck("catalog");
  const approvals = yield* CatalogDisclosureApproval;
  const sql = yield* SqlClient.SqlClient;
  const attempts = yield* Attempts.WorkflowAttempts;
  const searches = new Map<string, typeof CatalogSearchResult.Type>();
  const enabled = () =>
    current().pipe(
      Effect.flatMap((unchanged) =>
        !unchanged
          ? Effect.fail(new CatalogError({ reason: "disabled" }))
          : source.sourceId
            ? source.enabled
              ? Effect.void
              : Effect.fail(new CatalogError({ reason: "disabled" }))
            : Effect.fail(new CatalogError({ reason: "unconfigured" })),
      ),
    );
  const boundProduct = (requestId: string, productId: string) =>
    Effect.gen(function* () {
      const request = searches.get(requestId);
      const product = request?.products.find((product) => product.productId === productId);
      if (!request || !product || Date.now() - request.fetchedAt > 300_000)
        return yield* Effect.fail(new CatalogError({ reason: "expired-request" }));
      return product;
    });
  const search = (query: CatalogSearch) =>
    Effect.gen(function* () {
      yield* enabled();
      const raw = yield* source.search(query);
      const products = yield* Schema.decodeUnknownEffect(Schema.Array(CatalogProduct))(raw).pipe(
        Effect.mapError((cause) => new CatalogError({ reason: "invalid-response", cause })),
      );
      if (
        products.length > 100 ||
        JSON.stringify(products).length > 512_000 ||
        products.some((p) => !p.publicUrl.startsWith("https://"))
      )
        return yield* Effect.fail(new CatalogError({ reason: "invalid-response" }));
      const result = {
        requestId: randomUUID(),
        sourceId: source.sourceId,
        fetchedAt: Date.now(),
        products,
      };
      if (searches.size >= 100) searches.delete(searches.keys().next().value!);
      searches.set(result.requestId, result);
      return result;
    });
  return IntegrationCatalog.of({
    status: () =>
      current().pipe(
        Effect.map((unchanged) => ({
          sourceId: source.sourceId,
          state: !source.sourceId
            ? ("unconfigured" as const)
            : unchanged && source.enabled
              ? ("available" as const)
              : ("disabled" as const),
        })),
      ),
    search,
    details: (requestId, productId) =>
      Effect.gen(function* () {
        yield* enabled();
        const bound = yield* boundProduct(requestId, productId);
        const raw = yield* source.details(productId, bound.revision);
        const detail = yield* Schema.decodeUnknownEffect(CatalogProduct)(raw).pipe(
          Effect.mapError((cause) => new CatalogError({ reason: "invalid-response", cause })),
        );
        if (
          JSON.stringify(detail).length > 512_000 ||
          detail.productId !== bound.productId ||
          detail.revision !== bound.revision ||
          detail.publicUrl !== bound.publicUrl
        )
          return yield* Effect.fail(new CatalogError({ reason: "invalid-response" }));
        return detail;
      }),
    select: (requestId, productId, reason) =>
      Effect.gen(function* () {
        yield* enabled();
        const product = yield* boundProduct(requestId, productId);
        const selection = {
          selectionId: randomUUID(),
          sourceId: source.sourceId,
          productId,
          revision: product.revision,
          publicUrl: product.publicUrl,
          requestId,
          reason,
          selectedAt: Date.now(),
        };
        yield* sql`INSERT INTO integration_selections (selection_id, selection_json) VALUES (${selection.selectionId}, ${JSON.stringify(selection)})`.pipe(
          Effect.mapError(() => new CatalogError({ reason: "transport" })),
        );
        return selection;
      }),
    selections: () =>
      sql<{
        selection_json: string;
      }>`SELECT selection_json FROM integration_selections ORDER BY selection_id LIMIT 1000`.pipe(
        Effect.flatMap((rows) =>
          Effect.forEach(rows, (row) =>
            Effect.try({
              try: () => JSON.parse(row.selection_json) as unknown,
              catch: () => new CatalogError({ reason: "invalid-response" }),
            }).pipe(Effect.flatMap(Schema.decodeUnknownEffect(CatalogSelectionSchema))),
          ),
        ),
        Effect.mapError(() => new CatalogError({ reason: "invalid-response" })),
      ),
    clearSelection: (id) =>
      sql`DELETE FROM integration_selections WHERE selection_id = ${id}`.pipe(
        Effect.asVoid,
        Effect.mapError(() => new CatalogError({ reason: "transport" })),
      ),
    selectOffCatalog: (productId, publicUrl, reason) =>
      Effect.gen(function* () {
        if (!productId || !publicUrl.startsWith("https://"))
          return yield* Effect.fail(new CatalogError({ reason: "invalid-response" }));
        const selection = {
          selectionId: randomUUID(),
          sourceId: "off-catalog",
          productId,
          publicUrl,
          revision: "",
          requestId: "",
          reason,
          selectedAt: Date.now(),
        };
        yield* sql`INSERT INTO integration_selections (selection_id, selection_json) VALUES (${selection.selectionId}, ${JSON.stringify(selection)})`.pipe(
          Effect.mapError(() => new CatalogError({ reason: "transport" })),
        );
        return selection;
      }),
    suggest: (payload, approvalId, key) =>
      Effect.gen(function* () {
        yield* enabled();
        yield* approvals.consume(approvalId, payload);
        yield* attempts
          .claim(key, "catalog.suggest", payload)
          .pipe(Effect.mapError(() => new CatalogError({ reason: "approval-required" })));
        yield* source.suggest(payload, key);
        yield* attempts
          .settle(key, "completed")
          .pipe(Effect.mapError(() => new CatalogError({ reason: "transport" })));
      }),
  });
});
export const layer = Layer.effect(IntegrationCatalog, make);
