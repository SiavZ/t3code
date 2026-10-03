import { Context, Effect, Layer } from "effect";
import * as Catalog from "./IntegrationCatalog.ts";

/** Routes are supplied by the operator from the selected backend's documented protocol. */
export class CatalogHttpConfiguration extends Context.Service<
  CatalogHttpConfiguration,
  {
    readonly sourceId: string;
    readonly enabled: boolean;
    readonly searchUrl: string;
    readonly detailsUrl: string;
    readonly suggestUrl: string;
  }
>()("t3/integrations/CatalogHttpConfiguration") {}

export class CatalogHttpTransport extends Context.Service<
  CatalogHttpTransport,
  {
    readonly post: (
      url: string,
      body: unknown,
      idempotencyKey?: string,
    ) => Effect.Effect<unknown, Catalog.CatalogError>;
  }
>()("t3/integrations/CatalogHttpTransport") {}

/** Only opt in for sources speaking the explicitly configured JSON POST protocol. */
export const sourceLayer = Layer.effect(
  Catalog.CatalogSource,
  Effect.gen(function* () {
    const config = yield* CatalogHttpConfiguration;
    const http = yield* CatalogHttpTransport;
    return Catalog.CatalogSource.of({
      sourceId: config.sourceId,
      enabled: config.enabled,
      search: (query) => http.post(config.searchUrl, query),
      details: (productId, revision) => http.post(config.detailsUrl, { productId, revision }),
      suggest: (payload, key) => http.post(config.suggestUrl, { payload }, key).pipe(Effect.asVoid),
    });
  }),
);

export const transportLayer = Layer.succeed(
  CatalogHttpTransport,
  CatalogHttpTransport.of({
    post: (url, body, key) =>
      Effect.tryPromise({
        try: async (signal) => {
          const endpoint = new URL(url);
          if (endpoint.protocol !== "https:") throw new Error("Catalog endpoints require HTTPS");
          const response = await fetch(endpoint, {
            method: "POST",
            redirect: "error",
            signal,
            headers: {
              "content-type": "application/json",
              ...(key ? { "idempotency-key": key } : {}),
            },
            body: JSON.stringify(body),
          });
          if (!response.ok || !response.body) throw new Error("Catalog response rejected");
          const reader = response.body.getReader();
          const chunks: Uint8Array[] = [];
          let size = 0;
          try {
            for (;;) {
              const part = await reader.read();
              if (part.done) break;
              size += part.value.byteLength;
              if (size > 512_000) throw new Error("Catalog response too large");
              chunks.push(part.value);
            }
          } finally {
            await reader.cancel();
          }
          return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown;
        },
        catch: () => new Catalog.CatalogError({ reason: "transport" }),
      }).pipe(
        Effect.timeout("15 seconds"),
        Effect.mapError(() => new Catalog.CatalogError({ reason: "transport" })),
      ),
  }),
);
