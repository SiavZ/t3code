import { Context, Effect, Layer, Schema } from "effect";

export class IntegrationHttpError extends Schema.TaggedError<IntegrationHttpError>()(
  "IntegrationHttpError",
  {
    reason: Schema.Literals(["transport", "response", "too-large"]),
    status: Schema.optional(Schema.Number),
  },
) {}
export interface HttpRequest {
  readonly url: string;
  readonly method: "GET" | "POST" | "PUT" | "DELETE";
  readonly headers?: Readonly<Record<string, string>>;
  readonly body?: string;
  readonly maxBytes?: number;
  readonly timeoutMs?: number;
}
export class IntegrationHttp extends Context.Service<
  IntegrationHttp,
  {
    readonly request: (request: HttpRequest) => Effect.Effect<unknown, IntegrationHttpError>;
  }
>()("t3/integrations/IntegrationHttp") {}
export const layer = Layer.succeed(IntegrationHttp, {
  request: (input) =>
    Effect.tryPromise({
      try: async (signal) => {
        if (input.body && Buffer.byteLength(input.body) > 30 * 1024 * 1024)
          throw new IntegrationHttpError({ reason: "too-large" });
        if (new URL(input.url).protocol !== "https:")
          throw new IntegrationHttpError({ reason: "transport" });
        const response = await fetch(input.url, {
          method: input.method,
          ...(input.headers ? { headers: input.headers } : {}),
          ...(input.body === undefined ? {} : { body: input.body }),
          signal,
          redirect: "error",
        });
        if (!response.ok)
          throw new IntegrationHttpError({ reason: "response", status: response.status });
        if (!response.body) return {};
        const reader = response.body.getReader();
        let length = 0;
        const chunks: Uint8Array[] = [];
        try {
          for (;;) {
            const next = await reader.read();
            if (next.done) break;
            length += next.value.byteLength;
            if (length > (input.maxBytes ?? 2_000_000))
              throw new IntegrationHttpError({ reason: "too-large" });
            chunks.push(next.value);
          }
        } finally {
          await reader.cancel();
        }
        const text = Buffer.concat(chunks).toString("utf8");
        return text ? (JSON.parse(text) as unknown) : {};
      },
      catch: (error) =>
        error instanceof IntegrationHttpError
          ? error
          : new IntegrationHttpError({ reason: "transport" }),
    }).pipe(
      Effect.timeout(Math.min(720_000, Math.max(1, input.timeoutMs ?? 60_000))),
      Effect.mapError((error) =>
        error instanceof IntegrationHttpError
          ? error
          : new IntegrationHttpError({ reason: "transport" }),
      ),
    ),
});
