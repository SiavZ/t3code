import { Context, Effect, Layer, Option, Schema } from "effect";
import { makeCurrentCheck } from "./IntegrationConfigurationGuard.ts";
import * as NodeCrypto from "node:crypto";
const { createHash } = NodeCrypto;
import * as NodeFSP from "node:fs/promises";
const { mkdir, readFile, writeFile, unlink } = NodeFSP;
import * as NodePath from "node:path";
const { join } = NodePath;
import { isIntegrationSecretRefFor } from "./IntegrationSecrets.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import * as Attempts from "./WorkflowAttempts.ts";
import type { ImageDeleteInput } from "@t3tools/contracts";
export class ImageError extends Schema.TaggedError<ImageError>()("ImageError", {
  reason: Schema.Literals([
    "unconfigured",
    "authentication-required",
    "approval-required",
    "invalid-response",
    "invalid-input",
    "already-attempted",
    "unknown-outcome",
    "asset-unavailable",
  ]),
}) {}
export class ImageConfiguration extends Context.Service<
  ImageConfiguration,
  {
    readonly enabled: boolean;
    readonly protocol: "openai-images-v1";
    readonly apiKeySecretRef: string;
    readonly model: string;
    readonly assetDirectory: string;
  }
>()("t3/integrations/ImageConfiguration") {}
const Response = Schema.Struct({ data: Schema.Array(Schema.Struct({ b64_json: Schema.String })) });
const decodeResponse = Schema.decodeUnknownEffect(Response);
export interface ImageRequest {
  readonly requestId: string;
  readonly prompt: string;
  readonly size: "1024x1024" | "1536x1024" | "1024x1536";
  readonly approvalId: string;
}
export class ImageGenerationService extends Context.Service<
  ImageGenerationService,
  {
    readonly status: () => Effect.Effect<
      {
        readonly state: "unconfigured" | "authentication-required" | "configured";
        readonly model: string;
        readonly upstreamCancel: false;
      },
      ImageError
    >;
    /** Synchronous upstream protocol. Interrupting this Effect cancels local waiting, not upstream billing. */
    readonly create: (input: ImageRequest) => Effect.Effect<
      {
        readonly assetId: string;
        readonly mime: "image/png";
        readonly bytes: number;
        readonly model: string;
      },
      ImageError
    >;
    readonly asset: (assetId: string) => Effect.Effect<Uint8Array, ImageError>;
    readonly deleteAsset: (input: typeof ImageDeleteInput.Type) => Effect.Effect<void, ImageError>;
  }
>()("t3/integrations/ImageGenerationService") {}
/** Official protocol: https://developers.openai.com/api/reference/resources/images/methods/generate */
const make = Effect.gen(function* () {
  const config = yield* ImageConfiguration;
  const current = yield* makeCurrentCheck("images");
  const secrets = yield* Secrets.ServerSecretStore;
  const http = yield* Http.IntegrationHttp;
  const approvals = yield* Approvals.WorkflowApprovals;
  const attempts = yield* Attempts.WorkflowAttempts;
  const error = (reason: ImageError["reason"]) => new ImageError({ reason });
  const path = (id: string): Effect.Effect<string, ImageError> =>
    /^[a-f0-9]{64}$/.test(id)
      ? Effect.succeed(join(config.assetDirectory, `${id}.png`))
      : Effect.fail(error("invalid-input"));
  return ImageGenerationService.of({
    status: () =>
      Effect.gen(function* () {
        if (
          !(yield* current()) ||
          !config.enabled ||
          !isIntegrationSecretRefFor(config.apiKeySecretRef, "images")
        )
          return {
            state: "unconfigured" as const,
            model: config.model,
            upstreamCancel: false as const,
          };
        const key = yield* secrets
          .get(config.apiKeySecretRef)
          .pipe(Effect.mapError(() => error("authentication-required")));
        return {
          state: Option.isSome(key)
            ? ("configured" as const)
            : ("authentication-required" as const),
          model: config.model,
          upstreamCancel: false as const,
        };
      }),
    create: (input) =>
      Effect.gen(function* () {
        if (!(yield* current()) || !config.enabled || config.protocol !== "openai-images-v1")
          return yield* Effect.fail(error("unconfigured"));
        if (
          !input.prompt.trim() ||
          input.prompt.length > 32_000 ||
          !["1024x1024", "1536x1024", "1024x1536"].includes(input.size) ||
          !config.model.startsWith("gpt-image-")
        )
          return yield* Effect.fail(error("invalid-input"));
        if (!isIntegrationSecretRefFor(config.apiKeySecretRef, "images"))
          return yield* Effect.fail(error("authentication-required"));
        const key = yield* secrets
          .get(config.apiKeySecretRef)
          .pipe(Effect.mapError(() => error("authentication-required")));
        if (Option.isNone(key)) return yield* Effect.fail(error("authentication-required"));
        const review = JSON.stringify({
          backend: "openai-images-v1",
          model: config.model,
          prompt: input.prompt,
          size: input.size,
          count: 1,
          output: "png",
          cost: "unknown-billed-api-cost",
        });
        yield* approvals
          .consume(input.approvalId, "image.create", review)
          .pipe(Effect.mapError(() => error("approval-required")));
        yield* attempts
          .claim(input.requestId, "image.create", review)
          .pipe(Effect.mapError(() => error("already-attempted")));
        if (!(yield* current())) return yield* Effect.fail(error("unconfigured"));
        const raw = yield* http
          .request({
            url: "https://api.openai.com/v1/images/generations",
            method: "POST",
            headers: {
              authorization: `Bearer ${Buffer.from(key.value).toString("utf8")}`,
              "content-type": "application/json",
            },
            body: JSON.stringify({
              model: config.model,
              prompt: input.prompt,
              size: input.size,
              n: 1,
              output_format: "png",
            }),
            maxBytes: 16_000_000,
          })
          .pipe(Effect.mapError(() => error("unknown-outcome")));
        const response = yield* decodeResponse(raw).pipe(
          Effect.mapError(() => error("invalid-response")),
        );
        const image = response.data[0];
        if (response.data.length !== 1 || !image || !/^[A-Za-z0-9+/]*={0,2}$/.test(image.b64_json))
          return yield* Effect.fail(error("invalid-response"));
        const bytes = Buffer.from(image.b64_json, "base64");
        if (
          bytes.length > 10_000_000 ||
          !bytes.subarray(0, 8).equals(Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]))
        )
          return yield* Effect.fail(error("invalid-response"));
        const assetId = createHash("sha256").update(bytes).digest("hex");
        yield* Effect.tryPromise({
          try: async () => {
            await mkdir(config.assetDirectory, { recursive: true, mode: 0o700 });
            await writeFile(join(config.assetDirectory, `${assetId}.png`), bytes, {
              flag: "wx",
              mode: 0o600,
            }).catch((failure: NodeJS.ErrnoException) => {
              if (failure.code !== "EEXIST") throw failure;
            });
          },
          catch: () => error("asset-unavailable"),
        });
        yield* attempts
          .settle(input.requestId, "completed")
          .pipe(Effect.mapError(() => error("unknown-outcome")));
        return { assetId, mime: "image/png" as const, bytes: bytes.length, model: config.model };
      }),
    asset: (id) =>
      path(id).pipe(
        Effect.flatMap((file) =>
          Effect.tryPromise({ try: () => readFile(file), catch: () => error("asset-unavailable") }),
        ),
      ),
    deleteAsset: (input) =>
      Effect.gen(function* () {
        if (!(yield* current()) || !config.enabled)
          return yield* Effect.fail(error("unconfigured"));
        const file = yield* path(input.assetId);
        yield* approvals
          .consume(
            input.approvalId,
            "image.delete",
            JSON.stringify({ backend: config.protocol, assetId: input.assetId }),
          )
          .pipe(Effect.mapError(() => error("approval-required")));
        if (!(yield* current())) return yield* Effect.fail(error("unconfigured"));
        yield* Effect.tryPromise({
          try: () => unlink(file),
          catch: () => error("asset-unavailable"),
        });
      }),
  });
});
export const layer = Layer.effect(ImageGenerationService, make);
