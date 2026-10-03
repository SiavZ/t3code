import { expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as NodeFSP from "node:fs/promises";
const { mkdtemp, rm } = NodeFSP;
import * as NodeOS from "node:os";
const { tmpdir } = NodeOS;
import * as NodePath from "node:path";
const { join } = NodePath;
import * as Images from "./ImageGenerationService.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import { testPersistence, testSecrets } from "./integrationTestSupport.ts";

it.effect(
  "uses independent API credentials and exact approval, stores bounded assets and prevents duplicate billing",
  () =>
    Effect.gen(function* () {
      const assetDirectory = yield* Effect.promise(() =>
        mkdtemp(join(process.env.JCODE_SCRATCH_DIR ?? tmpdir(), "t3-image-fixture-")),
      );
      const png = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10, 0]);
      let calls = 0;
      const config = {
        enabled: true,
        protocol: "openai-images-v1" as const,
        apiKeySecretRef: "integration-images-fixture",
        model: "gpt-image-1",
        assetDirectory,
      };
      const persistence = testPersistence();
      const dependencies = Layer.mergeAll(
        persistence,
        testSecrets({ [config.apiKeySecretRef]: "fixture-api-key" }).layer,
        Layer.succeed(Images.ImageConfiguration, config),
        Layer.succeed(Http.IntegrationHttp, {
          request: (request) =>
            Effect.sync(() => {
              calls++;
              expect(request.url).toBe("https://api.openai.com/v1/images/generations");
              expect(request.headers?.authorization).toBe("Bearer fixture-api-key");
              expect(JSON.parse(request.body!)).toEqual({
                model: config.model,
                prompt: "fixture",
                size: "1024x1024",
                n: 1,
                output_format: "png",
              });
              return { data: [{ b64_json: png.toString("base64") }] };
            }),
        }),
      );
      try {
        yield* Effect.gen(function* () {
          const images = yield* Images.ImageGenerationService;
          const approvals = yield* Approvals.WorkflowApprovals;
          const review = JSON.stringify({
            backend: config.protocol,
            model: config.model,
            prompt: "fixture",
            size: "1024x1024",
            count: 1,
            output: "png",
            cost: "unknown-billed-api-cost",
          });
          const grant = () =>
            approvals.grant({ humanSessionId: "human", operation: "image.create", review });
          const input = { requestId: "request-1", prompt: "fixture", size: "1024x1024" as const };
          expect(
            (yield* Effect.flip(images.create({ ...input, approvalId: "agent-forged" }))).reason,
          ).toBe("approval-required");
          expect(calls).toBe(0);
          const asset = yield* images.create({ ...input, approvalId: yield* grant() });
          expect(Buffer.from(yield* images.asset(asset.assetId))).toEqual(png);
          expect(
            (yield* Effect.flip(images.create({ ...input, approvalId: yield* grant() }))).reason,
          ).toBe("already-attempted");
          expect(calls).toBe(1);
          const deletion = { assetId: asset.assetId };
          expect(
            (yield* Effect.flip(images.deleteAsset({ ...deletion, approvalId: "" }))).reason,
          ).toBe("approval-required");
          const deleteGrant = (backend: string, assetId: string, operation = "image.delete") =>
            approvals.grant({
              humanSessionId: "human",
              operation,
              review: JSON.stringify({ backend, assetId }),
            });
          const foreign = yield* deleteGrant("foreign-images-v1", asset.assetId);
          expect(
            (yield* Effect.flip(images.deleteAsset({ ...deletion, approvalId: foreign }))).reason,
          ).toBe("approval-required");
          const approvalId = yield* deleteGrant(config.protocol, asset.assetId);
          expect(
            (yield* Effect.flip(images.deleteAsset({ assetId: "0".repeat(64), approvalId })))
              .reason,
          ).toBe("approval-required");
          expect(Buffer.from(yield* images.asset(asset.assetId))).toEqual(png);
          yield* images.deleteAsset({ ...deletion, approvalId });
          expect((yield* Effect.flip(images.deleteAsset({ ...deletion, approvalId }))).reason).toBe(
            "approval-required",
          );
          expect((yield* Effect.flip(images.asset(asset.assetId))).reason).toBe(
            "asset-unavailable",
          );
          expect((yield* Effect.flip(images.asset("../../secrets"))).reason).toBe("invalid-input");
        }).pipe(Effect.provide(Images.layer.pipe(Layer.provideMerge(dependencies))));
      } finally {
        yield* Effect.promise(() => rm(assetDirectory, { recursive: true, force: true }));
      }
    }),
);
