import { expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Settings from "../serverSettings.ts";
import * as Guard from "./IntegrationConfigurationGuard.ts";
import * as Images from "./ImageGenerationService.ts";
import * as Http from "./IntegrationHttp.ts";
import { testPersistence, testSecrets } from "./integrationTestSupport.ts";

it.effect(
  "immediately disables captured production adapters after persisted configuration changes",
  () => {
    const images = {
      enabled: true,
      protocol: "openai-images-v1" as const,
      apiKeySecretRef: "integration-images-fixture",
      model: "gpt-image-1",
    };
    const settings = Settings.layerTest({ optionalIntegrations: { images } });
    const guard = Guard.configuredLayer({ images }).pipe(Layer.provideMerge(settings));
    const dependencies = Layer.mergeAll(
      guard,
      testPersistence(),
      testSecrets({ [images.apiKeySecretRef]: "fixture" }).layer,
      Layer.succeed(Images.ImageConfiguration, { ...images, assetDirectory: "/unused" }),
      Layer.succeed(Http.IntegrationHttp, {
        request: () => Effect.die("disabled adapter must never dispatch"),
      }),
    );
    return Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      const service = yield* Images.ImageGenerationService;
      expect((yield* service.status()).state).toBe("configured");
      yield* settings.updateSettings({
        optionalIntegrations: { images: { ...images, enabled: false } },
      });
      expect((yield* service.status()).state).toBe("unconfigured");
      expect(
        (yield* Effect.flip(
          service.create({
            requestId: "fixture",
            prompt: "fixture",
            size: "1024x1024",
            approvalId: "forged",
          }),
        )).reason,
      ).toBe("unconfigured");
      yield* settings.updateSettings({
        optionalIntegrations: { images: { ...images, model: "gpt-image-new" } },
      });
      expect((yield* service.status()).state).toBe("unconfigured");
    }).pipe(Effect.provide(Images.layer.pipe(Layer.provideMerge(dependencies))));
  },
);
