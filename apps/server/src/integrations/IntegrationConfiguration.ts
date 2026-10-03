import { Effect, Layer } from "effect";
import type { OptionalIntegrations } from "../../../../packages/contracts/src/integrationWorkflows.ts";
import * as Guard from "./IntegrationConfigurationGuard.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import * as Attempts from "./WorkflowAttempts.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Catalog from "./IntegrationCatalog.ts";
import * as CatalogHttp from "./CatalogHttp.ts";
import * as Gmail from "./GmailService.ts";
import * as Build from "./RemoteBuildService.ts";
import * as Snapshots from "./SourceSnapshots.ts";
import * as Images from "./ImageGenerationService.ts";
import * as Browser from "./BrowserTaskService.ts";
import * as SecretsProvisioning from "./IntegrationSecrets.ts";
import * as Desktop from "./DesktopAutomationBroker.ts";

/** Root supplies authenticated persisted settings and an environment-owned artifact directory.
 * Requires only existing SqlClient, ServerSecretStore and PreviewAutomationBroker layers.
 * Construction performs no network, account connection, file upload or desktop automation.
 */
export const configuredLayer = (settings: OptionalIntegrations, assetDirectory: string) => {
  const configuration = Layer.mergeAll(
    Layer.succeed(
      Gmail.GmailConfiguration,
      settings.gmail ?? {
        enabled: false,
        accountId: "",
        clientId: "",
        clientSecretRef: "",
        tokenSecretRef: "",
        redirectUri: "",
        scopes: [],
      },
    ),
    Layer.succeed(
      Build.RemoteBuildConfiguration,
      settings.build ?? {
        enabled: false,
        protocol: "jcode-compile-v1",
        baseUrl: "",
        apiKeySecretRef: "",
      },
    ),
    Layer.succeed(Images.ImageConfiguration, {
      ...(settings.images ?? {
        enabled: false,
        protocol: "openai-images-v1" as const,
        apiKeySecretRef: "",
        model: "",
      }),
      assetDirectory,
    }),
    Layer.succeed(
      Browser.BrowserExecutorConfiguration,
      settings.browser ?? { enabled: false, model: "", apiKeySecretRef: "" },
    ),
    Layer.succeed(
      CatalogHttp.CatalogHttpConfiguration,
      settings.catalog ?? {
        enabled: false,
        sourceId: "",
        searchUrl: "",
        detailsUrl: "",
        suggestUrl: "",
      },
    ),
  );
  const common = Layer.mergeAll(
    Guard.configuredLayer(settings),
    Approvals.layer,
    Attempts.layer,
    SecretsProvisioning.layer,
    Http.layer,
    Snapshots.layer,
    configuration,
    CatalogHttp.transportLayer,
  );
  const catalogSource = CatalogHttp.sourceLayer.pipe(Layer.provide(common));
  const disclosure = Layer.effect(
    Catalog.CatalogDisclosureApproval,
    Effect.gen(function* () {
      const approvals = yield* Approvals.WorkflowApprovals;
      return Catalog.CatalogDisclosureApproval.of({
        consume: (id, payload) =>
          approvals
            .consume(id, "catalog.suggest", payload)
            .pipe(Effect.mapError(() => new Catalog.CatalogError({ reason: "approval-required" }))),
      });
    }),
  ).pipe(Layer.provide(common));
  return Layer.mergeAll(
    Catalog.layer.pipe(Layer.provide(Layer.merge(catalogSource, disclosure))),
    Gmail.layer,
    Build.layer,
    Images.layer,
    Browser.layer,
    Desktop.layer,
  ).pipe(Layer.provideMerge(common));
};
