import { Effect } from "effect";
import * as Catalog from "../../../integrations/IntegrationCatalog.ts";
import * as Gmail from "../../../integrations/GmailService.ts";
import * as Build from "../../../integrations/RemoteBuildService.ts";
import * as Images from "../../../integrations/ImageGenerationService.ts";
import * as Browser from "../../../integrations/BrowserTaskService.ts";
import * as Desktop from "../../../integrations/DesktopAutomationBroker.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import { IntegrationsToolkit } from "./tools.ts";
const make = Effect.gen(function* () {
  const catalog = yield* Catalog.IntegrationCatalog;
  const gmail = yield* Gmail.GmailService;
  const build = yield* Build.RemoteBuildService;
  const images = yield* Images.ImageGenerationService;
  const browser = yield* Browser.BrowserTaskService;
  const desktop = yield* Desktop.DesktopAutomationBroker;
  const scope = () => Invocation.requireMcpCapability("integrations");
  const browserScope = () =>
    scope().pipe(Effect.andThen(Invocation.requireMcpCapability("preview")));
  return IntegrationsToolkit.of({
    integrations_catalog_status: () => scope().pipe(Effect.andThen(catalog.status())),
    integrations_catalog_search: (input) => scope().pipe(Effect.andThen(catalog.search(input))),
    integrations_catalog_details: (input) =>
      scope().pipe(Effect.andThen(catalog.details(input.requestId, input.productId))),
    integrations_catalog_select: (input) =>
      scope().pipe(Effect.andThen(catalog.select(input.requestId, input.productId, input.reason))),
    integrations_catalog_select_off_catalog: (input) =>
      scope().pipe(
        Effect.andThen(catalog.selectOffCatalog(input.productId, input.publicUrl, input.reason)),
      ),
    integrations_catalog_selections: () => scope().pipe(Effect.andThen(catalog.selections())),
    integrations_catalog_clear_selection: (input) =>
      scope().pipe(Effect.andThen(catalog.clearSelection(input.selectionId))),
    integrations_catalog_suggest: (input) =>
      scope().pipe(
        Effect.andThen(catalog.suggest(input.payload, input.approvalId, input.idempotencyKey)),
      ),
    integrations_gmail_status: () => scope().pipe(Effect.andThen(gmail.status())),
    integrations_gmail_search: (input) =>
      scope().pipe(Effect.andThen(gmail.search(input.query, input.pageToken))),
    integrations_gmail_read: (input) => scope().pipe(Effect.andThen(gmail.read(input.messageId))),
    integrations_gmail_attachment: (input) =>
      scope().pipe(Effect.andThen(gmail.attachment(input.messageId, input.attachmentId))),
    integrations_gmail_labels: () => scope().pipe(Effect.andThen(gmail.labels())),
    integrations_gmail_threads: (input) => scope().pipe(Effect.andThen(gmail.threads(input.query))),
    integrations_gmail_mutate: (input) => scope().pipe(Effect.andThen(gmail.mutate(input))),
    integrations_build_status: () => scope().pipe(Effect.andThen(build.status())),
    integrations_build_prepare: () =>
      scope().pipe(Effect.flatMap((current) => build.prepareForThread(current.threadId))),
    integrations_build_discard: (input) =>
      scope().pipe(Effect.andThen(build.discard(input.snapshotId))),
    integrations_build_submit: (input) => scope().pipe(Effect.andThen(build.submit(input))),
    integrations_image_status: () => scope().pipe(Effect.andThen(images.status())),
    integrations_image_create: (input) => scope().pipe(Effect.andThen(images.create(input))),
    integrations_image_delete_asset: (input) =>
      scope().pipe(Effect.andThen(images.deleteAsset(input))),
    integrations_browser_run: (input) =>
      browserScope().pipe(Effect.flatMap((current) => browser.run(current, input))),
    integrations_browser_get: (input) =>
      browserScope().pipe(Effect.flatMap((current) => browser.get(current, input.taskId))),
    integrations_browser_cancel: (input) =>
      browserScope().pipe(Effect.flatMap((current) => browser.cancel(current, input.taskId))),
    integrations_desktop_hosts: () =>
      scope().pipe(Effect.flatMap((current) => desktop.hosts(current.environmentId))),
    integrations_desktop_lease: (input) =>
      scope().pipe(
        Effect.flatMap((current) =>
          desktop.lease({
            ...input,
            environmentId: current.environmentId,
            threadId: current.threadId,
          }),
        ),
      ),
    integrations_desktop_invoke: (input) =>
      scope().pipe(
        Effect.flatMap((current) => desktop.invoke(current, input.leaseId, input.action)),
      ),
  });
});
export const IntegrationsToolkitHandlersLive = IntegrationsToolkit.toLayer(make);
