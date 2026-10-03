import { Schema } from "effect";
import { PreviewTabId } from "./preview.ts";
import {
  PreviewAutomationNavigateInput,
  PreviewAutomationClickInput,
} from "./previewAutomation.ts";

export const IntegrationAvailability = Schema.Literals([
  "unconfigured",
  "disabled",
  "authentication-required",
  "available",
  "unreachable",
]);
export const IntegrationStatus = Schema.Struct({
  sourceId: Schema.String,
  state: IntegrationAvailability,
  protocolVersion: Schema.String,
});
export const CatalogProduct = Schema.Struct({
  productId: Schema.String,
  name: Schema.String,
  publicUrl: Schema.String,
  capabilities: Schema.Array(Schema.String),
  setupInstructions: Schema.String,
  authRequirements: Schema.Array(Schema.String),
  revision: Schema.String,
});
export const CatalogSearch = Schema.Struct({
  query: Schema.String,
  category: Schema.String,
  requirements: Schema.Array(Schema.String),
});
export const CatalogSearchResult = Schema.Struct({
  requestId: Schema.String,
  sourceId: Schema.String,
  fetchedAt: Schema.Number,
  products: Schema.Array(CatalogProduct),
});
export const CatalogSelection = Schema.Struct({
  selectionId: Schema.String,
  sourceId: Schema.String,
  productId: Schema.String,
  publicUrl: Schema.String,
  revision: Schema.String,
  requestId: Schema.String,
  reason: Schema.String,
  selectedAt: Schema.Number,
});
export type CatalogProduct = typeof CatalogProduct.Type;
export type CatalogSearch = typeof CatalogSearch.Type;
export type CatalogSearchResult = typeof CatalogSearchResult.Type;
export type CatalogSelection = typeof CatalogSelection.Type;

/** Environment-admin configuration. Secret references never contain credential values. */
export const OptionalIntegrations = Schema.Struct({
  catalog: Schema.optional(
    Schema.Struct({
      enabled: Schema.Boolean,
      sourceId: Schema.String,
      protocol: Schema.Literal("catalog-json-post-v1"),
      searchUrl: Schema.String,
      detailsUrl: Schema.String,
      suggestUrl: Schema.String,
    }),
  ),
  gmail: Schema.optional(
    Schema.Struct({
      enabled: Schema.Boolean,
      accountId: Schema.String,
      clientId: Schema.String,
      clientSecretRef: Schema.String,
      tokenSecretRef: Schema.String,
      redirectUri: Schema.String,
      scopes: Schema.Array(Schema.String),
    }),
  ),
  build: Schema.optional(
    Schema.Struct({
      enabled: Schema.Boolean,
      protocol: Schema.Literal("jcode-compile-v1"),
      baseUrl: Schema.String,
      apiKeySecretRef: Schema.String,
    }),
  ),
  images: Schema.optional(
    Schema.Struct({
      enabled: Schema.Boolean,
      protocol: Schema.Literal("openai-images-v1"),
      apiKeySecretRef: Schema.String,
      model: Schema.String,
    }),
  ),
  browser: Schema.optional(
    Schema.Struct({
      enabled: Schema.Boolean,
      model: Schema.String,
      apiKeySecretRef: Schema.String,
    }),
  ),
});
export type OptionalIntegrations = typeof OptionalIntegrations.Type;

export const GmailMutationInput = Schema.Struct({
  operation: Schema.Literals(["draft", "send-draft", "trash", "modify-labels"]),
  targetId: Schema.String,
  raw: Schema.optional(Schema.String),
  addLabelIds: Schema.optional(Schema.Array(Schema.String)),
  removeLabelIds: Schema.optional(Schema.Array(Schema.String)),
  approvalId: Schema.String,
});
const { approvalId: _gmailApprovalId, ...gmailReviewFields } = GmailMutationInput.fields;
export const GmailMutationReviewInput = Schema.Struct(gmailReviewFields);
export const BuildSubmissionInput = Schema.Struct({
  snapshotId: Schema.String,
  requestId: Schema.String,
  command: Schema.String,
  timeoutSeconds: Schema.Int,
  unknownCostAcknowledged: Schema.Literal(true),
  approvalId: Schema.String,
});
export const ImageGenerationInput = Schema.Struct({
  requestId: Schema.String,
  prompt: Schema.String,
  size: Schema.Literals(["1024x1024", "1536x1024", "1024x1536"]),
  approvalId: Schema.String,
});
export const BrowserTaskInput = Schema.Struct({
  taskId: Schema.String,
  tabId: PreviewTabId,
  goal: Schema.String,
  completion: Schema.Union([
    Schema.Struct({ kind: Schema.Literal("visible-text"), text: Schema.String }),
    Schema.Struct({ kind: Schema.Literal("url"), url: Schema.String }),
  ]),
  context: Schema.String,
  candidates: Schema.Array(
    Schema.Union([
      Schema.Struct({
        operation: Schema.Literal("navigate"),
        input: PreviewAutomationNavigateInput,
      }),
      Schema.Struct({ operation: Schema.Literal("click"), input: PreviewAutomationClickInput }),
    ]),
  ),
  maxSteps: Schema.Int,
  approvalId: Schema.String,
});

/** The transport supplies the authenticated human session, never the caller. */
export const ApprovalGrant = Schema.Struct({ operation: Schema.String, review: Schema.String });
export const ApprovalGrantResult = Schema.Struct({ approvalId: Schema.String });
export const CatalogDetailsInput = Schema.Struct({
  requestId: Schema.String,
  productId: Schema.String,
});
export const CatalogSelectInput = Schema.Struct({
  requestId: Schema.String,
  productId: Schema.String,
  reason: Schema.String,
});
export const CatalogOffCatalogInput = Schema.Struct({
  productId: Schema.String,
  publicUrl: Schema.String,
  reason: Schema.String,
});
export const CatalogSuggestInput = Schema.Struct({
  payload: Schema.String,
  approvalId: Schema.String,
  idempotencyKey: Schema.String,
});
export const CatalogStatus = Schema.Struct({
  sourceId: Schema.String,
  state: Schema.Literals(["available", "disabled", "unconfigured"]),
});
export const SelectionIdInput = Schema.Struct({ selectionId: Schema.String });
export const GmailConnectInput = Schema.Struct({ approvalId: Schema.String });
export const GmailConnectResult = Schema.Struct({ url: Schema.String, state: Schema.String });
export const GmailCompleteConnectInput = Schema.Struct({
  state: Schema.String,
  code: Schema.String,
});
export const GmailSearchInput = Schema.Struct({
  query: Schema.String,
  pageToken: Schema.optional(Schema.String),
});
export const GmailReadInput = Schema.Struct({ messageId: Schema.String });
export const GmailAttachmentInput = Schema.Struct({
  messageId: Schema.String,
  attachmentId: Schema.String,
});
export const GmailAttachment = Schema.Struct({ size: Schema.Number, data: Schema.String });
export const GmailThreadsInput = Schema.Struct({ query: Schema.String });
export const GmailStatus = Schema.Struct({
  accountId: Schema.String,
  state: Schema.Literals(["unconfigured", "authentication-required", "configured"]),
});
export const GmailMessage = Schema.Struct({
  id: Schema.String,
  threadId: Schema.optional(Schema.String),
  snippet: Schema.optional(Schema.String),
  raw: Schema.optional(Schema.String),
});
/** Gmail resource variants are preserved rather than represented as a fabricated subset. */
export const GmailResourceResult = Schema.Unknown;
export const BuildPrepareInput = Schema.Struct({ threadId: Schema.String });
export const BuildSnapshotInput = Schema.Struct({ snapshotId: Schema.String });
export const BuildSnapshot = Schema.Struct({
  snapshotId: Schema.String,
  digest: Schema.String,
  paths: Schema.Array(Schema.String),
  excluded: Schema.Array(Schema.String),
  bytes: Schema.Number,
});
export const BuildStatus = Schema.Struct({
  state: Schema.Literals(["unconfigured", "authentication-required", "eligible", "not-entitled"]),
  availableMicrocredits: Schema.optional(Schema.Number),
  upstreamCancel: Schema.Literal(false),
  artifacts: Schema.Literal(false),
});
export const BuildResult = Schema.Struct({
  exit_code: Schema.Number,
  stdout: Schema.String,
  stderr: Schema.String,
  truncated: Schema.optional(Schema.Boolean),
  cleanup_confirmed: Schema.optional(Schema.Boolean),
});
export const ImageStatus = Schema.Struct({
  state: Schema.Literals(["unconfigured", "authentication-required", "configured"]),
  model: Schema.String,
  upstreamCancel: Schema.Literal(false),
});
export const ImageAssetInput = Schema.Struct({ assetId: Schema.String });
export const ImageDeleteInput = Schema.Struct({
  assetId: Schema.String,
  approvalId: Schema.String,
});
export const ImageAsset = Schema.Struct({
  assetId: Schema.String,
  mime: Schema.Literal("image/png"),
  bytes: Schema.Number,
  model: Schema.String,
});
export const BrowserTaskIdInput = Schema.Struct({ taskId: Schema.String });
export const BrowserTaskRecord = Schema.Struct({
  taskId: Schema.String,
  state: Schema.Literals(["running", "completed", "blocked", "canceled", "failed"]),
  steps: Schema.Number,
  evidence: Schema.optional(Schema.String),
});
const workflowError = <const Tag extends string>(
  tag: Tag,
  reasons: readonly [string, ...string[]],
) => Schema.Struct({ _tag: Schema.Literal(tag), reason: Schema.Literals(reasons) });
export const IntegrationWorkflowError = Schema.Union([
  workflowError("ApprovalError", ["required", "expired", "mismatch", "storage"]),
  workflowError("CatalogError", [
    "unconfigured",
    "disabled",
    "expired-request",
    "invalid-response",
    "approval-required",
    "transport",
  ]),
  workflowError("GmailError", [
    "unconfigured",
    "authentication-required",
    "approval-required",
    "invalid-response",
    "invalid-input",
    "state-mismatch",
    "transport",
    "unknown-outcome",
  ]),
  workflowError("RemoteBuildError", [
    "unconfigured",
    "authentication-required",
    "not-entitled",
    "insufficient-credits",
    "invalid-response",
    "snapshot-missing",
    "invalid-input",
    "approval-required",
    "unknown-outcome",
    "already-attempted",
  ]),
  workflowError("ImageError", [
    "unconfigured",
    "authentication-required",
    "approval-required",
    "invalid-response",
    "invalid-input",
    "already-attempted",
    "unknown-outcome",
    "asset-unavailable",
  ]),
  workflowError("BrowserTaskError", [
    "executor-unconfigured",
    "approval-required",
    "invalid-decision",
    "broker",
    "budget",
    "canceled",
    "not-found",
  ]),
]);
