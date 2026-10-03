import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import {
  AgentDocument,
  AgentDocumentAcceptedAction,
  AgentDocumentError,
  AgentDocumentAsset,
  AgentDocumentAssetPrepareInput,
  AgentDocumentWriteInput,
  AgentDocumentWaitInput,
} from "@t3tools/contracts";
import * as AgentDocumentAssets from "../../../orchestration/AgentDocumentAssets.ts";
import * as AgentDocuments from "../../../orchestration/AgentDocuments.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";

const dependencies = [
  AgentDocuments.AgentDocuments,
  McpInvocationContext.McpInvocationContext,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
];
const failure = Schema.Union([AgentDocumentError, McpCapabilityUnavailableError]);
// Scope fields are not tool parameters. Handlers derive them from the invocation.
export const AgentDocumentsToolkit = Toolkit.make(
  Tool.make("agent_documents_prepare_pdf", {
    description:
      "Copy a workspace-relative PDF into immutable authenticated storage owned by this invocation thread. Returns an assetId, never a host path.",
    parameters: Schema.Struct({ relativePath: AgentDocumentAssetPrepareInput.fields.relativePath }),
    success: AgentDocumentAsset,
    failure,
    dependencies: [...dependencies, AgentDocumentAssets.AgentDocumentAssets],
  }).annotate(Tool.Idempotent, false),
  Tool.make("agent_documents_read", {
    description: "Fetch this thread's bounded documents or one document revision.",
    parameters: Schema.Struct({
      input: Schema.Union([
        Schema.Struct({ operation: Schema.Literal("get"), documentId: Schema.String }),
        Schema.Struct({
          operation: Schema.Literal("list"),
          includeClosed: Schema.optional(Schema.Boolean),
        }),
      ]),
    }),
    success: Schema.Array(AgentDocument),
    failure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("agent_documents_write", {
    description:
      "Mount, replace, patch, move, close, reopen or acknowledge this thread's documents. Reuse operationId on retries. Mutations require expectedRevision.",
    parameters: Schema.Struct({
      input: Schema.Union(
        AgentDocumentWriteInput.members.map((member) => {
          const { ownerThreadId: _owner, projectId: _project, ...fields } = member.fields;
          return Schema.Struct(fields);
        }),
      ),
    }),
    success: AgentDocument,
    failure,
    dependencies,
  }).annotate(Tool.Idempotent, true),
  Tool.make("agent_documents_wait", {
    description:
      "Wait for a durable accepted document action after a sequence. Closing the document cancels waiting.",
    parameters: Schema.Struct({
      documentId: AgentDocumentWaitInput.fields.documentId,
      afterSequence: AgentDocumentWaitInput.fields.afterSequence,
    }),
    success: Schema.Array(AgentDocumentAcceptedAction),
    failure,
    dependencies,
  }).annotate(Tool.Readonly, true),
);
