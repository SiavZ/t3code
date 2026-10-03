import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { AgentDocumentError, AgentDocumentWriteInput } from "@t3tools/contracts";
import * as AgentDocuments from "../../../orchestration/AgentDocuments.ts";
import * as AgentDocumentAssets from "../../../orchestration/AgentDocumentAssets.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { AgentDocumentsToolkit } from "./tools.ts";

const decodeWriteInput = Schema.decodeUnknownEffect(AgentDocumentWriteInput);

export const documentInvocationScope = Effect.gen(function* () {
  const invocation = yield* McpInvocationContext.requireMcpCapability("agent-documents");
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const thread = yield* snapshots
    .getThreadShellById(invocation.threadId)
    .pipe(
      Effect.mapError(
        () => new AgentDocumentError({ code: "notFound", detail: "Owning thread not found." }),
      ),
    );
  if (Option.isNone(thread))
    return yield* new AgentDocumentError({ code: "notFound", detail: "Owning thread not found." });
  return { ownerThreadId: invocation.threadId, projectId: thread.value.projectId };
});
const make = Effect.gen(function* () {
  const documents = yield* AgentDocuments.AgentDocuments;
  const assets = yield* AgentDocumentAssets.AgentDocumentAssets;
  return AgentDocumentsToolkit.of({
    agent_documents_prepare_pdf: Effect.fn("AgentDocumentsToolkit.preparePdf")(function* (input) {
      const scope = yield* documentInvocationScope;
      return yield* assets.prepare({
        relativePath: input.relativePath,
        ownerThreadId: scope.ownerThreadId,
      });
    }),
    agent_documents_read: Effect.fn("AgentDocumentsToolkit.read")(function* (input) {
      const scope = yield* documentInvocationScope;
      return yield* documents.read({ ...input.input, ...scope });
    }),
    agent_documents_write: Effect.fn("AgentDocumentsToolkit.write")(function* (input) {
      const scope = yield* documentInvocationScope;
      const decoded = yield* decodeWriteInput({
        ...input.input,
        ...scope,
      }).pipe(
        Effect.mapError(
          () => new AgentDocumentError({ code: "invalid", detail: "Invalid document mutation." }),
        ),
      );
      return yield* documents.write(decoded);
    }),
    agent_documents_wait: Effect.fn("AgentDocumentsToolkit.wait")(function* (input) {
      const scope = yield* documentInvocationScope;
      return yield* documents.wait({ ...input, ...scope });
    }),
  });
});
export const AgentDocumentsToolkitHandlersLive = AgentDocumentsToolkit.toLayer(make);
