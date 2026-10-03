import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { createAgentDocumentsEnvironmentAtoms } from "@t3tools/client-runtime/state/agent-documents";
import {
  assetUrlStateFromResult,
  EMPTY_ASSET_URL_ATOM,
} from "@t3tools/client-runtime/state/assets";
import {
  createDocumentActionInput,
  documentPromptCommand,
  effectiveDocumentPlacement,
  safeDocumentUrl,
} from "@t3tools/client-runtime/agent-documents/model";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import type {
  AgentDocument,
  AgentDocumentAction,
  AgentDocumentActionInput,
  EnvironmentId,
} from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { useContext, useEffect, useState } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { randomUUID } from "../../lib/utils";
import { assetEnvironment } from "../../state/assets";
import { environmentSession } from "../../state/session";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { AgentDocumentView } from "./AgentDocumentView";

const documents = createAgentDocumentsEnvironmentAtoms(connectionAtomRuntime);
function DocumentAssetView(props: {
  document: AgentDocument;
  environmentId: EnvironmentId;
  connected: boolean;
  onAction: (
    action: AgentDocumentAction,
    state: AgentDocumentActionInput["state"],
  ) => Promise<void>;
  onClose: () => Promise<void>;
}) {
  const registry = useContext(RegistryContext);
  const prepared = useAtomValue(
    environmentSession.preparedConnectionValueAtom(props.environmentId),
  );
  const assetQuery =
    props.document.body.kind === "pdf" && !props.document.closed
      ? assetEnvironment.createUrl({
          environmentId: props.environmentId,
          input: {
            resource: {
              _tag: "attachment",
              attachmentId: props.document.body.assetId,
              mimeType: "application/pdf",
              disposition: "inline",
            },
          },
        })
      : EMPTY_ASSET_URL_ATOM;
  const asset = useAtomValue(assetQuery);
  useEffect(() => {
    if (props.connected && assetQuery !== EMPTY_ASSET_URL_ATOM) registry.refresh(assetQuery);
  }, [registry, assetQuery, props.connected, props.document.revision]);
  const resolved = assetUrlStateFromResult(
    asset,
    Option.isSome(prepared) ? prepared.value.httpBaseUrl : null,
  );
  return (
    <AgentDocumentView
      {...props}
      {...(resolved._tag === "Success" ? { assetUrl: resolved.url } : {})}
    />
  );
}
function OpenDocuments(props: {
  environmentId: EnvironmentId;
  ownerThreadId: string;
  projectId: string;
  connected: boolean;
}) {
  const registry = useContext(RegistryContext);
  const [clientId] = useState(() => randomUUID());
  const query = documents.read({
    environmentId: props.environmentId,
    input: {
      operation: "list",
      ownerThreadId: props.ownerThreadId,
      projectId: props.projectId,
      includeClosed: true,
    },
  });
  const result = useAtomValue(query);
  const values = Option.getOrElse(AsyncResult.value(result), () => []);
  const write = useAtomCommand(documents.write);
  const submit = useAtomCommand(documents.action);
  const startTurn = useAtomCommand(threadEnvironment.startTurn);
  const mutation = async (document: AgentDocument, operation: "close" | "reopen") => {
    const outcome = await write({
      environmentId: props.environmentId,
      input: {
        operation,
        ownerThreadId: document.ownerThreadId,
        projectId: document.projectId,
        documentId: document.id,
        operationId: randomUUID(),
        expectedRevision: document.revision,
      },
    });
    registry.refresh(query);
    if (outcome._tag === "Failure") throw squashAtomCommandFailure(outcome);
  };
  const action = async (
    document: AgentDocument,
    action: AgentDocumentAction,
    state: AgentDocumentActionInput["state"],
  ) => {
    const owner = registry
      .get(threadEnvironment.snapshotAtom(props.environmentId))
      ?.threads.find((thread) => thread.id === document.ownerThreadId);
    const promptCommand =
      (action.action === "host.send_prompt" || action.action === "host.start_chat") &&
      "prompt" in action
        ? owner
          ? documentPromptCommand({
              document,
              action: { action: action.action, prompt: action.prompt },
              owner,
              messageId: randomUUID(),
              newThreadId: randomUUID(),
              createdAt: new Date().toISOString(),
            })
          : (() => {
              throw new Error("Document owning thread is unavailable.");
            })()
        : undefined;
    const outcome = await submit({
      environmentId: props.environmentId,
      input: createDocumentActionInput(document, clientId, randomUUID(), action, state),
    });
    registry.refresh(query);
    if (outcome._tag === "Failure") throw squashAtomCommandFailure(outcome);
    if (promptCommand) {
      const dispatched = await startTurn({
        environmentId: props.environmentId,
        input: promptCommand,
      });
      if (dispatched._tag === "Failure") throw squashAtomCommandFailure(dispatched);
    }
    if (action.action === "host.copy" && "text" in action)
      await navigator.clipboard.writeText(action.text);
    if (action.action === "host.open_url" && "url" in action) {
      const url = safeDocumentUrl(action.url);
      if (!url) throw new Error("Unsafe document URL.");
      window.open(url, "_blank", "noopener,noreferrer");
    }
    if (action.action === "host.close")
      await mutation({ ...document, revision: document.revision + 1 }, "close");
    if (action.action === "host.set_state" && "key" in action) {
      const outcome = await write({
        environmentId: props.environmentId,
        input: {
          operation: "patch",
          ownerThreadId: document.ownerThreadId,
          projectId: document.projectId,
          documentId: document.id,
          operationId: randomUUID(),
          expectedRevision: document.revision + 1,
          patches: [
            {
              op: "add",
              path: `/state/${action.key.replace(/~/g, "~0").replace(/\//g, "~1")}`,
              value: action.value,
            },
          ],
        },
      });
      registry.refresh(query);
      if (outcome._tag === "Failure") throw squashAtomCommandFailure(outcome);
    }
  };
  return (
    <div className="max-h-96 space-y-3 overflow-auto p-2">
      <Button variant="ghost" disabled={!props.connected} onClick={() => registry.refresh(query)}>
        Refresh documents
      </Button>
      {result._tag === "Failure" && <p role="alert">Could not load documents.</p>}
      {values.length === 0 && <p>No documents in this thread.</p>}
      {values.map((document) => (
        <div key={document.id}>
          {effectiveDocumentPlacement(document.placement, "web").reason && (
            <p className="text-xs text-muted-foreground">
              {effectiveDocumentPlacement(document.placement, "web").reason}
            </p>
          )}
          <DocumentAssetView
            document={document}
            environmentId={props.environmentId}
            connected={props.connected}
            onAction={(actionValue, state) => action(document, actionValue, state)}
            onClose={() => mutation(document, document.closed ? "reopen" : "close")}
          />
        </div>
      ))}
    </div>
  );
}
export function AgentDocumentsSection(props: {
  environmentId: EnvironmentId;
  ownerThreadId: string;
  projectId: string;
  connected: boolean;
}) {
  const [open, setOpen] = useState(false);
  return (
    <section aria-label="Agent documents" className="shrink-0 border-b">
      <Button variant="ghost" aria-expanded={open} onClick={() => setOpen(!open)}>
        Documents
      </Button>
      {open && <OpenDocuments {...props} />}
    </section>
  );
}
