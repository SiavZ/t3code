import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { createAgentDocumentsEnvironmentAtoms } from "@t3tools/client-runtime/state/agent-documents";
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
import { Linking, Modal, ScrollView, View } from "react-native";
import { connectionAtomRuntime } from "../../connection/runtime";
import { assetEnvironment, useAssetUrl } from "../../state/assets";
import { threadEnvironment } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { tryCopyTextWithHaptic } from "../../lib/copyTextWithHaptic";
import { ControlPill } from "../../components/ControlPill";
import { AppText } from "../../components/AppText";
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
  const assetId =
    props.document.body.kind === "pdf" && !props.document.closed
      ? props.document.body.assetId
      : null;
  useEffect(() => {
    if (props.connected && assetId)
      registry.refresh(
        assetEnvironment.createUrl({
          environmentId: props.environmentId,
          input: {
            resource: {
              _tag: "attachment",
              attachmentId: assetId,
              mimeType: "application/pdf",
              disposition: "inline",
            },
          },
        }),
      );
  }, [registry, props.environmentId, props.connected, props.document.revision, assetId]);
  const url = useAssetUrl(
    props.environmentId,
    props.document.body.kind === "pdf" && !props.document.closed
      ? {
          _tag: "attachment",
          attachmentId: props.document.body.assetId,
          mimeType: "application/pdf",
          disposition: "inline",
        }
      : null,
  );
  return <AgentDocumentView {...props} {...(url ? { assetUrl: url } : {})} />;
}
function OpenDocuments(props: {
  environmentId: EnvironmentId;
  ownerThreadId: string;
  projectId: string;
  connected: boolean;
}) {
  const registry = useContext(RegistryContext);
  const [clientId] = useState(() => `${Date.now()}-${Math.random().toString(36).slice(2)}`);
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
  const nextId = () => `${clientId}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
  const mutation = async (document: AgentDocument, operation: "close" | "reopen") => {
    const outcome = await write({
      environmentId: props.environmentId,
      input: {
        operation,
        ownerThreadId: document.ownerThreadId,
        projectId: document.projectId,
        documentId: document.id,
        operationId: nextId(),
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
              messageId: nextId(),
              newThreadId: nextId(),
              createdAt: new Date().toISOString(),
            })
          : (() => {
              throw new Error("Document owning thread is unavailable.");
            })()
        : undefined;
    const outcome = await submit({
      environmentId: props.environmentId,
      input: createDocumentActionInput(document, clientId, nextId(), action, state),
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
    if (action.action === "host.copy" && "text" in action) await tryCopyTextWithHaptic(action.text);
    if (action.action === "host.open_url" && "url" in action) {
      const url = safeDocumentUrl(action.url);
      if (!url) throw new Error("Unsafe document URL.");
      await Linking.openURL(url);
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
          operationId: nextId(),
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
    <View className="gap-3 p-3">
      <ControlPill
        variant="pill"
        label="Refresh documents"
        disabled={!props.connected}
        onPress={() => registry.refresh(query)}
      />
      {result._tag === "Failure" && (
        <AppText accessibilityRole="alert">Could not load documents.</AppText>
      )}
      {values.length === 0 && <AppText>No documents in this thread.</AppText>}
      {values.map((document) => (
        <View key={document.id}>
          <AppText>{effectiveDocumentPlacement(document.placement, "mobile").reason}</AppText>
          <DocumentAssetView
            document={document}
            environmentId={props.environmentId}
            connected={props.connected}
            onAction={(actionValue, state) => action(document, actionValue, state)}
            onClose={() => mutation(document, document.closed ? "reopen" : "close")}
          />
        </View>
      ))}
    </View>
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
    <View>
      <ControlPill
        variant="pill"
        label="Documents"
        accessibilityLabel="Open agent documents"
        onPress={() => setOpen(true)}
      />
      <Modal
        visible={open}
        presentationStyle="pageSheet"
        animationType="none"
        onRequestClose={() => setOpen(false)}
      >
        <View className="flex-1 bg-screen pt-12">
          <ControlPill label="Done" variant="pill" onPress={() => setOpen(false)} />
          <ScrollView>{open && <OpenDocuments {...props} />}</ScrollView>
        </View>
      </Modal>
    </View>
  );
}
