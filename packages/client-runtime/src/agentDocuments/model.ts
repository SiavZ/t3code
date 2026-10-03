export { documentPromptCommand } from "./prompts.ts";
import type {
  AgentDocument,
  AgentDocumentAction,
  AgentDocumentActionInput,
  AgentDocumentReadInput,
  AgentDocumentWriteInput,
} from "@t3tools/contracts";

export interface AgentDocumentsClient {
  readonly read: (input: AgentDocumentReadInput) => Promise<readonly AgentDocument[]>;
  readonly write: (input: AgentDocumentWriteInput) => Promise<AgentDocument>;
  readonly action: (input: AgentDocumentActionInput) => Promise<number>;
}
export function mergeDocumentSnapshot(
  previous: readonly AgentDocument[],
  incoming: readonly AgentDocument[],
): readonly AgentDocument[] {
  const documents = new Map(previous.map((document) => [document.id, document]));
  for (const document of incoming) {
    const old = documents.get(document.id);
    if (!old || document.revision > old.revision) documents.set(document.id, document);
  }
  return [...documents.values()];
}
export function effectiveDocumentPlacement(
  placement: AgentDocument["placement"],
  surface: "web" | "mobile",
): { actualPlacement: "end" | "sheet"; reason: string | null } {
  if (placement === "end") return { actualPlacement: "end", reason: null };
  if (surface === "mobile" && ["panel", "sidebar", "overlay"].includes(placement))
    return { actualPlacement: "sheet", reason: "This surface presents document panels as sheets." };
  return {
    actualPlacement: "end",
    reason: "This document is shown at the thread end because its preferred anchor is unavailable.",
  };
}
export function safeDocumentUrl(url: string): string | null {
  try {
    const parsed = new URL(url);
    return ["https:", "http:"].includes(parsed.protocol) && !parsed.username && !parsed.password
      ? parsed.href
      : null;
  } catch {
    return null;
  }
}
export function createDocumentActionInput(
  document: AgentDocument,
  clientId: string,
  actionId: string,
  action: AgentDocumentAction,
  state: AgentDocumentActionInput["state"],
): AgentDocumentActionInput {
  return {
    projectId: document.projectId,
    ownerThreadId: document.ownerThreadId,
    documentId: document.id,
    expectedRevision: document.revision,
    clientId,
    actionId,
    action,
    state: structuredClone(state),
  };
}
