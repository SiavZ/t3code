import {
  MessageId,
  ThreadId,
  type AgentDocument,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import type { StartThreadTurnInput } from "../operations/commands.ts";

/** Builds an independent ordinary turn command from the owning thread snapshot.
 * Neither the selected thread nor any composer draft participates in this action. */
export function documentPromptCommand(input: {
  readonly document: AgentDocument;
  readonly action: {
    readonly action: "host.send_prompt" | "host.start_chat";
    readonly prompt: string;
  };
  readonly owner: Pick<
    OrchestrationThreadShell,
    "id" | "projectId" | "runtimeMode" | "interactionMode" | "modelSelection"
  >;
  readonly messageId: string;
  readonly newThreadId: string;
  readonly createdAt: string;
}): StartThreadTurnInput {
  const { document, action, owner } = input;
  if (document.ownerThreadId !== owner.id || document.projectId !== owner.projectId)
    throw new Error("Document owning thread is unavailable.");
  if (document.closed) throw new Error("Document is closed.");
  if (!action.prompt.trim() || action.prompt.length > 65_536)
    throw new Error("Document prompt is empty or exceeds its limit.");
  const command = {
    threadId: ThreadId.make(
      action.action === "host.start_chat" ? input.newThreadId : document.ownerThreadId,
    ),
    message: {
      messageId: MessageId.make(input.messageId),
      role: "user" as const,
      text: action.prompt,
      attachments: [],
    },
    runtimeMode: owner.runtimeMode,
    interactionMode: owner.interactionMode,
    modelSelection: owner.modelSelection,
    createdAt: input.createdAt,
  };
  if (action.action === "host.send_prompt") return command;
  return {
    ...command,
    bootstrap: {
      createThread: {
        projectId: owner.projectId,
        title: "Document conversation",
        modelSelection: owner.modelSelection,
        runtimeMode: owner.runtimeMode,
        interactionMode: owner.interactionMode,
        branch: null,
        worktreePath: null,
        createdAt: input.createdAt,
      },
    },
  };
}
