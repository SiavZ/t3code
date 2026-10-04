import {
  McpCapabilityUnavailableError,
  MemoryEntry,
  MemoryForgetInput,
  MemoryQueryInput,
  MemoryQueryResult,
  MemoryRememberInput,
  MemoryStorageError,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import * as MemoryService from "../../../memory/MemoryService.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  MemoryService.MemoryService,
  Orchestrator.OrchestratorV2,
];

export class MemoryThreadNotFoundError extends Schema.TaggedError<MemoryThreadNotFoundError>()(
  "MemoryThreadNotFoundError",
  { threadId: Schema.String },
) {
  override get message(): string {
    return `Thread ${this.threadId} was not found, so its project memory cannot be reached.`;
  }
}

export const MemoryToolError = Schema.Union([
  McpCapabilityUnavailableError,
  MemoryThreadNotFoundError,
  MemoryService.MemoryProjectFullError,
  MemoryStorageError,
]);

const SCOPE =
  "Project memory is shared by every agent and provider working in this thread's project, persists across threads, and the user can read and delete it in project settings.";

const RememberTool = Tool.make("memory_remember", {
  description: `Save one durable note for future agents in this project: how to build or test, a non-obvious API behaviour, a decision and its reason, or a user preference. Do not save secrets, transient task state, or anything already in the repository's instruction files. ${SCOPE}`,
  parameters: MemoryRememberInput,
  success: MemoryEntry,
  failure: MemoryToolError,
  dependencies,
})
  .annotate(Tool.Title, "Remember a project note")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

const RecallTool = Tool.make("memory_recall", {
  description: `Recall project notes relevant to what you are about to do. Call it at the start of a task with words describing the task. Matching is lexical: entries containing more of your words rank first. ${SCOPE}`,
  parameters: MemoryQueryInput,
  success: MemoryQueryResult,
  failure: MemoryToolError,
  dependencies,
})
  .annotate(Tool.Title, "Recall project notes")
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(Tool.Idempotent, true)
  .annotate(Tool.OpenWorld, false);

const ForgetTool = Tool.make("memory_forget", {
  description: `Delete a project note that is wrong or outdated, by the id that memory_recall returned. forgotten is false when no such note exists in this project. To correct a note, forget it and remember the corrected version. ${SCOPE}`,
  parameters: MemoryForgetInput,
  success: Schema.Struct({ forgotten: Schema.Boolean }),
  failure: MemoryToolError,
  dependencies,
})
  .annotate(Tool.Title, "Forget a project note")
  .annotate(Tool.Readonly, false)
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, false)
  .annotate(Tool.OpenWorld, false);

export const MemoryToolkit = Toolkit.make(RememberTool, RecallTool, ForgetTool);
