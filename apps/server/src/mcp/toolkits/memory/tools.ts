import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import * as M from "../../../../../../packages/contracts/src/memory.ts";
import * as Memory from "../../../memory/Memory.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
const dependencies = [
  McpInvocationContext.McpInvocationContext,
  Memory.MemoryService,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
];
const failure = Schema.Union([M.MemoryError, McpCapabilityUnavailableError]);
export const MemoryToolkit = Toolkit.make(
  Tool.make("memory_remember", {
    description:
      "Remember an explicit project memory. Global authority cannot be granted by an agent. Reuse operationId for retries.",
    parameters: M.MemoryRememberInput,
    success: M.MemoryMutationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("memory_recall", {
    description:
      "Retrieve bounded project memories using lexical matching, not semantic selection.",
    parameters: M.MemoryReadInput,
    success: M.MemoryResult,
    failure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("memory_search", {
    description: "Search explicit project memory content and tags. At most 20 results.",
    parameters: M.MemoryReadInput,
    success: M.MemoryResult,
    failure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("memory_forget", {
    description:
      "Hard-delete a memory body and links. Existing provider transcripts are not erased. Requires expectedRevision.",
    parameters: M.MemoryMutationInput,
    success: M.MemoryMutationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, true)
    .annotate(Tool.Idempotent, true),
  Tool.make("memory_tag", {
    description: "Replace normalized project memory tags with revision checking.",
    parameters: M.MemoryTagInput,
    success: M.MemoryMutationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("memory_link", {
    description: "Link two explicit memories within the same project store.",
    parameters: M.MemoryLinkInput,
    success: M.MemoryMutationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("memory_related", {
    description: "Retrieve bounded one-hop neighbors of an authorized project memory.",
    parameters: M.MemoryRelatedInput,
    success: M.MemoryResult,
    failure,
    dependencies,
  }).annotate(Tool.Readonly, true),
);
