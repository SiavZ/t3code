import {
  OPTIONAL_AGENT_TOOL_CAPABILITIES,
  type OptionalAgentToolCapability,
} from "@t3tools/contracts";

export const OPTIONAL_AGENT_TOOLS_NOTICE =
  "Optional tools still require grants and configured credentials. Enabling a tool requires restarting the provider session to activate broader access. Disabling takes effect live and only narrows existing access.";
export const MEMORY_AUTO_RECALL_NOTICE =
  "Automatically include relevant memories using lexical matches to the conversation context, not semantic search. Only effective when Memory is enabled.";

export const OPTIONAL_AGENT_TOOL_DETAILS: Record<
  OptionalAgentToolCapability,
  { label: string; description: string }
> = {
  memory: { label: "Memory", description: "Save, search and recall durable memories." },
  "quality-records": {
    label: "Quality records",
    description: "Record task progress and verification evidence.",
  },
};

/** Change one flag using each target's effective value, never a representative's array. */
export function setOptionalAgentToolCapability(
  current: readonly OptionalAgentToolCapability[],
  capability: OptionalAgentToolCapability,
  enabled: boolean,
): OptionalAgentToolCapability[] {
  const selected = new Set(current);
  if (enabled) selected.add(capability);
  else selected.delete(capability);
  return OPTIONAL_AGENT_TOOL_CAPABILITIES.filter((entry) => selected.has(entry));
}

export function optionalAgentToolCapabilityValue(
  targets: readonly { readonly agentToolCapabilities: readonly OptionalAgentToolCapability[] }[],
  capability: OptionalAgentToolCapability,
): boolean | null {
  const first = targets[0];
  if (!first) return null;
  const value = first.agentToolCapabilities.includes(capability);
  return targets.every((target) => target.agentToolCapabilities.includes(capability) === value)
    ? value
    : null;
}
