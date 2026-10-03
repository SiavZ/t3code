import { OPTIONAL_AGENT_TOOL_CAPABILITIES } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  optionalAgentToolCapabilityValue,
  setOptionalAgentToolCapability,
} from "./agentToolSettings.ts";

describe("optional agent tool settings", () => {
  it("preserves each target's unrelated flags when enabling or disabling one tool", () => {
    const first = ["agent-documents", "memory"] as const;
    const second = ["automation"] as const;
    expect(setOptionalAgentToolCapability(first, "memory", false)).toEqual(["agent-documents"]);
    expect(setOptionalAgentToolCapability(second, "memory", true)).toEqual([
      "memory",
      "automation",
    ]);
    expect(first).toEqual(["agent-documents", "memory"]);
    expect(second).toEqual(["automation"]);
  });
  it("is idempotent, deduplicated and bounded by the capability catalog", () => {
    expect(setOptionalAgentToolCapability(["memory", "memory"], "memory", true)).toEqual([
      "memory",
    ]);
    expect(
      setOptionalAgentToolCapability(OPTIONAL_AGENT_TOOL_CAPABILITIES, "memory", true),
    ).toHaveLength(5);
    expect(setOptionalAgentToolCapability([], "memory", false)).toEqual([]);
  });
  it("disabling any capability never broadens access", () => {
    for (const capability of OPTIONAL_AGENT_TOOL_CAPABILITIES) {
      const narrowed = setOptionalAgentToolCapability(
        OPTIONAL_AGENT_TOOL_CAPABILITIES,
        capability,
        false,
      );
      expect(narrowed).toHaveLength(4);
      expect(narrowed).not.toContain(capability);
      expect(narrowed.every((entry) => OPTIONAL_AGENT_TOOL_CAPABILITIES.includes(entry))).toBe(
        true,
      );
    }
  });
  it("compares the selected flag rather than entire capability arrays", () => {
    expect(
      optionalAgentToolCapabilityValue(
        [
          { agentToolCapabilities: ["memory", "agent-documents"] },
          { agentToolCapabilities: ["memory"] },
        ],
        "memory",
      ),
    ).toBe(true);
    expect(
      optionalAgentToolCapabilityValue(
        [{ agentToolCapabilities: ["agent-documents"] }, { agentToolCapabilities: [] }],
        "memory",
      ),
    ).toBe(false);
    expect(
      optionalAgentToolCapabilityValue(
        [{ agentToolCapabilities: ["memory"] }, { agentToolCapabilities: [] }],
        "memory",
      ),
    ).toBeNull();
    expect(optionalAgentToolCapabilityValue([], "memory")).toBeNull();
  });
});
