import { OPTIONAL_AGENT_TOOL_CAPABILITIES } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  optionalAgentToolCapabilityValue,
  setOptionalAgentToolCapability,
} from "./agentToolSettings.ts";

describe("optional agent tool settings", () => {
  it("preserves each target's unrelated flags when enabling or disabling one tool", () => {
    const first = ["knowledge", "memory"] as const;
    const second = ["automation"] as const;
    expect(setOptionalAgentToolCapability(first, "memory", false)).toEqual(["knowledge"]);
    expect(setOptionalAgentToolCapability(second, "memory", true)).toEqual([
      "memory",
      "automation",
    ]);
    expect(first).toEqual(["knowledge", "memory"]);
    expect(second).toEqual(["automation"]);
  });
  it("is idempotent, deduplicated and bounded by the capability catalog", () => {
    expect(setOptionalAgentToolCapability(["memory", "memory"], "memory", true)).toEqual([
      "memory",
    ]);
    expect(
      setOptionalAgentToolCapability(OPTIONAL_AGENT_TOOL_CAPABILITIES, "memory", true),
    ).toHaveLength(9);
    expect(setOptionalAgentToolCapability([], "memory", false)).toEqual([]);
  });
  it("disabling any capability never broadens access", () => {
    for (const capability of OPTIONAL_AGENT_TOOL_CAPABILITIES) {
      const narrowed = setOptionalAgentToolCapability(
        OPTIONAL_AGENT_TOOL_CAPABILITIES,
        capability,
        false,
      );
      expect(narrowed).toHaveLength(8);
      expect(narrowed).not.toContain(capability);
      expect(narrowed.every((entry) => OPTIONAL_AGENT_TOOL_CAPABILITIES.includes(entry))).toBe(
        true,
      );
    }
  });
  it("compares the selected flag rather than entire capability arrays", () => {
    expect(
      optionalAgentToolCapabilityValue(
        [{ agentToolCapabilities: ["memory", "knowledge"] }, { agentToolCapabilities: ["memory"] }],
        "memory",
      ),
    ).toBe(true);
    expect(
      optionalAgentToolCapabilityValue(
        [{ agentToolCapabilities: ["knowledge"] }, { agentToolCapabilities: [] }],
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
