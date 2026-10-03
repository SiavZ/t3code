import { OPTIONAL_AGENT_TOOL_CAPABILITIES } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";
import {
  optionalAgentToolCapabilityValue,
  setOptionalAgentToolCapability,
} from "./agentToolSettings.ts";

describe("optional agent tool settings", () => {
  it("returns a new target array without mutating the input", () => {
    const enabled = ["memory"] as const;
    const disabled = [] as const;
    expect(setOptionalAgentToolCapability(enabled, "memory", false)).toEqual([]);
    expect(setOptionalAgentToolCapability(disabled, "memory", true)).toEqual(["memory"]);
    expect(enabled).toEqual(["memory"]);
    expect(disabled).toEqual([]);
  });
  it("is idempotent, deduplicated and bounded by the capability catalog", () => {
    expect(setOptionalAgentToolCapability(["memory", "memory"], "memory", true)).toEqual([
      "memory",
    ]);
    expect(
      setOptionalAgentToolCapability(OPTIONAL_AGENT_TOOL_CAPABILITIES, "memory", true),
    ).toHaveLength(1);
    expect(setOptionalAgentToolCapability([], "memory", false)).toEqual([]);
  });
  it("disabling any capability never broadens access", () => {
    for (const capability of OPTIONAL_AGENT_TOOL_CAPABILITIES) {
      const narrowed = setOptionalAgentToolCapability(
        OPTIONAL_AGENT_TOOL_CAPABILITIES,
        capability,
        false,
      );
      expect(narrowed).toHaveLength(0);
      expect(narrowed).not.toContain(capability);
    }
  });
  it("reports a shared value only when every target agrees", () => {
    expect(
      optionalAgentToolCapabilityValue(
        [{ agentToolCapabilities: ["memory"] }, { agentToolCapabilities: ["memory"] }],
        "memory",
      ),
    ).toBe(true);
    expect(
      optionalAgentToolCapabilityValue(
        [{ agentToolCapabilities: [] }, { agentToolCapabilities: [] }],
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
