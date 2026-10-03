import {
  DEFAULT_SERVER_SETTINGS,
  type EnvironmentId,
  type ProjectId,
  type ServerSettings,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import type { SettingsTarget } from "./settings-environment-filter";
import {
  planMobileScopedSettingsClear,
  planMobileScopedSettingsPatch,
  planMobileAgentToolCapability,
  resolveMobileSettingsTargets,
} from "./settings-scoped-server";

const firstId = "first" as EnvironmentId;
const secondId = "second" as EnvironmentId;
const firstProject = "first-project" as ProjectId;
const secondProject = "second-project" as ProjectId;

function environment(environmentId: EnvironmentId, settings: ServerSettings): SettingsTarget {
  return {
    environmentId,
    serverConfig: {
      settings,
      environment: { capabilities: { projectSettingsOverrides: true } },
    },
  } as SettingsTarget;
}

describe("mobile project settings scope", () => {
  it("edits global memory only at environment scope", () => {
    const selected = [
      environment(firstId, DEFAULT_SERVER_SETTINGS),
      environment(secondId, DEFAULT_SERVER_SETTINGS),
    ];
    const environmentTargets = resolveMobileSettingsTargets(selected, null);
    expect(
      planMobileScopedSettingsPatch(environmentTargets, false, { enableGlobalMemory: true }),
    ).toEqual([
      { environmentId: firstId, patch: { enableGlobalMemory: true } },
      { environmentId: secondId, patch: { enableGlobalMemory: true } },
    ]);
    const projectTargets = resolveMobileSettingsTargets(selected, [
      { environmentId: firstId, id: firstProject },
    ]);
    expect(
      planMobileScopedSettingsPatch(projectTargets, true, { enableGlobalMemory: true }),
    ).toEqual([]);
  });
  it("turns recall off independently and can restore inherited recall without removing memory", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      agentToolCapabilities: ["memory"],
      enableMemoryAutoRecall: true,
      projectSettingsOverrides: {
        [firstProject]: { enableMemoryAutoRecall: false, defaultAutoPull: true },
      },
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, settings)],
      [{ environmentId: firstId, id: firstProject }],
    );
    expect(targets[0]?.settings.enableMemoryAutoRecall).toBe(false);
    expect(targets[0]?.settings.agentToolCapabilities).toEqual(["memory"]);
    expect(
      planMobileScopedSettingsPatch(targets, true, { enableMemoryAutoRecall: false })[0]?.patch
        .projectSettingsOverrides?.[firstProject],
    ).toEqual({ enableMemoryAutoRecall: false, defaultAutoPull: true });
    expect(
      planMobileScopedSettingsClear(targets, ["enableMemoryAutoRecall"])[0]?.patch
        .projectSettingsOverrides?.[firstProject],
    ).toEqual({ defaultAutoPull: true });
    expect(DEFAULT_SERVER_SETTINGS.enableMemoryAutoRecall).toBe(false);
    expect(DEFAULT_SERVER_SETTINGS.agentToolCapabilities).toEqual([]);
  });
  it("preserves each target's optional flags and merges multiple project writes on one environment", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      agentToolCapabilities: ["quality-records"],
      projectSettingsOverrides: {
        [firstProject]: { agentToolCapabilities: [], enableMemoryAutoRecall: true },
        [secondProject]: { defaultAutoPull: true },
      },
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, settings)],
      [
        { environmentId: firstId, id: firstProject },
        { environmentId: firstId, id: secondProject },
      ],
    );
    expect(planMobileAgentToolCapability(targets, true, "memory", true)).toEqual([
      {
        environmentId: firstId,
        patch: {
          projectSettingsOverrides: {
            [firstProject]: {
              agentToolCapabilities: ["memory"],
              enableMemoryAutoRecall: true,
            },
            [secondProject]: {
              defaultAutoPull: true,
              agentToolCapabilities: ["memory", "quality-records"],
            },
          },
        },
      },
    ]);
    expect(
      planMobileScopedSettingsClear(targets, ["agentToolCapabilities", "enableMemoryAutoRecall"])[0]
        ?.patch.projectSettingsOverrides,
    ).toEqual({ [firstProject]: null, [secondProject]: { defaultAutoPull: true } });
  });
  it("fans out environment capabilities without changing other flags", () => {
    const targets = resolveMobileSettingsTargets(
      [
        environment(firstId, {
          ...DEFAULT_SERVER_SETTINGS,
          agentToolCapabilities: ["memory", "quality-records"],
        }),
        environment(secondId, {
          ...DEFAULT_SERVER_SETTINGS,
          agentToolCapabilities: [],
        }),
      ],
      null,
    );
    expect(
      planMobileAgentToolCapability(targets, false, "memory", false).map(
        (write) => write.patch.agentToolCapabilities,
      ),
    ).toEqual([["quality-records"], []]);
  });
  it("edits each checkout's own override without changing either environment default", () => {
    const firstSettings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      responseStreamingMode: "paragraph",
      projectSettingsOverrides: { [firstProject]: { defaultAutoPull: true } },
    };
    const secondSettings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      responseStreamingMode: "token",
      projectSettingsOverrides: {},
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, firstSettings), environment(secondId, secondSettings)],
      [
        { environmentId: firstId, id: firstProject },
        { environmentId: secondId, id: secondProject },
      ],
    );

    const writes = planMobileScopedSettingsPatch(targets, true, {
      responseStreamingMode: "turn",
    });
    expect(writes).toEqual([
      {
        environmentId: firstId,
        patch: {
          projectSettingsOverrides: {
            [firstProject]: { defaultAutoPull: true, responseStreamingMode: "turn" },
          },
        },
      },
      {
        environmentId: secondId,
        patch: { projectSettingsOverrides: { [secondProject]: { responseStreamingMode: "turn" } } },
      },
    ]);
    expect(firstSettings.responseStreamingMode).toBe("paragraph");
    expect(secondSettings.responseStreamingMode).toBe("token");
  });

  it("removes a project override when a picker sends null for a key that cannot store it", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: {
        [firstProject]: { defaultThreadEnvMode: "worktree", defaultAutoPull: true },
      },
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, settings)],
      [{ environmentId: firstId, id: firstProject }],
    );
    expect(planMobileScopedSettingsPatch(targets, true, { defaultThreadEnvMode: null })).toEqual([
      {
        environmentId: firstId,
        patch: { projectSettingsOverrides: { [firstProject]: { defaultAutoPull: true } } },
      },
    ]);
    expect(planMobileScopedSettingsPatch(targets, true, { defaultModelSelection: null })).toEqual([
      {
        environmentId: firstId,
        patch: {
          projectSettingsOverrides: {
            [firstProject]: {
              defaultThreadEnvMode: "worktree",
              defaultAutoPull: true,
              defaultModelSelection: null,
            },
          },
        },
      },
    ]);
  });

  it("resets only the selected page's override and rejects environment-wide writes", () => {
    const settings: ServerSettings = {
      ...DEFAULT_SERVER_SETTINGS,
      projectSettingsOverrides: {
        [firstProject]: { defaultAutoPull: true, responseStreamingMode: "turn" },
      },
    };
    const targets = resolveMobileSettingsTargets(
      [environment(firstId, settings)],
      [{ environmentId: firstId, id: firstProject }],
    );

    expect(planMobileScopedSettingsClear(targets, ["responseStreamingMode"])).toEqual([
      {
        environmentId: firstId,
        patch: { projectSettingsOverrides: { [firstProject]: { defaultAutoPull: true } } },
      },
    ]);
    expect(
      planMobileScopedSettingsPatch(targets, true, { enableProviderUpdateChecks: false }),
    ).toEqual([]);
  });
});
