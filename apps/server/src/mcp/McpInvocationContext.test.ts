import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  McpCapabilityUnavailableError,
  PreviewAutomationUnavailableError,
  ProviderInstanceId,
  ThreadId,
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  OrchestrationThreadShell,
  ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ServerSettingsService } from "../serverSettings.ts";

import * as McpInvocationContext from "./McpInvocationContext.ts";

it.effect("rechecks explicit project opt-ins without expanding immutable worker ceilings", () =>
  Effect.gen(function* () {
    const projectId = ProjectId.make("optional-tools-project");
    const rootId = ThreadId.make("optional-tools-root");
    const workerId = ThreadId.make("optional-tools-worker");
    const root = Schema.decodeUnknownSync(OrchestrationThreadShell)({
      id: rootId,
      projectId,
      title: "Root",
      modelSelection: { instanceId: "codex", model: "gpt-6.1" },
      runtimeMode: "full-access",
      branch: null,
      worktreePath: null,
      latestTurn: null,
      session: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
      createdAt: "2026-10-03T00:00:00.000Z",
      updatedAt: "2026-10-03T00:00:00.000Z",
    });
    const worker = Schema.decodeUnknownSync(OrchestrationThreadShell)({
      ...root,
      id: workerId,
      worker: {
        ownerThreadId: rootId,
        rootThreadId: rootId,
        depth: 1,
        spawnCommandId: CommandId.make("optional-tools-spawn"),
        spawnFingerprint: "optional-tools-spawn",
        label: "Worker",
        runtimeModeCeiling: "full-access",
        mcpCapabilityCeiling: ["preview", "workers"],
        stopRequestedAt: null,
        lastStopSequence: null,
      },
    });
    let settings = { ...DEFAULT_SERVER_SETTINGS, enableAgentBrowserAccess: false };
    const resolve = yield* McpInvocationContext.makeThreadMcpCapabilities.pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.mock(ServerSettingsService)({ getSettings: Effect.sync(() => settings) }),
          Layer.mock(ProjectionSnapshotQuery)({
            getThreadShellById: (id) =>
              Effect.succeed(
                Option.fromNullishOr(id === rootId ? root : id === workerId ? worker : undefined),
              ),
          }),
        ),
      ),
    );
    expect((yield* resolve(rootId))?.has("preview")).toBe(false);
    settings = { ...settings, enableAgentBrowserAccess: true };
    expect((yield* resolve(rootId))?.has("preview")).toBe(true);
    expect([...(yield* resolve(workerId))!].sort()).toEqual(["preview", "workers"]);
    settings = {
      ...settings,
      projectSettingsOverrides: { [projectId]: { enableAgentBrowserAccess: false } },
    };
    expect([...(yield* resolve(workerId))!]).toEqual(["workers"]);
    expect((yield* resolve(ThreadId.make("missing")))?.has("workers")).toBe(false);
  }),
);

it.effect("reports the scoped credential context when preview capability is unavailable", () => {
  const invocation: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
    capabilities: new Set(),
    issuedAt: 1,
  };

  return Effect.gen(function* () {
    const error = yield* McpInvocationContext.requireMcpCapability("preview").pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.flip,
    );

    expect(error).toBeInstanceOf(PreviewAutomationUnavailableError);
    expect(error).toMatchObject({
      capability: "preview",
      environmentId: invocation.environmentId,
      threadId: invocation.threadId,
      providerSessionId: invocation.providerSessionId,
      providerInstanceId: invocation.providerInstanceId,
    });
    expect(error.message).toContain("MCP credential does not grant the preview capability");
    expect(error.message).toContain("use a headless browser from the shell");
  });
});

it.effect("reports other missing capabilities with the neutral error", () => {
  const invocation: McpInvocationContext.McpInvocationScope = {
    environmentId: EnvironmentId.make("environment-1"),
    threadId: ThreadId.make("thread-1"),
    providerSessionId: "provider-session-1",
    providerInstanceId: ProviderInstanceId.make("codex"),
    capabilities: new Set(["preview"]),
    issuedAt: 1,
  };

  return Effect.gen(function* () {
    const error = yield* McpInvocationContext.requireMcpCapability("pull-requests").pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
      Effect.flip,
    );

    expect(error).toBeInstanceOf(McpCapabilityUnavailableError);
    expect(error).toMatchObject({ capability: "pull-requests", threadId: invocation.threadId });

    const scope = yield* McpInvocationContext.requireMcpCapability("preview").pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
    );
    expect(scope).toBe(invocation);
  });
});
