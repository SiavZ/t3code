import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";

import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { ServerSettingsService } from "../serverSettings.ts";

const environmentId = EnvironmentId.make("environment-1");
const makeFakeHttpServer = (hostname: string, port = 43123) =>
  HttpServer.HttpServer.of({
    address: NetAddress.inetAddressFromIpStringUnsafe(hostname, port),
    serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
  });
const fakeHttpServer = makeFakeHttpServer("127.0.0.1");
const fakeEnvironment = ServerEnvironment.ServerEnvironment.of({
  getEnvironmentId: Effect.succeed(environmentId),
  getDescriptor: Effect.die("unused"),
});

const makeRegistry = (now: () => number, httpServer = fakeHttpServer) =>
  McpSessionRegistry.__testing
    .make({
      now,
      livenessWindowMs: 100,
    })
    .pipe(
      Effect.provideService(HttpServer.HttpServer, httpServer),
      Effect.provideService(ServerEnvironment.ServerEnvironment, fakeEnvironment),
      Effect.provide(NodeServices.layer),
    );

it.effect("stores only a token hash, resolves the bearer token, and revokes by thread", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-1");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["preview"]),
    });
    expect(issued.config.endpoint).toBe("http://127.0.0.1:43123/mcp");
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    expect(token.length).toBeGreaterThan(20);

    const resolved = yield* registry.resolve(token);
    expect(resolved?.threadId).toBe(threadId);

    yield* registry.revokeThread(threadId);
    expect(yield* registry.resolve(token)).toBeUndefined();

    timestamp += 2_000;
  }),
);

it.effect("always grants pull-requests and gates browser and device access independently", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const withPreview = yield* registry.issue({
      threadId: ThreadId.make("thread-preview"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["preview"]),
    });
    const withoutPreview = yield* registry.issue({
      threadId: ThreadId.make("thread-no-preview"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(),
    });
    const withDevice = yield* registry.issue({
      threadId: ThreadId.make("thread-device"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["device"]),
    });
    const capabilitiesOf = (issued: typeof withPreview) =>
      registry
        .resolve(issued.config.authorizationHeader.replace(/^Bearer\s+/, ""))
        .pipe(Effect.map((scope) => [...(scope?.capabilities ?? [])].sort()));

    expect(yield* capabilitiesOf(withPreview)).toEqual(["preview", "pull-requests"]);
    expect(yield* capabilitiesOf(withoutPreview)).toEqual(["pull-requests"]);
    expect(yield* capabilitiesOf(withDevice)).toEqual(["device", "pull-requests"]);
  }),
);

it.effect("builds MCP endpoints from the bound server host", () =>
  Effect.gen(function* () {
    const cases = [
      ["100.64.0.40", "http://100.64.0.40:43123/mcp"],
      ["0.0.0.0", "http://127.0.0.1:43123/mcp"],
      ["::", "http://127.0.0.1:43123/mcp"],
      ["::1", "http://[::1]:43123/mcp"],
      ["127.0.0.1", "http://127.0.0.1:43123/mcp"],
    ] as const;

    for (const [hostname, expectedEndpoint] of cases) {
      const registry = yield* makeRegistry(() => 1_000, makeFakeHttpServer(hostname));
      const issued = yield* registry.issue({
        threadId: ThreadId.make(`thread-${hostname}`),
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["preview"]),
      });
      expect(issued.config.endpoint).toBe(expectedEndpoint);
    }
  }),
);

it.effect("does not auto-grant pull requests or workers beyond an issuance ceiling", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry(() => 1_000);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("restricted-worker"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["workers", "preview", "pull-requests"]),
      capabilityCeiling: new Set(["preview"]),
    });
    const resolved = yield* registry.resolve(issued.config.authorizationHeader.slice(7));
    expect([...resolved!.capabilities]).toEqual(["preview"]);
    yield* registry.restrictThreadCapabilities(
      issued.config.threadId,
      new Set(["workers", "device"]),
    );
    expect([
      ...(yield* registry.resolve(issued.config.authorizationHeader.slice(7)))!.capabilities,
    ]).toEqual([]);
    yield* registry.restrictThreadCapabilities(
      issued.config.threadId,
      new Set(["preview", "workers"]),
    );
    expect([
      ...(yield* registry.resolve(issued.config.authorizationHeader.slice(7)))!.capabilities,
    ]).toEqual([]);
  }),
);

it.effect(
  "rechecks live settings and ancestor ceilings on every worker credential use without re-expanding a token",
  () =>
    Effect.gen(function* () {
      const rootId = ThreadId.make("root");
      const childId = ThreadId.make("child");
      const projectId = ProjectId.make("project");
      const decodeThread = Schema.decodeUnknownSync(OrchestrationThreadShell);
      const root = decodeThread({
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
        createdAt: "2026-10-02T00:00:00.000Z",
        updatedAt: "2026-10-02T00:00:00.000Z",
      });
      let child = decodeThread({
        ...root,
        id: childId,
        worker: {
          ownerThreadId: rootId,
          rootThreadId: rootId,
          depth: 1,
          spawnCommandId: CommandId.make("spawn"),
          spawnFingerprint: "spawn",
          label: "Child",
          runtimeModeCeiling: "full-access",
          mcpCapabilityCeiling: ["preview", "workers"],
          stopRequestedAt: null,
          lastStopSequence: null,
        },
      });
      let settings = {
        ...DEFAULT_SERVER_SETTINGS,
        enableAgentBrowserAccess: true,
        enableAgentDeviceAccess: true,
      };
      const registry = yield* makeRegistry(() => 1_000).pipe(
        Effect.provide(
          Layer.mergeAll(
            Layer.mock(ServerSettingsService)({ getSettings: Effect.sync(() => settings) }),
            Layer.mock(ProjectionSnapshotQuery)({
              getThreadShellById: (id) =>
                Effect.succeed(
                  Option.fromNullishOr(id === rootId ? root : id === childId ? child : undefined),
                ),
            }),
          ),
        ),
      );
      const issued = yield* registry.issue({
        threadId: childId,
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(["preview", "device", "pull-requests", "workers"]),
      });
      const token = issued.config.authorizationHeader.slice(7);
      expect([...(yield* registry.resolve(token))!.capabilities].sort()).toEqual([
        "preview",
        "workers",
      ]);
      settings = { ...settings, enableAgentBrowserAccess: false };
      expect([...(yield* registry.resolve(token))!.capabilities]).toEqual(["workers"]);
      settings = { ...settings, enableAgentBrowserAccess: true };
      expect([...(yield* registry.resolve(token))!.capabilities]).toEqual(["workers"]);
      child = {
        ...child,
        worker: { ...child.worker!, stopRequestedAt: "2026-10-02T01:00:00.000Z" },
      };
      expect([...(yield* registry.resolve(token))!.capabilities]).toEqual([]);
      child = { ...child, worker: { ...child.worker!, stopRequestedAt: null } };
      expect([...(yield* registry.resolve(token))!.capabilities]).toEqual([]);
    }),
);

it.effect("expires credentials once their session stops showing signs of life", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-2"),
      providerInstanceId: ProviderInstanceId.make("claude"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");
    timestamp += 101;
    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);

it.effect("keeps a credential alive across turns that never touch an MCP tool", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const threadId = ThreadId.make("thread-3");
    const issued = yield* registry.issue({
      threadId,
      providerInstanceId: ProviderInstanceId.make("claude"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    // Well past the liveness window in total, but each turn reports in before
    // it lapses — this is the long-session case that used to lose the toolkit.
    for (let turn = 0; turn < 10; turn += 1) {
      timestamp += 99;
      yield* registry.touch(threadId);
    }

    expect((yield* registry.resolve(token))?.threadId).toBe(threadId);
  }),
);

it.effect("does not keep credentials of other threads alive", () =>
  Effect.gen(function* () {
    let timestamp = 1_000;
    const registry = yield* makeRegistry(() => timestamp);
    const issued = yield* registry.issue({
      threadId: ThreadId.make("thread-4"),
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["preview"]),
    });
    const token = issued.config.authorizationHeader.replace(/^Bearer\s+/, "");

    timestamp += 99;
    yield* registry.touch(ThreadId.make("thread-unrelated"));
    timestamp += 2;

    expect(yield* registry.resolve(token)).toBeUndefined();
  }),
);
