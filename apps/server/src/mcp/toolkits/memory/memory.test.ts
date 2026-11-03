import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/ai";

import * as MemoryService from "../../../memory/MemoryService.ts";
import * as Sqlite from "../../../persistence/Sqlite.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { liveThreadShell } from "../../McpToolAccess.testkit.ts";

const threadA = ThreadId.make("memory-thread-a");
const threadB = ThreadId.make("memory-thread-b");
const projectA = ProjectId.make("memory-project-a");
const projectB = ProjectId.make("memory-project-b");
const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "memory-test", version: "1" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "memory-test", version: "1" },
  },
  getClient: Effect.die("unused"),
});

const scope = (threadId: ThreadId, enabled = true): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("memory-environment"),
  requestNamespace: `memory:${threadId}`,
  thread: {
    threadId,
    providerSessionId: `session:${threadId}`,
    providerInstanceId: ProviderInstanceId.make("codex"),
  },
  client: undefined,
  capabilities: new Set(enabled ? ["memory"] : []),
  issuedAt: 0,
});

const call = (
  name: string,
  args: Record<string, unknown>,
  invocation: McpInvocationContext.McpInvocationScope,
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    return yield* server
      .callTool({ name, arguments: args })
      .pipe(
        Effect.provideService(McpInvocationContext.McpInvocationContext, invocation),
        Effect.provideService(McpSchema.McpServerClient, client),
      );
  });

const layer = it.layer(
  McpHttpServer.layerMemoryToolkit.pipe(
    Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
    Layer.provideMerge(MemoryService.layer.pipe(Layer.provideMerge(Sqlite.layerMemory))),
    Layer.provide(
      Layer.mock(ThreadManagement.ThreadManagementService)({
        getThreadShell: (id) =>
          Effect.succeed(
            id === ThreadId.make("missing")
              ? null
              : {
                  ...liveThreadShell(id, id === ThreadId.make("idle") ? { activeRunId: null } : {}),
                  projectId: id === threadB ? projectB : projectA,
                },
          ),
      }),
    ),
    Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
  ),
);

layer("memory MCP registration", (it) => {
  it.effect(
    "uses the request credential's project on every registered call, never tool input",
    () =>
      Effect.gen(function* () {
        const saved = yield* call(
          "memory_remember",
          { category: "fact", content: "MCP isolation note", projectId: projectB },
          scope(threadA),
        );
        expect(saved.isError).toBe(false);
        expect(saved.structuredContent).toMatchObject({
          projectId: projectA,
          sourceThreadId: threadA,
        });
        const stored = yield* MemoryService.MemoryService;
        const [entry] = (yield* stored.list({ projectId: projectA })).entries;
        expect(entry).toBeDefined();
        const recallB = yield* call("memory_recall", { query: "isolation" }, scope(threadB));
        expect(recallB.structuredContent).toEqual({ entries: [], truncated: false });
        const forgetB = yield* call("memory_forget", { id: entry!.id }, scope(threadB));
        expect(forgetB.structuredContent).toEqual({ forgotten: false });
        const recallA = yield* call(
          "memory_recall",
          { query: "isolation", category: null, limit: null },
          scope(threadA),
        );
        expect(recallA.isError).toBe(false);
        expect(recallA.structuredContent).toMatchObject({ entries: [{ id: entry!.id }] });
        expect(
          (yield* call("memory_forget", { id: entry!.id }, scope(threadA))).structuredContent,
        ).toEqual({ forgotten: true });
      }),
  );

  it.effect("refuses disabled, missing-thread, external and no-longer-active writers", () =>
    Effect.gen(function* () {
      for (const invocation of [
        scope(threadA, false),
        scope(ThreadId.make("missing")),
        scope(ThreadId.make("idle")),
        {
          ...scope(threadA),
          thread: undefined,
          client: { sessionId: "outside", label: "outside", access: "read-only" as const },
        },
      ]) {
        const saved = yield* call(
          "memory_remember",
          { category: "fact", content: "must not be saved" },
          invocation,
        );
        expect(saved.isError).toBe(true);
      }
      expect((yield* call("memory_recall", { query: "note" }, scope(threadA, false))).isError).toBe(
        true,
      );
      expect(
        (yield* call("memory_recall", { query: "note" }, scope(ThreadId.make("missing")))).isError,
      ).toBe(true);
      expect(
        (yield* (yield* MemoryService.MemoryService).list({ projectId: projectA })).entries,
      ).toEqual([]);
    }),
  );
});
