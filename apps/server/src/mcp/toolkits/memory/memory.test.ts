import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EnvironmentId,
  type OrchestrationV2ThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { McpSchema, McpServer } from "effect/unstable/ai";

import * as MemoryService from "../../../memory/MemoryService.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import { SqlitePersistenceMemory } from "../../../persistence/Layers/Sqlite.ts";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const threadA = ThreadId.make("thread-memory-a");
const threadB = ThreadId.make("thread-memory-b");
const projectOf: Record<string, ProjectId> = {
  [threadA]: ProjectId.make("project-memory-a"),
  [threadB]: ProjectId.make("project-memory-b"),
};

const TestLayer = McpHttpServer.MemoryToolkitRegistrationLive.pipe(
  Layer.provideMerge(McpServer.McpServer.layer),
  Layer.provideMerge(
    MemoryService.layer.pipe(
      Layer.provideMerge(SqlitePersistenceMemory),
      Layer.provideMerge(NodeServices.layer),
    ),
  ),
  Layer.provide(
    Layer.mock(Orchestrator.OrchestratorV2)({
      getThreadShell: (threadId) =>
        Effect.succeed(
          projectOf[threadId] === undefined
            ? null
            : ({ id: threadId, projectId: projectOf[threadId] } as OrchestrationV2ThreadShell),
        ),
    }),
  ),
);

const client = McpSchema.McpServerClient.of({
  clientId: 1,
  clientCapabilities: {},
  clientInfo: { name: "memory-test", version: "1.0.0" },
  protocolVersion: "2025-06-18",
  initializePayload: {
    protocolVersion: "2025-06-18",
    capabilities: {},
    clientInfo: { name: "memory-test", version: "1.0.0" },
  },
  getClient: Effect.die("unused"),
});

const call = (
  name: string,
  args: Record<string, unknown>,
  input: { readonly threadId: ThreadId; readonly memory: boolean },
) =>
  Effect.gen(function* () {
    const server = yield* McpServer.McpServer;
    const result = yield* server.callTool({ name, arguments: args }).pipe(
      Effect.provideService(McpInvocationContext.McpInvocationContext, {
        environmentId: EnvironmentId.make("environment-memory-test"),
        threadId: input.threadId,
        providerSessionId: "provider-session-memory-test",
        providerInstanceId: ProviderInstanceId.make("codex"),
        capabilities: new Set(input.memory ? (["memory"] as const) : ([] as const)),
        issuedAt: 1,
      }),
      Effect.provideService(McpSchema.McpServerClient, client),
    );
    return {
      isError: result.isError === true,
      structured: result.structuredContent as Record<string, unknown> | undefined,
      text: result.content.map((part) => ("text" in part ? part.text : "")).join("\n"),
    };
  });

it.layer(TestLayer)("memory MCP tools", (it) => {
  it.effect("remember, recall and forget through the registered toolkit", () =>
    Effect.gen(function* () {
      const remembered = yield* call(
        "memory_remember",
        { category: "fact", content: "Storybook runs on port 6006 via `vp run storybook`." },
        { threadId: threadA, memory: true },
      );
      assert.isFalse(remembered.isError, remembered.text);
      assert.strictEqual(remembered.structured?.projectId, projectOf[threadA]);
      const id = remembered.structured?.id;

      const recalled = yield* call(
        "memory_recall",
        { query: "storybook port" },
        { threadId: threadA, memory: true },
      );
      assert.isFalse(recalled.isError, recalled.text);
      const entries = recalled.structured?.entries as ReadonlyArray<{ readonly id: string }>;
      assert.deepEqual(
        entries.map((entry) => entry.id),
        [id],
      );

      // Another project's thread cannot see or delete it, even with the id.
      const otherRecall = yield* call(
        "memory_recall",
        { query: "storybook port" },
        { threadId: threadB, memory: true },
      );
      assert.deepEqual(otherRecall.structured?.entries, []);
      const otherForget = yield* call("memory_forget", { id }, { threadId: threadB, memory: true });
      assert.deepEqual(otherForget.structured, { forgotten: false });

      const forgotten = yield* call("memory_forget", { id }, { threadId: threadA, memory: true });
      assert.deepEqual(forgotten.structured, { forgotten: true });
      const empty = yield* call(
        "memory_recall",
        { query: "storybook" },
        { threadId: threadA, memory: true },
      );
      assert.deepEqual(empty.structured?.entries, []);
    }),
  );

  it.effect("refuses every memory tool when the credential lacks the memory capability", () =>
    Effect.gen(function* () {
      for (const [name, args] of [
        ["memory_remember", { category: "fact", content: "should not be stored" }],
        ["memory_recall", { query: "stored" }],
        ["memory_forget", { id: "mem_anything" }],
      ] as const) {
        const result = yield* call(name, args, { threadId: threadA, memory: false });
        assert.isTrue(result.isError, name);
        assert.include(result.text, "memory", name);
      }
      const after = yield* call(
        "memory_recall",
        { query: "should not be stored" },
        { threadId: threadA, memory: true },
      );
      assert.deepEqual(after.structured?.entries, []);
    }),
  );
});
