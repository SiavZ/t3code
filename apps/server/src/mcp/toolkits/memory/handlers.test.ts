import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThreadShell,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Memory from "../../../memory/Memory.ts";
import * as Snapshots from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Mcp from "../../McpInvocationContext.ts";
import migrate from "../../../persistence/Migrations/058_Memory.ts";
import { MemoryToolkit } from "./tools.ts";
import { MemoryToolkitHandlersLive } from "./handlers.ts";
const threadId = ThreadId.make("authenticated-thread");
const projectId = ProjectId.make("authenticated-project");
const scope: Mcp.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment"),
  threadId,
  providerSessionId: "session",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["memory"]),
  issuedAt: 1,
};
const database = NodeSqliteClient.layer({ filename: ":memory:" });
const memoryLayer = Memory.layer.pipe(Layer.provideMerge(database));
// Only the projected owner lookup is external to the memory capability under test.
const shell: OrchestrationThreadShell = {
  id: threadId,
  projectId,
  title: "Authenticated thread",
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  pullRequests: [],
  latestTurn: null,
  createdAt: "2026-10-03T00:00:00.000Z",
  updatedAt: "2026-10-03T00:00:00.000Z",
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
};
const snapshots = Layer.mock(Snapshots.ProjectionSnapshotQuery)({
  getThreadShellById: () => Effect.succeed(Option.some(shell)),
});
const services = Layer.merge(memoryLayer, snapshots);
it.effect(
  "authenticated memory tools derive project authority and never grant global access from JSON",
  () =>
    Effect.gen(function* () {
      yield* migrate;
      const memory = yield* Memory.MemoryService;
      const toolkit = yield* MemoryToolkit.pipe(
        Effect.provide(MemoryToolkitHandlersLive.pipe(Layer.provide(services))),
      );
      const forgedAuthority = { projectId: "spoofed-project", allowGlobal: true };
      yield* toolkit
        .handle("memory_remember", {
          id: "tool-memory",
          operationId: "tool-op",
          scope: "project",
          category: "fact",
          content: "explicit sqlite tool memory",
          tags: [],
          ...forgedAuthority,
        })
        .pipe(
          Stream.unwrap,
          Stream.runDrain,
          Effect.provideService(Mcp.McpInvocationContext, scope),
        );
      const saved = yield* memory.search({ query: "sqlite" }, { projectId, allowGlobal: false });
      expect(saved.entries[0]?.projectId).toBe(projectId);
      expect(saved.entries[0]?.sourceThreadId).toBe(threadId);
      const denied = yield* toolkit
        .handle("memory_remember", {
          id: "tool-global",
          operationId: "global-op",
          scope: "global",
          category: "fact",
          content: "not permitted",
          tags: [],
          ...forgedAuthority,
        })
        .pipe(
          Stream.unwrap,
          Stream.runDrain,
          Effect.provideService(Mcp.McpInvocationContext, scope),
          Effect.flip,
        );
      expect(denied).toMatchObject({ _tag: "MemoryError", code: "forbidden" });
      const unavailable = yield* toolkit.handle("memory_search", { query: "" }).pipe(
        Stream.unwrap,
        Stream.runDrain,
        Effect.provideService(Mcp.McpInvocationContext, {
          ...scope,
          capabilities: new Set<Mcp.McpCapability>(),
        }),
        Effect.flip,
      );
      expect(unavailable).toMatchObject({
        _tag: "McpCapabilityUnavailableError",
        capability: "memory",
      });
    }).pipe(Effect.provide(services)),
);
