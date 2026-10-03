import { expect, it } from "@effect/vitest";
import {
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
import * as Stream from "effect/Stream";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Quality from "../../../orchestration/QualityRecords.ts";
import * as Snapshots from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Mcp from "../../McpInvocationContext.ts";
import migrate from "../../../persistence/Migrations/059_QualityRecords.ts";
import { QualityRecordsToolkit } from "./tools.ts";
import { QualityRecordsToolkitHandlersLive } from "./handlers.ts";
const threadId = ThreadId.make("quality-authenticated-thread");
const scope: Mcp.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment"),
  threadId,
  providerSessionId: "session",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["quality-records"]),
  issuedAt: 1,
};
const decodeShell = Schema.decodeUnknownEffect(OrchestrationThreadShell);
const snapshots = Layer.mock(Snapshots.ProjectionSnapshotQuery)({
  getThreadShellById: () =>
    decodeShell({
      id: threadId,
      projectId: ProjectId.make("project"),
      title: "Thread",
      modelSelection: { instanceId: "codex", model: "gpt-5" },
      runtimeMode: "full-access",
      branch: null,
      worktreePath: null,
      latestTurn: null,
      createdAt: "2026-10-03T00:00:00.000Z",
      updatedAt: "2026-10-03T00:00:00.000Z",
      session: null,
      latestUserMessageAt: null,
      hasPendingApprovals: false,
      hasPendingUserInput: false,
      hasActionableProposedPlan: false,
    }).pipe(Effect.orDie, Effect.map(Option.some)),
});
const qualityLayer = Quality.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const services = Layer.merge(qualityLayer, snapshots);
it.effect(
  "quality MCP records remain caller-owned agent claims even with forged verification and thread fields",
  () =>
    Effect.gen(function* () {
      yield* migrate;
      const quality = yield* Quality.QualityRecords;
      const toolkit = yield* QualityRecordsToolkit.pipe(
        Effect.provide(QualityRecordsToolkitHandlersLive.pipe(Layer.provide(services))),
      );
      const spoofed = {
        threadId: ThreadId.make("foreign"),
        source: "user-reported",
        independentlyVerified: true,
      };
      yield* toolkit
        .handle("quality_records_update", {
          operationId: "quality-op",
          expectedRevision: 0,
          intention: "Agent claim",
          todos: [
            {
              id: "todo",
              content: "Claim tests passed",
              group: null,
              status: "completed",
              priority: "high",
              confidence: "verified",
              completionConfidence: "verified",
            },
          ],
          ...spoofed,
        })
        .pipe(
          Stream.unwrap,
          Stream.runDrain,
          Effect.provideService(Mcp.McpInvocationContext, scope),
        );
      const stored = yield* quality.get({ threadId }, { threadId, source: "agent-reported" });
      expect(stored).toMatchObject({
        threadId,
        source: "agent-reported",
        independentlyVerified: false,
      });
      expect(stored?.todos[0]?.confidenceHistory[0]?.source).toBe("agent-reported");
      const denied = yield* toolkit.handle("quality_records_get", {}).pipe(
        Stream.unwrap,
        Stream.runDrain,
        Effect.provideService(Mcp.McpInvocationContext, {
          ...scope,
          capabilities: new Set<Mcp.McpCapability>(),
        }),
        Effect.flip,
      );
      expect(denied).toMatchObject({
        _tag: "McpCapabilityUnavailableError",
        capability: "quality-records",
      });
    }).pipe(Effect.provide(services)),
);
