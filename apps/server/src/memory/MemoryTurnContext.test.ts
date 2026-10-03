import { assert, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationThreadShell,
  type ServerSettings,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Ref from "effect/Ref";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Snapshots from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Settings from "../serverSettings.ts";
import migrate from "../persistence/Migrations/057_Memory.ts";
import migrateProjections from "../persistence/Migrations/005_Projections.ts";
import migrateAuthority from "../persistence/Migrations/059_ScheduledWork.ts";
import * as Memory from "./Memory.ts";
import { makeMemoryTurnContext } from "./MemoryTurnContext.ts";
const projectId = ProjectId.make("turn-project");
const threadId = ThreadId.make("turn-root");
const workerId = ThreadId.make("turn-worker");
const shell: OrchestrationThreadShell = {
  id: threadId,
  projectId,
  title: "Turn context",
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
const worker: OrchestrationThreadShell = {
  ...shell,
  id: workerId,
  worker: {
    ownerThreadId: threadId,
    rootThreadId: threadId,
    depth: 1,
    spawnCommandId: CommandId.make("spawn"),
    spawnFingerprint: "fixture",
    label: "Restricted worker",
    runtimeModeCeiling: "full-access",
    mcpCapabilityCeiling: ["workers"],
    stopRequestedAt: null,
    lastStopSequence: null,
  },
};
const context = (settings: Ref.Ref<ServerSettings>) =>
  makeMemoryTurnContext.pipe(
    Effect.provide(
      Layer.merge(
        Layer.mock(Settings.ServerSettingsService)({ getSettings: Ref.get(settings) }),
        Layer.mock(Snapshots.ProjectionSnapshotQuery)({
          getThreadShellById: (id) =>
            Effect.succeed(
              id === threadId
                ? Option.some(shell)
                : id === workerId
                  ? Option.some(worker)
                  : Option.none(),
            ),
        }),
      ),
    ),
  );
const services = Memory.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
);
const authority = { projectId, threadId, allowGlobal: false };
it.layer(services)("Memory turn context", (it) => {
  it.effect(
    "default off and project opt-in append reference context without mutating persisted user text",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        yield* migrateAuthority;
        yield* migrateProjections;
        const memory = yield* Memory.MemoryService;
        const sql = yield* SqlClient.SqlClient;
        yield* memory.remember(
          {
            id: "turn-opt-in",
            operationId: "opt-in",
            scope: "project",
            category: "fact",
            content: "SQLite lexical context",
            tags: [],
          },
          authority,
        );
        const settings = yield* Ref.make<ServerSettings>(DEFAULT_SERVER_SETTINGS);
        const append = yield* context(settings);
        const text = "Explain SQLite";
        yield* sql`INSERT INTO projection_thread_messages VALUES('turn-message',${threadId},NULL,'user',${text},0,'2026-10-03T00:00:00.000Z','2026-10-03T00:00:00.000Z')`;
        assert.equal(yield* append(threadId, text), text);
        yield* Ref.set(settings, {
          ...DEFAULT_SERVER_SETTINGS,
          projectSettingsOverrides: {
            [projectId]: {
              enableMemoryAutoRecall: true,
              agentToolCapabilities: ["memory"] as const,
            },
          },
        });
        const providerText = yield* append(threadId, text);
        assert.ok(providerText.startsWith(text));
        assert.ok(providerText.includes('<project-memory-context mode="lexical-context">'));
        assert.ok(providerText.includes("SQLite lexical context"));
        const persisted = yield* sql<{
          text: string;
        }>`SELECT text FROM projection_thread_messages WHERE message_id='turn-message'`;
        assert.equal(persisted[0]?.text, text);
      }),
  );
  it.effect("live capability and worker ceilings deny recall even when enabled", () =>
    Effect.gen(function* () {
      yield* migrate;
      yield* migrateAuthority;
      const memory = yield* Memory.MemoryService;
      yield* memory.remember(
        {
          id: "turn-ceiling",
          operationId: "ceiling",
          scope: "project",
          category: "fact",
          content: "Restricted ceiling memory",
          tags: [],
        },
        authority,
      );
      const settings = yield* Ref.make<ServerSettings>({
        ...DEFAULT_SERVER_SETTINGS,
        enableMemoryAutoRecall: true,
      });
      const append = yield* context(settings);
      const text = "Restricted ceiling";
      assert.equal(yield* append(threadId, text), text);
      yield* Ref.update(settings, (current) => ({
        ...current,
        agentToolCapabilities: ["memory"] as const,
      }));
      assert.ok((yield* append(threadId, text)).includes("Restricted ceiling memory"));
      assert.equal(yield* append(workerId, text), text);
      yield* Ref.update(settings, (current) => ({
        ...current,
        projectSettingsOverrides: { [projectId]: { enableMemoryAutoRecall: false } },
      }));
      assert.equal(yield* append(threadId, text), text);
    }),
  );
  it.effect(
    "forget is reflected on the next turn, blank queries and global memories are not injected",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        yield* migrateAuthority;
        const memory = yield* Memory.MemoryService;
        yield* memory.remember(
          {
            id: "turn-forget",
            operationId: "forget-seed",
            scope: "project",
            category: "fact",
            content: "Forgettoken project memory",
            tags: [],
          },
          authority,
        );
        yield* memory.remember(
          {
            id: "turn-global",
            operationId: "global-seed",
            scope: "global",
            category: "fact",
            content: "Globaltoken privileged memory",
            tags: [],
          },
          { ...authority, allowGlobal: true },
        );
        const settings = yield* Ref.make<ServerSettings>({
          ...DEFAULT_SERVER_SETTINGS,
          enableMemoryAutoRecall: true,
          agentToolCapabilities: ["memory"] as const,
        });
        const append = yield* context(settings);
        assert.ok((yield* append(threadId, "Forgettoken")).includes("Forgettoken project memory"));
        yield* memory.forget(
          { id: "turn-forget", operationId: "forget-turn", expectedRevision: 1 },
          authority,
        );
        assert.equal(yield* append(threadId, "Forgettoken"), "Forgettoken");
        assert.equal(yield* append(threadId, "Globaltoken"), "Globaltoken");
        assert.equal(yield* append(threadId, "  "), "  ");
        assert.equal(yield* append(threadId, "!!!"), "!!!");
      }),
  );
  it.effect(
    "bounds appended context and storage failure falls through to the original prompt",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        yield* migrateAuthority;
        const memory = yield* Memory.MemoryService;
        const sql = yield* SqlClient.SqlClient;
        for (let n = 0; n < 10; n++)
          yield* memory.remember(
            {
              id: `turn-budget-${n}`,
              operationId: `budget-${n}`,
              scope: "project",
              category: "fact",
              content: `Budgettoken ${"reference ".repeat(100)}`,
              tags: [],
            },
            authority,
          );
        const settings = yield* Ref.make<ServerSettings>({
          ...DEFAULT_SERVER_SETTINGS,
          enableMemoryAutoRecall: true,
          agentToolCapabilities: ["memory"] as const,
        });
        const append = yield* context(settings);
        const text = "Budgettoken";
        const providerText = yield* append(threadId, text);
        assert.ok(providerText.includes("project-memory-context"));
        assert.ok(providerText.length - text.length <= 6300);
        yield* sql`DROP TABLE memory_entries`;
        assert.equal(yield* append(threadId, text), text);
      }),
  );
});
