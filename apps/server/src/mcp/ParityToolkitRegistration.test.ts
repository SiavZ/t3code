import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  OrchestrationThreadShell,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as McpServer from "effect/unstable/ai/McpServer";
import * as McpSchema from "effect/unstable/ai/McpSchema";
import * as Tool from "effect/unstable/ai/Tool";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { migrateAgentDocuments as migrate } from "../testUtils/agentDocumentsSchema.ts";
import * as Documents from "../orchestration/AgentDocuments.ts";
import * as Assets from "../orchestration/AgentDocumentAssets.ts";
import * as Snapshots from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as Invocation from "./McpInvocationContext.ts";
import { AgentDocumentsToolkit } from "./toolkits/agentDocuments/tools.ts";
import { AgentDocumentsToolkitHandlersLive } from "./toolkits/agentDocuments/handlers.ts";
import { CoordinationToolkit } from "./toolkits/coordination/tools.ts";
import { WorkersToolkit } from "./toolkits/workers/tools.ts";
import { AutomationToolkit } from "./toolkits/automation/tools.ts";
import { MemoryToolkit } from "./toolkits/memory/tools.ts";
import { QualityRecordsToolkit } from "./toolkits/qualityRecords/tools.ts";
import { KnowledgeToolkit } from "./toolkits/knowledge/tools.ts";
import { ExternalMcpToolkit } from "./toolkits/externalMcp/tools.ts";
import { RuntimeToolkit } from "./toolkits/runtime/tools.ts";

const decodeToolJsonSchema = Schema.decodeUnknownEffect(McpSchema.ToolJsonSchema);
const owner = ThreadId.make("document-owner");
const shell = Schema.decodeUnknownSync(OrchestrationThreadShell)({
  id: owner,
  projectId: "project",
  title: "Owner",
  modelSelection: { instanceId: "codex", model: "model" },
  runtimeMode: "approval-required",
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
});
const invocation: Invocation.McpInvocationScope = {
  environmentId: EnvironmentId.make("environment"),
  threadId: owner,
  providerSessionId: "session",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(["agent-documents"]),
  issuedAt: 1,
};
const database = NodeSqliteClient.layer({ filename: ":memory:" });
const services = Layer.mergeAll(
  Documents.layer.pipe(Layer.provide(database)),
  database,
  Layer.mock(Snapshots.ProjectionSnapshotQuery)({
    getThreadShellById: (id) => Effect.succeed(id === owner ? Option.some(shell) : Option.none()),
  }),
  Layer.mock(Assets.AgentDocumentAssets)({
    prepare: () => Effect.die("PDF preparation not called"),
  }),
);
const registeredFor = (scope: Invocation.McpInvocationScope) =>
  McpServer.toolkit(AgentDocumentsToolkit).pipe(
    Layer.provide(AgentDocumentsToolkitHandlersLive),
    Layer.provideMerge(services),
    Layer.provide(Layer.succeed(Invocation.McpInvocationContext, scope)),
    Layer.provideMerge(McpServer.McpServer.layer),
  );
const registered = registeredFor(invocation);
const client = Layer.mock(McpSchema.McpServerClient)({
  clientId: 1,
  protocolVersion: "2025-11-25",
  clientCapabilities: {},
  clientInfo: { name: "test", version: "1" },
  initializePayload: {
    protocolVersion: "2025-11-25",
    capabilities: {},
    clientInfo: { name: "test", version: "1" },
  },
});

it.effect(
  "registers the full document toolkit and binds public read/write calls to invocation scope",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* migrate;
        const server = yield* McpServer.McpServer;
        expect(server.tools.map(({ tool }) => tool.name)).toEqual([
          "agent_documents_prepare_pdf",
          "agent_documents_read",
          "agent_documents_write",
          "agent_documents_wait",
        ]);
        const mount = {
          operation: "mount",
          operationId: "mount",
          documentId: "doc",
          expectedRevision: 0,
          title: "Report",
          body: { kind: "markdown", content: "scoped" },
          placement: "panel",
          lifetime: "persistent",
          ownerThreadId: "forged-owner",
          projectId: "forged-project",
        };
        const written = yield* server.callTool({
          name: "agent_documents_write",
          arguments: { input: mount },
        });
        expect(written.isError).not.toBe(true);
        const documents = yield* Documents.AgentDocuments;
        const actual = yield* documents.read({
          operation: "get",
          documentId: "doc",
          ownerThreadId: owner,
          projectId: "project",
        });
        expect(actual[0]).toMatchObject({
          revision: 1,
          ownerThreadId: owner,
          projectId: "project",
        });
        const read = yield* server.callTool({
          name: "agent_documents_read",
          arguments: {
            input: { operation: "get", documentId: "doc", ownerThreadId: "forged-owner" },
          },
        });
        expect(read.isError).not.toBe(true);
        expect(JSON.stringify(read)).toContain("scoped");
        const invalid = yield* server
          .callTool({
            name: "agent_documents_write",
            arguments: { input: { operation: "mount", documentId: "bad" } },
          })
          .pipe(Effect.result);
        expect(invalid._tag).toBe("Failure");
      }).pipe(Effect.provide(registered), Effect.provide(client)),
    ),
);

it.effect("registered document tools deny an invocation without document capability", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const server = yield* McpServer.McpServer;
      for (const [name, input] of [
        ["agent_documents_read", { operation: "list" }],
        [
          "agent_documents_write",
          { operation: "close", documentId: "doc", expectedRevision: 1, operationId: "close" },
        ],
      ] as const) {
        const result = yield* server.callTool({ name, arguments: { input } });
        expect(result.isError).toBe(true);
        expect(JSON.stringify(result)).toContain("does not grant the agent-documents capability");
      }
    }).pipe(
      Effect.provide(registeredFor({ ...invocation, capabilities: new Set() })),
      Effect.provide(client),
    ),
  ),
);

const uncalled = () => Effect.die("Registration fixture must not execute this tool");
const registrationHandlers = Layer.mergeAll(
  CoordinationToolkit.toLayer({
    coordination_read: uncalled,
    coordination_write: uncalled,
    coordination_mailbox_read: uncalled,
    coordination_mailbox_write: uncalled,
  }),
  WorkersToolkit.toLayer({
    workers_spawn: uncalled,
    workers_list: uncalled,
    workers_get: uncalled,
    workers_send: uncalled,
    workers_stop: uncalled,
    workers_wait: uncalled,
  }),
  AutomationToolkit.toLayer({
    schedule_create: uncalled,
    schedule_list: uncalled,
    schedule_get: uncalled,
    schedule_cancel: uncalled,
    unattended_grants_list: uncalled,
    background_job_start: uncalled,
    background_job_list: uncalled,
    background_job_get: uncalled,
    background_job_output: uncalled,
    background_job_cancel: uncalled,
    background_job_wait: uncalled,
    background_job_subscribe: uncalled,
    background_job_cleanup: uncalled,
  }),
  MemoryToolkit.toLayer({
    memory_remember: uncalled,
    memory_recall: uncalled,
    memory_search: uncalled,
    memory_forget: uncalled,
    memory_tag: uncalled,
    memory_link: uncalled,
    memory_related: uncalled,
  }),
  QualityRecordsToolkit.toLayer({
    quality_records_update: uncalled,
    quality_records_get: uncalled,
  }),
  KnowledgeToolkit.toLayer({
    knowledge_code_search: uncalled,
    knowledge_history_search: uncalled,
    knowledge_history_read: uncalled,
    knowledge_skills_list: uncalled,
    knowledge_skills_read: uncalled,
    knowledge_skills_load: uncalled,
    knowledge_skills_reload: uncalled,
  }),
  ExternalMcpToolkit.toLayer({
    external_mcp_list: uncalled,
    external_mcp_connect: uncalled,
    external_mcp_disconnect: uncalled,
    external_mcp_reload: uncalled,
    external_mcp_search_tools: uncalled,
    external_mcp_call_tool: uncalled,
    external_mcp_cancel_call: uncalled,
  }),
  RuntimeToolkit.toLayer({
    runtime_metadata: uncalled,
    runtime_fork: uncalled,
    runtime_handoff: uncalled,
  }),
);
it.effect("all added toolkits register through the MCP server", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* McpServer.registerToolkit(CoordinationToolkit);
      yield* McpServer.registerToolkit(WorkersToolkit);
      yield* McpServer.registerToolkit(AutomationToolkit);
      yield* McpServer.registerToolkit(MemoryToolkit);
      yield* McpServer.registerToolkit(QualityRecordsToolkit);
      yield* McpServer.registerToolkit(KnowledgeToolkit);
      yield* McpServer.registerToolkit(ExternalMcpToolkit);
      yield* McpServer.registerToolkit(RuntimeToolkit);
      const server = yield* McpServer.McpServer;
      expect(server.tools.length).toBe(53);
      for (const { tool } of server.tools) expect(tool.inputSchema.type, tool.name).toBe("object");
    }).pipe(Effect.provide(registrationHandlers), Effect.provide(registered)),
  ),
);

it.effect("all added toolkits expose MCP-compatible object parameter schemas", () =>
  Effect.gen(function* () {
    for (const toolkit of [
      AgentDocumentsToolkit,
      CoordinationToolkit,
      WorkersToolkit,
      AutomationToolkit,
      MemoryToolkit,
      QualityRecordsToolkit,
      KnowledgeToolkit,
      ExternalMcpToolkit,
      RuntimeToolkit,
    ]) {
      for (const tool of Object.values(toolkit.tools)) {
        const encoded = Tool.getJsonSchema(tool);
        expect(encoded.type, tool.name).toBe("object");
        const json = yield* decodeToolJsonSchema(encoded);
        expect(json.type, tool.name).toBe("object");
      }
    }
  }),
);
