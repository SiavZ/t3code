import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as McpServer from "effect/unstable/ai/McpServer";
import * as McpSchema from "effect/unstable/ai/McpSchema";
import * as Tool from "effect/unstable/ai/Tool";
import { CoordinationToolkit } from "./toolkits/coordination/tools.ts";
import { WorkersToolkit } from "./toolkits/workers/tools.ts";
import { AutomationToolkit } from "./toolkits/automation/tools.ts";
import { MemoryToolkit } from "./toolkits/memory/tools.ts";
import { QualityRecordsToolkit } from "./toolkits/qualityRecords/tools.ts";

const decodeToolJsonSchema = Schema.decodeUnknownEffect(McpSchema.ToolJsonSchema);

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
);
it.effect("all added toolkits register through the MCP server", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* McpServer.registerToolkit(CoordinationToolkit);
      yield* McpServer.registerToolkit(WorkersToolkit);
      yield* McpServer.registerToolkit(AutomationToolkit);
      yield* McpServer.registerToolkit(MemoryToolkit);
      yield* McpServer.registerToolkit(QualityRecordsToolkit);
      const server = yield* McpServer.McpServer;
      expect(server.tools.length).toBe(32);
      for (const { tool } of server.tools) expect(tool.inputSchema.type, tool.name).toBe("object");
    }).pipe(Effect.provide(registrationHandlers), Effect.provide(McpServer.McpServer.layer)),
  ),
);

it.effect("all added toolkits expose MCP-compatible object parameter schemas", () =>
  Effect.gen(function* () {
    for (const toolkit of [
      CoordinationToolkit,
      WorkersToolkit,
      AutomationToolkit,
      MemoryToolkit,
      QualityRecordsToolkit,
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
