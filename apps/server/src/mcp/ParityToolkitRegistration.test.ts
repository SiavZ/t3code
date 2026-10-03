import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as McpServer from "effect/unstable/ai/McpServer";
import * as McpSchema from "effect/unstable/ai/McpSchema";
import * as Tool from "effect/unstable/ai/Tool";
import { WorkersToolkit } from "./toolkits/workers/tools.ts";

const decodeToolJsonSchema = Schema.decodeUnknownEffect(McpSchema.ToolJsonSchema);

const uncalled = () => Effect.die("Registration fixture must not execute this tool");
const registrationHandlers = WorkersToolkit.toLayer({
  workers_spawn: uncalled,
  workers_list: uncalled,
  workers_get: uncalled,
  workers_send: uncalled,
  workers_stop: uncalled,
  workers_wait: uncalled,
});
it.effect("all added toolkits register through the MCP server", () =>
  Effect.scoped(
    Effect.gen(function* () {
      yield* McpServer.registerToolkit(WorkersToolkit);
      const server = yield* McpServer.McpServer;
      expect(server.tools.length).toBe(6);
      for (const { tool } of server.tools) expect(tool.inputSchema.type, tool.name).toBe("object");
    }).pipe(Effect.provide(registrationHandlers), Effect.provide(McpServer.McpServer.layer)),
  ),
);

it.effect("all added toolkits expose MCP-compatible object parameter schemas", () =>
  Effect.gen(function* () {
    for (const toolkit of [WorkersToolkit]) {
      for (const tool of Object.values(toolkit.tools)) {
        const encoded = Tool.getJsonSchema(tool);
        expect(encoded.type, tool.name).toBe("object");
        const json = yield* decodeToolJsonSchema(encoded);
        expect(json.type, tool.name).toBe("object");
      }
    }
  }),
);
