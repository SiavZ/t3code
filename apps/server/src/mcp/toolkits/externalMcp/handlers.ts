import * as Effect from "effect/Effect";
import * as ExternalMcpConnections from "../../ExternalMcpConnections.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ExternalMcpToolkit } from "./tools.ts";
const make = Effect.gen(function* () {
  const service = yield* ExternalMcpConnections.ExternalMcpConnections;
  return ExternalMcpToolkit.of({
    external_mcp_list: Effect.fn(function* () {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.list();
    }),
    external_mcp_connect: Effect.fn(function* (input) {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.connect(input);
    }),
    external_mcp_disconnect: Effect.fn(function* (input) {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.disconnect(input);
    }),
    external_mcp_reload: Effect.fn(function* (input) {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.reload(input);
    }),
    external_mcp_search_tools: Effect.fn(function* (input) {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.searchTools(input);
    }),
    external_mcp_call_tool: Effect.fn(function* (input) {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.callTool(input);
    }),
    external_mcp_cancel_call: Effect.fn(function* (input) {
      yield* McpInvocationContext.requireMcpCapability("external-mcp");
      return yield* service.cancelCall(input);
    }),
  });
});
export const ExternalMcpToolkitHandlersLive = ExternalMcpToolkit.toLayer(make);
