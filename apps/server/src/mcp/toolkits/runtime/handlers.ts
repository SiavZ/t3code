import * as Effect from "effect/Effect";
import { ThreadRuntimeService } from "../../../orchestration/ThreadRuntimeService.ts";
import { requireMcpCapability } from "../../McpInvocationContext.ts";
import { RuntimeToolkit } from "./tools.ts";
const make = Effect.gen(function* () {
  const runtime = yield* ThreadRuntimeService;
  return RuntimeToolkit.of({
    runtime_metadata: Effect.fn(function* () {
      const scope = yield* requireMcpCapability("runtime-tools");
      return yield* runtime.metadata(scope.threadId);
    }),
    runtime_fork: Effect.fn(function* (input) {
      const scope = yield* requireMcpCapability("runtime-tools");
      return yield* runtime.fork({ ...input, sourceThreadId: scope.threadId });
    }),
    runtime_handoff: Effect.fn(function* (input) {
      const scope = yield* requireMcpCapability("runtime-tools");
      return yield* runtime.handoff({ ...input, threadId: scope.threadId });
    }),
  });
});
export const RuntimeToolkitHandlersLive = RuntimeToolkit.toLayer(make);
