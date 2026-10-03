import * as Effect from "effect/Effect";

import * as OwnedWorkers from "../../../orchestration/OwnedWorkers.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { WorkersToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const workers = yield* OwnedWorkers.OwnedWorkers;
  return WorkersToolkit.of({
    workers_spawn: Effect.fn("WorkersToolkit.spawn")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* workers.spawn(
        { ...input, callerThreadId: scope.threadId },
        {
          mcpCapabilityCeiling: [...scope.capabilities],
          ...(scope.unattendedAuthority ? { unattendedAuthority: scope.unattendedAuthority } : {}),
        },
      );
    }),
    workers_list: Effect.fn("WorkersToolkit.list")(function* () {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* workers.list({ callerThreadId: scope.threadId });
    }),
    workers_get: Effect.fn("WorkersToolkit.get")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* workers.get({ ...input, callerThreadId: scope.threadId });
    }),
    workers_send: Effect.fn("WorkersToolkit.send")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* workers.send(
        { ...input, callerThreadId: scope.threadId },
        {
          ...(scope.unattendedAuthority ? { unattendedAuthority: scope.unattendedAuthority } : {}),
        },
      );
    }),
    workers_stop: Effect.fn("WorkersToolkit.stop")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* workers.stop({ ...input, callerThreadId: scope.threadId });
    }),
    workers_wait: Effect.fn("WorkersToolkit.wait")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* workers.wait({ ...input, callerThreadId: scope.threadId });
    }),
  });
});

export const WorkersToolkitHandlersLive = WorkersToolkit.toLayer(make);
