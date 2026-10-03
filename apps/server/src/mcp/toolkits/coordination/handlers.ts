import * as Effect from "effect/Effect";
import * as CoordinationPlans from "../../../orchestration/CoordinationPlans.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { CoordinationToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const plans = yield* CoordinationPlans.CoordinationPlans;
  return CoordinationToolkit.of({
    coordination_mailbox_read: Effect.fn("CoordinationToolkit.mailboxRead")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* plans.mailboxRead({ ...input, callerThreadId: scope.threadId });
    }),
    coordination_mailbox_write: Effect.fn("CoordinationToolkit.mailboxWrite")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* plans.mailboxWrite({ ...input.input, callerThreadId: scope.threadId });
    }),
    coordination_read: Effect.fn("CoordinationToolkit.read")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* plans.read({ ...input, callerThreadId: scope.threadId });
    }),
    coordination_write: Effect.fn("CoordinationToolkit.write")(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("workers");
      return yield* plans.write(
        { ...input.input, callerThreadId: scope.threadId },
        scope.unattendedAuthority,
      );
    }),
  });
});
export const CoordinationToolkitHandlersLive = CoordinationToolkit.toLayer(make);
