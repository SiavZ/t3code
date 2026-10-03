import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import { RuntimeOperationError } from "../../../../../../packages/contracts/src/runtimeOperations.ts";
import { ThreadRuntimeService } from "../../../orchestration/ThreadRuntimeService.ts";
import { ProviderDoctor } from "../../../provider/ProviderDoctor.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { requireMcpCapability } from "../../McpInvocationContext.ts";
import { RuntimeToolkit } from "./tools.ts";
const make = Effect.gen(function* () {
  const runtime = yield* ThreadRuntimeService;
  const doctor = yield* ProviderDoctor;
  const snapshots = yield* ProjectionSnapshotQuery;
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
    runtime_doctor_offline: Effect.fn(function* (input) {
      const scope = yield* requireMcpCapability("runtime-tools");
      const thread = yield* snapshots.getThreadShellById(scope.threadId).pipe(
        Effect.mapError(
          () =>
            new RuntimeOperationError({
              code: "storage",
              detail: "Thread provider could not be read.",
            }),
        ),
      );
      if (Option.isNone(thread))
        return yield* new RuntimeOperationError({
          code: "notFound",
          detail: "Thread was not found.",
        });
      return yield* doctor.run(
        {
          instanceId: thread.value.modelSelection.instanceId,
          tier: "offline",
          runId: input.runId,
        },
        { trustedOperator: false },
      );
    }),
  });
});
export const RuntimeToolkitHandlersLive = RuntimeToolkit.toLayer(make);
