import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as M from "../../../../../../packages/contracts/src/memory.ts";
import * as Memory from "../../../memory/Memory.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { MemoryToolkit } from "./tools.ts";
const make = Effect.gen(function* () {
  const memory = yield* Memory.MemoryService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const authority = Effect.fn("MemoryToolkit.authority")(function* () {
    const scope = yield* McpInvocationContext.requireMcpCapability("memory");
    const thread = yield* snapshots.getThreadShellById(scope.threadId).pipe(
      Effect.mapError(
        () =>
          new M.MemoryError({
            code: "forbidden",
            detail: "Authenticated project could not be resolved.",
          }),
      ),
    );
    if (Option.isNone(thread))
      return yield* new M.MemoryError({
        code: "forbidden",
        detail: "Authenticated thread no longer exists.",
      });
    return { projectId: thread.value.projectId, threadId: scope.threadId, allowGlobal: false };
  });
  return MemoryToolkit.of({
    memory_remember: Effect.fn("MemoryToolkit.remember")(function* (input) {
      return yield* memory.remember(input, yield* authority());
    }),
    memory_recall: Effect.fn("MemoryToolkit.recall")(function* (input) {
      return yield* memory.recall(input, yield* authority());
    }),
    memory_search: Effect.fn("MemoryToolkit.search")(function* (input) {
      return yield* memory.search(input, yield* authority());
    }),
    memory_forget: Effect.fn("MemoryToolkit.forget")(function* (input) {
      return yield* memory.forget(input, yield* authority());
    }),
    memory_tag: Effect.fn("MemoryToolkit.tag")(function* (input) {
      return yield* memory.tag(input, yield* authority());
    }),
    memory_link: Effect.fn("MemoryToolkit.link")(function* (input) {
      return yield* memory.link(input, yield* authority());
    }),
    memory_related: Effect.fn("MemoryToolkit.related")(function* (input) {
      return yield* memory.related(input, yield* authority());
    }),
  });
});
export const MemoryToolkitHandlersLive = MemoryToolkit.toLayer(make);
