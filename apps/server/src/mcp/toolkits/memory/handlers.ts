import * as Effect from "effect/Effect";

import * as MemoryService from "../../../memory/MemoryService.ts";
import * as Orchestrator from "../../../orchestration-v2/Orchestrator.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { MemoryThreadNotFoundError, MemoryToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const memory = yield* MemoryService.MemoryService;
  const engine = yield* Orchestrator.OrchestratorV2;

  /** The project always comes from the credential's thread, never from tool input. */
  const authority = Effect.gen(function* () {
    const scope = yield* McpInvocationContext.requireMcpCapability("memory");
    const thread = yield* engine
      .getThreadShell(scope.threadId)
      .pipe(Effect.mapError(() => new MemoryThreadNotFoundError({ threadId: scope.threadId })));
    if (thread === null || thread === undefined) {
      return yield* new MemoryThreadNotFoundError({ threadId: scope.threadId });
    }
    return {
      projectId: thread.projectId,
      threadId: thread.id,
    } satisfies MemoryService.MemoryAuthority;
  });

  return MemoryToolkit.of({
    memory_remember: (input) =>
      authority.pipe(Effect.flatMap((caller) => memory.remember(input, caller))),
    memory_recall: (input) =>
      authority.pipe(Effect.flatMap((caller) => memory.search(input, caller))),
    memory_forget: (input) =>
      authority.pipe(
        Effect.flatMap((caller) => memory.forget(input, caller)),
        Effect.map((forgotten) => ({ forgotten })),
      ),
  });
});

export const MemoryToolkitHandlersLive = MemoryToolkit.toLayer(make);
