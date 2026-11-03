import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as MemoryService from "../../../memory/MemoryService.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { MemoryToolkit } from "./tools.ts";

const authority = Effect.gen(function* () {
  const scope = yield* McpInvocationContext.requireMcpCapability("memory");
  const { thread } = yield* McpInvocationContext.requireThreadScope(scope, "Project memory");
  const threads = yield* ThreadManagement.ThreadManagementService;
  const shell = yield* threads.getThreadShell(thread.threadId).pipe(
    Effect.mapError(
      () =>
        new OrchestratorMcpFailure({
          code: "thread_not_found",
          message: "The calling thread was not found.",
        }),
    ),
  );
  if (shell === null || shell.deletedAt !== null) {
    return yield* new OrchestratorMcpFailure({
      code: "thread_not_found",
      message: "The calling thread was not found.",
    });
  }
  return { projectId: shell.projectId, threadId: shell.id } satisfies MemoryService.MemoryAuthority;
});

export const layer = McpToolAccess.toLayer(MemoryToolkit, {
  memory_remember: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const caller = yield* authority;
      const memory = yield* MemoryService.MemoryService;
      return yield* memory.remember(input, caller);
    }),
  ),
  memory_recall: McpToolAccess.readsAsCaller((input) =>
    Effect.gen(function* () {
      const caller = yield* authority;
      const memory = yield* MemoryService.MemoryService;
      return yield* memory.search(input, caller);
    }),
  ),
  memory_forget: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const caller = yield* authority;
      const memory = yield* MemoryService.MemoryService;
      return { forgotten: yield* memory.forget(input, caller) };
    }),
  ),
});
