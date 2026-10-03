import type { OrchestrationEvent } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { AgentDocuments } from "./AgentDocuments.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";

export function documentLifecycleReason(
  event: OrchestrationEvent,
): "deleted" | "turnEnded" | undefined {
  if (event.type === "thread.deleted") return "deleted";
  if (event.type === "thread.settled") return "turnEnded";
  if (
    event.type === "thread.session-set" &&
    ["ready", "interrupted", "stopped", "error"].includes(event.payload.session.status)
  )
    return "turnEnded";
  return undefined;
}

export class DocumentLifecycle extends Context.Service<
  DocumentLifecycle,
  {
    readonly process: (event: OrchestrationEvent) => Effect.Effect<void>;
  }
>()("t3/orchestration/DocumentLifecycle") {}

export const layer = Layer.effect(
  DocumentLifecycle,
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const documents = yield* AgentDocuments;
    const sql = yield* SqlClient.SqlClient;
    const process = Effect.fn("DocumentLifecycle.process")(function* (event: OrchestrationEvent) {
      const reason = documentLifecycleReason(event);
      if (!reason || event.aggregateKind !== "thread") return;
      // Deletion has already removed the projected shell. Stored document ownership
      // remains authoritative and avoids guessing a project from caller input.
      const owners = yield* sql<{
        project_id: string;
      }>`SELECT DISTINCT project_id FROM agent_documents WHERE owner_thread_id = ${event.aggregateId}`;
      for (const owner of owners)
        yield* documents.releaseOwner({
          ownerThreadId: event.aggregateId,
          projectId: owner.project_id,
          operationId: `lifecycle:${event.sequence}`,
          reason,
          ...(reason === "turnEnded" ? { throughSequence: event.sequence } : {}),
        });
    });
    const safeProcess = (event: OrchestrationEvent) =>
      process(event).pipe(
        Effect.catchCause((cause) =>
          Effect.logError("Agent document lifecycle cleanup failed", {
            sequence: event.sequence,
            cause,
          }),
        ),
      );
    // Acquisition completes before assembly returns, so terminal events cannot be
    // lost between layer construction and consumer scheduling.
    const events = yield* engine.subscribeDomainEvents;
    yield* Stream.runForEach(events, safeProcess).pipe(Effect.forkScoped);
    return DocumentLifecycle.of({ process: safeProcess });
  }),
);
