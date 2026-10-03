import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Option from "effect/Option";
import * as Scope from "effect/Scope";
import * as Hooks from "./RuntimeHooks.ts";
import * as Engine from "../orchestration/Services/OrchestrationEngine.ts";
import * as Snapshots from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import type { RuntimeHookEvent } from "../../../../packages/contracts/src/runtimeHooks.ts";
import { ThreadId } from "@t3tools/contracts";
export class RuntimeHookObservers extends Context.Service<
  RuntimeHookObservers,
  { readonly start: () => Effect.Effect<void> }
>()("t3/provider/RuntimeHookObservers") {}
const make = Effect.gen(function* () {
  const hooks = yield* Hooks.RuntimeHooks;
  const engine = yield* Engine.OrchestrationEngineService;
  const snapshots = yield* Snapshots.ProjectionSnapshotQuery;
  const events = yield* engine.subscribeDomainEvents;
  const sessions = new Set<string>();
  const turns = new Set<string>();
  const runningTurns = new Set<string>();
  const consumer = Stream.runForEach(events, (event) =>
    Effect.gen(function* () {
      if (event.aggregateKind !== "thread") return;
      const kinds: RuntimeHookEvent[] = [];
      const id = event.aggregateId;
      if (event.type === "thread.turn-start-requested") {
        turns.add(id);
        kinds.push("turn.start");
      }
      if (event.type === "thread.session-set") {
        const status = event.payload.session.status;
        if (status === "running" && turns.has(id)) runningTurns.add(id);
        if (status === "ready" && !sessions.has(id)) {
          sessions.add(id);
          kinds.push("session.start");
        }
        if (status === "stopped" && sessions.delete(id)) kinds.push("session.end");
        if (
          (status === "error" ||
            status === "stopped" ||
            (status === "ready" && runningTurns.has(id))) &&
          turns.delete(id)
        ) {
          runningTurns.delete(id);
          kinds.push("turn.end");
        }
      }
      if (event.type === "thread.turn-diff-completed" && turns.delete(id)) {
        runningTurns.delete(id);
        kinds.push("turn.end");
      }
      if (
        event.type === "thread.activity-appended" &&
        event.payload.activity.kind === "provider.turn.start.failed" &&
        turns.delete(id)
      ) {
        runningTurns.delete(id);
        kinds.push("turn.end");
      }
      if (!kinds.length) return;
      const thread = yield* snapshots.getThreadShellById(ThreadId.make(id));
      if (Option.isNone(thread)) return;
      yield* Effect.forEach(
        kinds,
        (kind) =>
          hooks.observe({
            projectId: thread.value.projectId,
            threadId: thread.value.id,
            event: kind,
            receiptId: `${event.eventId}:${kind}`,
          }),
        { discard: true },
      );
    }).pipe(
      Effect.catchCause(() =>
        Effect.logWarning(
          "Runtime lifecycle observer failed. The committed operation remains successful.",
        ),
      ),
    ),
  );
  let started = false;
  const scope = yield* Scope.Scope;
  return RuntimeHookObservers.of({
    start: () =>
      Effect.gen(function* () {
        if (started) return;
        started = true;
        yield* consumer.pipe(Effect.forkScoped, Effect.provideService(Scope.Scope, scope));
      }),
  });
});
export const layer = Layer.effect(RuntimeHookObservers, make);
