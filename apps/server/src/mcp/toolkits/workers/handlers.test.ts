import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EnvironmentId,
  ProviderInstanceId,
  ThreadId,
  WorkerOperationError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";

import * as OwnedWorkers from "../../../orchestration/OwnedWorkers.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { WorkersToolkitHandlersLive } from "./handlers.ts";
import { WorkersToolkit } from "./tools.ts";

const owner = ThreadId.make("credential-owner");
const worker = ThreadId.make("owned-worker");
const invocation = (
  capabilities: ReadonlyArray<McpInvocationContext.McpCapability>,
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("environment"),
  threadId: owner,
  providerSessionId: "session",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(capabilities),
  issuedAt: 1,
});

it.effect("worker tools deny unprovisioned credentials before reaching the service", () =>
  Effect.gen(function* () {
    const service = Layer.mock(OwnedWorkers.OwnedWorkers)({
      list: () => Effect.die("an unprovisioned credential reached the workers service"),
    });
    const toolkit = yield* WorkersToolkit.pipe(
      Effect.provide(WorkersToolkitHandlersLive.pipe(Layer.provide(service))),
    );
    const error = yield* toolkit
      .handle("workers_list", {})
      .pipe(
        Stream.unwrap,
        Stream.runDrain,
        Effect.provide(service),
        Effect.provideService(
          McpInvocationContext.McpInvocationContext,
          invocation(["pull-requests"]),
        ),
        Effect.flip,
      );
    expect(error).toMatchObject({
      _tag: "McpCapabilityUnavailableError",
      capability: "workers",
      threadId: owner,
    });
  }),
);

it.effect("spoofed caller inputs cannot change the worker authority bound to a credential", () =>
  Effect.gen(function* () {
    const authorize = (caller: ThreadId) =>
      caller === owner
        ? Effect.succeed({ workerThreadId: worker, sequence: 1 })
        : Effect.fail(
            new WorkerOperationError({
              operation: "send",
              code: "forbidden",
              detail: "spoofed caller",
            }),
          );
    const service = Layer.mock(OwnedWorkers.OwnedWorkers)({
      send: (input) => authorize(input.callerThreadId),
      stop: (input) => authorize(input.callerThreadId),
      spawn: (input, authority) =>
        Effect.gen(function* () {
          expect(authority?.mcpCapabilityCeiling).toEqual(["workers", "pull-requests"]);
          return yield* authorize(input.callerThreadId);
        }),
    });
    const toolkit = yield* WorkersToolkit.pipe(
      Effect.provide(WorkersToolkitHandlersLive.pipe(Layer.provide(service))),
    );
    const forgedTarget = {
      callerThreadId: ThreadId.make("unrelated-owner"),
      workerThreadId: worker,
      commandId: CommandId.make("retry-safe-command"),
    };
    const effects = [
      toolkit.handle("workers_send", { ...forgedTarget, text: "Continue scoped work" }).pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.map((result) => result.at(-1)?.result),
      ),
      toolkit.handle("workers_stop", forgedTarget).pipe(
        Stream.unwrap,
        Stream.runCollect,
        Effect.map((result) => result.at(-1)?.result),
      ),
      toolkit
        .handle("workers_spawn", {
          ...forgedTarget,
          label: "Review",
          prompt: "Review scoped files",
          modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1" },
        })
        .pipe(
          Stream.unwrap,
          Stream.runCollect,
          Effect.map((result) => result.at(-1)?.result),
        ),
    ];
    for (const effect of effects) {
      const result = yield* effect.pipe(
        Effect.provide(service),
        Effect.provideService(
          McpInvocationContext.McpInvocationContext,
          invocation(["workers", "pull-requests"]),
        ),
      );
      expect(result).toEqual({ workerThreadId: worker, sequence: 1 });
    }
  }),
);
