import * as NodeCrypto from "node:crypto";
import { ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as R from "../../../../packages/contracts/src/runtimeOperations.ts";
import { ProviderService } from "./Services/ProviderService.ts";
import { clearStoppedRuntimeBinding } from "./Layers/ProviderSessionDirectory.ts";
import * as ProviderSessionRuntime from "../persistence/ProviderSessionRuntime.ts";

type Stage = R.ProviderDoctorResult["stages"][number];
export class ProviderDiagnosticRunner extends Context.Service<
  ProviderDiagnosticRunner,
  {
    readonly run: (
      input: R.ProviderDoctorInput,
    ) => Effect.Effect<ReadonlyArray<Stage>, R.RuntimeOperationError>;
  }
>()("t3/provider/ProviderDiagnosticRunner") {}

/** Owns only a fresh diagnostic native session. It never creates an orchestration thread or approves tools. */
const make = Effect.gen(function* () {
  const provider = yield* ProviderService;
  const fs = yield* FileSystem.FileSystem;
  const repository = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const run = (input: R.ProviderDoctorInput) =>
    Effect.scoped(
      Effect.gen(function* () {
        if (input.tier !== "full" || !input.model)
          return yield* new R.RuntimeOperationError({
            code: "invalid",
            detail: "Disposable inference requires a full diagnostic model selection.",
          });
        const threadId = ThreadId.make(`diagnostic:${NodeCrypto.randomUUID()}`);
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-provider-diagnostic-" });
        const modelSelection = {
          instanceId: ProviderInstanceId.make(input.instanceId),
          model: input.model,
        };
        // Acquire the subscription before dispatch, including adapters that complete synchronously.
        const pull = yield* Stream.toPull(
          provider.streamEvents.pipe(Stream.filter((event) => event.threadId === threadId)),
        );
        const stages: Stage[] = [];
        yield* Effect.acquireRelease(Effect.void, () =>
          Effect.gen(function* () {
            const cleanup = yield* Effect.exit(
              provider
                .stopSession({ threadId })
                .pipe(
                  Effect.andThen(
                    clearStoppedRuntimeBinding(threadId).pipe(
                      Effect.provideService(
                        ProviderSessionRuntime.ProviderSessionRuntimeRepository,
                        repository,
                      ),
                    ),
                  ),
                  Effect.timeout("30 seconds"),
                ),
            );
            stages.push({
              name: "cleanup",
              status: Exit.isSuccess(cleanup) ? "passed" : "failed",
              detail: Exit.isSuccess(cleanup)
                ? "Owned diagnostic session stopped and its runtime binding removed."
                : "Owned diagnostic session cleanup failed. No unrelated process or binding was touched.",
            });
          }),
        );
        const execution = yield* Effect.exit(
          Effect.gen(function* () {
            const terminalFiber = yield* Effect.forkScoped(
              Effect.gen(function* () {
                while (true) {
                  const events = yield* pull;
                  for (const event of events) {
                    if (
                      event.type === "request.opened" ||
                      event.type === "user-input.requested" ||
                      event.type === "turn.aborted" ||
                      event.type === "turn.completed"
                    )
                      return event;
                  }
                }
              }),
            );
            yield* provider.startDiagnosticSession(threadId, {
              threadId,
              cwd,
              providerInstanceId: modelSelection.instanceId,
              modelSelection,
              runtimeMode: "approval-required",
              approvalPolicy: "untrusted",
              sandboxMode: "read-only",
            });
            stages.push({
              name: "disposable-runtime",
              status: "passed",
              detail:
                "Fresh native session in an owned temporary workspace, without a resume cursor or durable conversation.",
            });
            const started = yield* provider.sendTurn({
              threadId,
              modelSelection,
              input:
                "Reply exactly OK. Do not use tools, run commands, access files, or ask questions.",
              interactionMode: "default",
            });
            const terminalEvent = yield* Fiber.join(terminalFiber);
            const terminal =
              terminalEvent.type === "turn.completed" &&
              (terminalEvent.turnId === undefined || terminalEvent.turnId === started.turnId) &&
              terminalEvent.payload.state === "completed";
            stages.push({
              name: "inference",
              status: terminal ? "passed" : "failed",
              detail: terminal
                ? "Native turn reached a terminal completion. Response contents are omitted."
                : "Native turn aborted or requested authority. No request was approved.",
            });
          }).pipe(Effect.timeout("60 seconds")),
        );
        if (Exit.isFailure(execution))
          stages.push({
            name: "inference",
            status: "failed",
            detail:
              "Disposable native inference failed or exceeded its bounded deadline. Native output is omitted.",
          });
        stages.push({
          name: "host-tool-roundtrip",
          status: "unavailable",
          detail:
            "This diagnostic proves native turn completion only. A diagnostic-only MCP echo capability has not been registered, so native tool invocation is not claimed.",
        });
        // The scoped finalizer awaits stop before deleting only this fresh binding, even on interruption.
        return stages;
      }),
    ).pipe(
      Effect.mapError(
        () =>
          new R.RuntimeOperationError({
            code: "provider",
            detail: "Disposable diagnostic workspace or subscription could not be prepared.",
          }),
      ),
    );
  return ProviderDiagnosticRunner.of({ run });
});
export const layer = Layer.effect(ProviderDiagnosticRunner, make);
