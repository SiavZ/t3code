import { Effect, Fiber, Stream } from "effect";
import type { DesktopBridge } from "@t3tools/contracts";
import { parityOperations } from "@t3tools/client-runtime/operations/parity";
/** Called only by the renderer's human consent button, never from an agent RPC. */
export const startDesktopAutomationHostLoop = (
  bridge: DesktopBridge,
  input: {
    readonly environmentId: string;
    readonly apps: ReadonlyArray<string>;
    readonly scriptsEnabled: boolean;
  },
  onState: (state: "registering" | "connected" | "stopped") => void,
) =>
  Effect.gen(function* () {
    if (
      !bridge.consentNativeAutomation ||
      !bridge.executeNativeAutomation ||
      !bridge.revokeNativeAutomation
    )
      return yield* Effect.fail(
        new Error("Desktop automation requires a current Mac desktop client"),
      );
    const registration = yield* Effect.tryPromise({
      try: () => bridge.consentNativeAutomation!(input),
      catch: (error) => error,
    });
    onState("registering");
    const operations = parityOperations.desktopAutomation;
    const process = operations.connectOnce(registration).pipe(
      Stream.runForEach((request) =>
        Effect.gen(function* () {
          if (registration.expiresAt <= Date.now())
            return yield* Effect.fail(new Error("Local host consent expired"));
          const identity = {
            hostId: registration.hostId,
            generation: registration.generation,
            requestId: request.requestId,
          };
          if (!(yield* operations.authorize(identity))) return;
          onState("connected");
          const result = yield* Effect.tryPromise({
            try: () =>
              bridge.executeNativeAutomation!({ generation: registration.generation, request }),
            catch: () => new Error("Native host action failed"),
          }).pipe(Effect.result);
          yield* operations.respond({
            ...identity,
            result: result._tag === "Success" ? result.success : null,
            failed: result._tag !== "Success",
          });
        }),
      ),
      Effect.timeout(Math.max(1, registration.expiresAt - Date.now())),
      Effect.ensuring(
        Effect.gen(function* () {
          yield* Effect.tryPromise({
            try: () => bridge.revokeNativeAutomation!(),
            catch: () => undefined,
          }).pipe(Effect.ignore);
          yield* operations
            .disconnect({ hostId: registration.hostId, generation: registration.generation })
            .pipe(Effect.ignore);
          onState("stopped");
        }),
      ),
      Effect.ignore,
    );
    const fiber = yield* process.pipe(Effect.forkDetach);
    // Read actual server registration rather than claiming that local consent means connected.
    const hosts = yield* operations.hosts({}).pipe(Effect.onError(() => Fiber.interrupt(fiber)));
    if (
      hosts.some(
        (host) =>
          host.hostId === registration.hostId && host.generation === registration.generation,
      )
    )
      onState("connected");
    return { registration, stop: Fiber.interrupt(fiber).pipe(Effect.asVoid) };
  });
