import { Clock, Effect, Fiber, Schema, Stream } from "effect";
import type { DesktopBridge } from "@t3tools/contracts";
import { parityOperations } from "@t3tools/client-runtime/operations/parity";

/** Why the local desktop automation host stopped or could not start. */
export class DesktopAutomationHostError extends Schema.TaggedError<DesktopAutomationHostError>()(
  "DesktopAutomationHostError",
  { reason: Schema.Literals(["unsupported-client", "consent-failed", "consent-expired"]) },
) {
  override get message(): string {
    switch (this.reason) {
      case "unsupported-client":
        return "Desktop automation requires a current Mac desktop client.";
      case "consent-failed":
        return "Local automation consent was not granted.";
      case "consent-expired":
        return "Local host consent expired.";
    }
  }
}

class NativeHostActionFailed extends Schema.TaggedError<NativeHostActionFailed>()(
  "NativeHostActionFailed",
  {},
) {}

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
      return yield* new DesktopAutomationHostError({ reason: "unsupported-client" });
    const registration = yield* Effect.tryPromise({
      try: () => bridge.consentNativeAutomation!(input),
      catch: () => new DesktopAutomationHostError({ reason: "consent-failed" }),
    });
    onState("registering");
    const operations = parityOperations.desktopAutomation;
    const startedAt = yield* Clock.currentTimeMillis;
    const process = operations.connectOnce(registration).pipe(
      Stream.runForEach((request) =>
        Effect.gen(function* () {
          if (registration.expiresAt <= (yield* Clock.currentTimeMillis))
            return yield* new DesktopAutomationHostError({ reason: "consent-expired" });
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
            catch: () => new NativeHostActionFailed(),
          }).pipe(Effect.result);
          yield* operations.respond({
            ...identity,
            result: result._tag === "Success" ? result.success : null,
            failed: result._tag !== "Success",
          });
        }),
      ),
      Effect.timeout(Math.max(1, registration.expiresAt - startedAt)),
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
