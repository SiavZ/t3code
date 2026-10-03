import { Effect, Schema } from "effect";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import * as NodeOS from "node:os";
const { hostname } = NodeOS;
import * as NodeCrypto from "node:crypto";
const { randomUUID } = NodeCrypto;
import * as Ipc from "../ipc/DesktopIpc.ts";
import * as Dialog from "../electron/ElectronDialog.ts";
import {
  DesktopAction,
  DesktopLocalExecuteInput,
  DesktopLocalConsentInput as Consent,
  DesktopLocalRegistration as Registration,
} from "../../../../packages/contracts/src/desktopAutomation.ts";
import { createMacAutomationHost } from "./MacAutomationHost.ts";
export const AUTOMATION_CONSENT_CHANNEL = "desktop:automation:consent";
export const AUTOMATION_EXECUTE_CHANNEL = "desktop:automation:execute";
export const AUTOMATION_REVOKE_CHANNEL = "desktop:automation:revoke";
export const AUTOMATION_STATUS_CHANNEL = "desktop:automation:status";
class ConsentError extends Schema.TaggedError<ConsentError>()("DesktopLocalConsentError", {
  reason: Schema.Literals(["denied", "expired", "unsupported", "scope-denied", "execution"]),
}) {}
/** Main-process installation is inert. Only a native human confirmation enables this host. */
export const installAutomationIpc = Effect.gen(function* () {
  const ipc = yield* Ipc.DesktopIpc;
  const dialog = yield* Dialog.ElectronDialog;
  const platform = yield* HostProcessPlatform;
  let consent:
    | {
        owner: number;
        registration: typeof Registration.Type;
        apps: ReadonlySet<string>;
        scriptsEnabled: boolean;
      }
    | undefined;
  const host = createMacAutomationHost({
    platform,
    scriptsEnabled: false,
    isConsentActive: () => false,
  });
  let activeHost = host;
  const revoke = () => {
    consent = undefined;
    activeHost.revoke();
  };
  // The native host checks consent synchronously between actions, so expiry is wall-clock
  // time on both sides rather than Effect's Clock.
  const current = (sender?: number) =>
    consent &&
    consent.owner === sender &&
    // @effect-diagnostics-next-line globalDate:off
    consent.registration.expiresAt > Date.now()
      ? consent
      : undefined;
  yield* Effect.addFinalizer(() => Effect.sync(revoke));
  yield* ipc.handle(
    Ipc.makeIpcMethod({
      channel: AUTOMATION_CONSENT_CHANNEL,
      payload: Consent,
      result: Registration,
      handler: (input, event) =>
        Effect.gen(function* () {
          if (platform !== "darwin") return yield* new ConsentError({ reason: "unsupported" });
          if (
            !event ||
            !input.environmentId ||
            input.apps.length < 1 ||
            input.apps.length > 10 ||
            input.apps.some((app) => !app.trim() || app.length > 200)
          )
            return yield* new ConsentError({ reason: "scope-denied" });
          const decision = yield* dialog.showMessageBox({
            type: "warning",
            title: "Allow native automation on this Mac?",
            message: `Allow this T3 Code window to automate ${input.apps.join(", ")} for five minutes?`,
            detail: input.scriptsEnabled
              ? "AppleScript can control other applications and access files. Only approve a trusted environment and task. No macOS permission prompt is opened automatically."
              : "Allows background Accessibility observation, press and value changes in the listed apps. Sensitive fields remain blocked. No macOS permission prompt is opened automatically.",
            buttons: ["Cancel", "Allow for five minutes"],
            defaultId: 0,
            cancelId: 0,
            noLink: true,
          });
          if (decision.response !== 1) return yield* new ConsentError({ reason: "denied" });
          revoke();
          const registration = {
            hostId: `mac-${event.sender.id}`,
            displayName: hostname(),
            environmentId: input.environmentId,
            generation: randomUUID(),
            operations: input.scriptsEnabled
              ? (["observe", "press", "set-value", "script"] as const)
              : (["observe", "press", "set-value"] as const),
            // @effect-diagnostics-next-line globalDateInEffect:off - must match the sync expiry check.
            expiresAt: Date.now() + 300_000,
          };
          consent = {
            owner: event.sender.id,
            registration,
            apps: new Set(input.apps),
            scriptsEnabled: input.scriptsEnabled,
          };
          activeHost = createMacAutomationHost({
            platform,
            scriptsEnabled: input.scriptsEnabled,
            isConsentActive: () => current(event.sender.id) !== undefined,
          });
          return registration;
        }),
    }),
  );
  yield* ipc.handle(
    Ipc.makeIpcMethod({
      channel: AUTOMATION_EXECUTE_CHANNEL,
      payload: DesktopLocalExecuteInput,
      result: Schema.Unknown,
      handler: (input, event) =>
        Effect.gen(function* () {
          const authorization = current(event?.sender.id);
          if (!authorization || authorization.registration.generation !== input.generation)
            return yield* new ConsentError({ reason: "expired" });
          if (
            !authorization.apps.has(input.request.action.app) ||
            !Schema.is(DesktopAction)(input.request.action)
          )
            return yield* new ConsentError({ reason: "scope-denied" });
          return yield* Effect.tryPromise({
            try: () => activeHost.execute(input.request.action),
            catch: () => new ConsentError({ reason: "execution" }),
          });
        }),
    }),
  );
  yield* ipc.handle(
    Ipc.makeIpcMethod({
      channel: AUTOMATION_REVOKE_CHANNEL,
      payload: Schema.Void,
      result: Schema.Void,
      handler: (_, event) =>
        Effect.sync(() => {
          if (consent?.owner === event?.sender.id) revoke();
        }),
    }),
  );
  yield* ipc.handle(
    Ipc.makeIpcMethod({
      channel: AUTOMATION_STATUS_CHANNEL,
      payload: Schema.Void,
      result: Schema.NullOr(Registration),
      handler: (_, event) => Effect.sync(() => current(event?.sender.id)?.registration ?? null),
    }),
  );
});
