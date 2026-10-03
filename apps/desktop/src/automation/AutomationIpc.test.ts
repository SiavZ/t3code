import { expect, it } from "@effect/vitest";
import { HostProcessPlatform } from "@t3tools/shared/hostProcess";
import { vi } from "vite-plus/test";
import { Effect, Layer } from "effect";
import * as Ipc from "../ipc/DesktopIpc.ts";
import * as Dialog from "../electron/ElectronDialog.ts";
import {
  installAutomationIpc,
  AUTOMATION_CONSENT_CHANNEL,
  AUTOMATION_EXECUTE_CHANNEL,
  AUTOMATION_STATUS_CHANNEL,
  AUTOMATION_REVOKE_CHANNEL,
} from "./AutomationIpc.ts";
vi.mock("electron", () => ({}));
vi.mock("@crowecawcaw/xa11y", () => ({
  App: {
    byName: async () => {
      throw new Error("native automation must not be invoked in consent tests");
    },
  },
}));

it.effect(
  "installs inert native host handlers and binds confirmed consent to the owner renderer",
  () => {
    const handlers = new Map<string, Ipc.DesktopIpcHandleListener>();
    let nativeConfirmations = 0;
    const ipc = Ipc.make({
      removeHandler: (channel) => {
        handlers.delete(channel);
      },
      handle: (channel, handler) => {
        handlers.set(channel, handler);
      },
      removeAllListeners: () => {},
      on: () => {},
    });
    const dialog = Dialog.ElectronDialog.of({
      pickFolder: () => Effect.die("unused"),
      pickFiles: () => Effect.die("unused"),
      showErrorBox: () => Effect.void,
      showMessageBox: () =>
        Effect.sync(() => {
          nativeConfirmations++;
          return { response: 1, checkboxChecked: false };
        }),
    });
    return Effect.scoped(
      Effect.gen(function* () {
        yield* installAutomationIpc;
        expect(nativeConfirmations).toBe(0);
        const invoke = (channel: string, raw: unknown, sender = 7) =>
          Effect.tryPromise({
            try: () => Promise.resolve(handlers.get(channel)!({ sender: { id: sender } }, raw)),
            catch: (error) => error,
          });
        expect(yield* invoke(AUTOMATION_STATUS_CHANNEL, undefined)).toBeNull();
        const unknown = yield* invoke(AUTOMATION_EXECUTE_CHANNEL, {
          generation: "forged",
          request: {
            requestId: "req",
            leaseId: "lease",
            action: { kind: "observe", app: "Fixture" },
          },
        }).pipe(Effect.result);
        expect(unknown._tag).toBe("Failure");
        const registration = yield* invoke(AUTOMATION_CONSENT_CHANNEL, {
          environmentId: "fixture-env",
          apps: ["Fixture"],
          scriptsEnabled: false,
        });
        expect(nativeConfirmations).toBe(1);
        expect(registration).toMatchObject({
          hostId: "mac-7",
          environmentId: "fixture-env",
          operations: ["observe", "press", "set-value"],
        });
        expect(yield* invoke(AUTOMATION_STATUS_CHANNEL, undefined, 8)).toBeNull();
        yield* invoke(AUTOMATION_REVOKE_CHANNEL, undefined, 8);
        expect(yield* invoke(AUTOMATION_STATUS_CHANNEL, undefined)).not.toBeNull();
        yield* invoke(AUTOMATION_REVOKE_CHANNEL, undefined);
        expect(yield* invoke(AUTOMATION_STATUS_CHANNEL, undefined)).toBeNull();
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          Layer.succeed(Ipc.DesktopIpc, ipc),
          Layer.succeed(HostProcessPlatform, "darwin"),
          Layer.succeed(Dialog.ElectronDialog, dialog),
        ),
      ),
    );
  },
);
