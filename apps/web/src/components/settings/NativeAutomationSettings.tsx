import { useEffect, useState } from "react";
import { Effect } from "effect";
import { createEnvironmentCommand } from "@t3tools/client-runtime/state/runtime";
import {
  DesktopAutomationHostError,
  startDesktopAutomationHostLoop,
} from "./DesktopAutomationHostLoop";
import { connectionAtomRuntime } from "../../connection/runtime";
import { useAtomCommand } from "../../state/use-atom-command";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsRow } from "./settingsLayout";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { Switch } from "../ui/switch";
type HostState = "stopped" | "registering" | "connected";
const sessions = new Map<string, Effect.Effect<void>>();
const controlHost = createEnvironmentCommand(connectionAtomRuntime, {
  label: "nativeAutomation.consent",
  execute: (
    input: {
      readonly action: "start" | "stop";
      readonly apps: ReadonlyArray<string>;
      readonly scriptsEnabled: boolean;
      readonly onState: (state: HostState) => void;
    },
    _,
    environmentId,
  ) =>
    Effect.gen(function* () {
      const previous = sessions.get(environmentId);
      if (previous) {
        sessions.delete(environmentId);
        yield* previous;
      }
      if (input.action === "stop") {
        input.onState("stopped");
        return;
      }
      const bridge = window.desktopBridge;
      if (!bridge) return yield* new DesktopAutomationHostError({ reason: "unsupported-client" });
      const session = yield* startDesktopAutomationHostLoop(
        bridge,
        { environmentId, apps: input.apps, scriptsEnabled: input.scriptsEnabled },
        input.onState,
      );
      sessions.set(environmentId, session.stop);
    }),
});

/** Human-only local consent, independent of an agent's integrations capability. */
export function NativeAutomationSettings() {
  const { target, targets } = useSettingsScope();
  const control = useAtomCommand(controlHost);
  const [apps, setApps] = useState("");
  const [scriptsEnabled, setScriptsEnabled] = useState(false);
  const [state, setState] = useState<HostState>("stopped");
  const [pending, setPending] = useState(false);
  const bridge = window.desktopBridge;
  const environmentId = targets.length === 1 ? target?.environmentId : undefined;
  useEffect(() => {
    const revoke = () => {
      void bridge?.revokeNativeAutomation?.();
    };
    window.addEventListener("beforeunload", revoke);
    return () => {
      window.removeEventListener("beforeunload", revoke);
      revoke();
      if (environmentId)
        void control({
          environmentId,
          input: { action: "stop", apps: [], scriptsEnabled: false, onState: () => {} },
        });
    };
  }, [bridge, control, environmentId]);
  if (!bridge?.consentNativeAutomation || bridge.getClientPlatform?.() !== "darwin") return null;
  const active = state !== "stopped";
  async function change() {
    if (!environmentId || pending) return;
    setPending(true);
    try {
      await control({
        environmentId,
        input: {
          action: active ? "stop" : "start",
          apps: apps
            .split(",")
            .map((app) => app.trim())
            .filter(Boolean),
          scriptsEnabled,
          onState: setState,
        },
      });
    } finally {
      setPending(false);
    }
  }
  return (
    <SettingsRow
      id="native-mac-automation"
      title="This Mac as an automation host"
      description="Locally approve named apps for five minutes. Agents still need a separate app/thread lease approval. Closing this settings page revokes consent. macOS Accessibility permission must already be granted."
      status={
        state === "connected"
          ? "Registered with this environment"
          : state === "registering"
            ? "Locally approved, awaiting server registration"
            : "Not connected"
      }
      control={
        <Button
          size="sm"
          variant="outline"
          disabled={pending || !environmentId || (!active && !apps.trim())}
          onClick={() => void change()}
        >
          {active ? "Disconnect host" : "Review local consent"}
        </Button>
      }
    >
      <div className="flex flex-col gap-2">
        <Input
          aria-label="Allowed macOS applications"
          placeholder="Application names, comma separated"
          value={apps}
          disabled={active}
          onChange={(event) => setApps(event.target.value)}
        />
        <label className="flex items-center gap-2 text-sm">
          <Switch
            checked={scriptsEnabled}
            disabled={active}
            onCheckedChange={setScriptsEnabled}
            aria-label="Allow AppleScript"
          />
          Allow AppleScript, which can access other apps and files
        </label>
      </div>
    </SettingsRow>
  );
}
