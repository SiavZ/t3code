// @effect-diagnostics nodeBuiltinImport:off -- Runs osascript as a short-lived native helper with fixed arguments, no shell.
import * as NodeCrypto from "node:crypto";
const { createHash, randomUUID } = NodeCrypto;
import * as NodeChildProcess from "node:child_process";
const { execFile } = NodeChildProcess;
import * as NodeUtil from "node:util";
const { promisify } = NodeUtil;
import type { DesktopAction } from "../../../../packages/contracts/src/desktopAutomation.ts";

const runFile = promisify(execFile);
/** Construct only after local human consent. Loading this module never performs automation. */
export const createMacAutomationHost = (options: {
  readonly platform: string;
  readonly scriptsEnabled: boolean;
  readonly isConsentActive: () => boolean;
}) => {
  const elements = new Map<
    string,
    {
      app: string;
      pid: number | null;
      fingerprint: string;
      element: import("@crowecawcaw/xa11y").Element;
    }
  >();
  const digest = (value: string) => createHash("sha256").update(value).digest("hex");
  return {
    revoke: () => elements.clear(),
    execute: async (action: DesktopAction): Promise<unknown> => {
      if (options.platform !== "darwin" || !options.isConsentActive())
        throw new Error("macOS host consent is required");
      if (action.kind === "script") {
        if (!options.scriptsEnabled || action.source.length > 32_000)
          throw new Error("Scripting is not authorized");
        // No shell, no visible-input fallback. Local consent explicitly grants scripting.
        const result = await runFile("/usr/bin/osascript", ["-e", action.source], {
          timeout: 10_000,
          maxBuffer: 256_000,
        });
        return { stdout: result.stdout, stderr: result.stderr };
      }
      const { App } = await import("@crowecawcaw/xa11y");
      const app = await App.byName(action.app, { timeout: 0 });
      const fingerprint = digest(await app.dump(8));
      if (action.kind === "observe") {
        elements.clear();
        const nodes: {
          id: string;
          role: string;
          name: string | null;
          value: string | null;
          parentId?: string;
        }[] = [];
        const visit = async (
          element: import("@crowecawcaw/xa11y").Element,
          depth: number,
          parentId?: string,
        ): Promise<void> => {
          if (depth > 8 || nodes.length >= 200) return;
          const id = randomUUID();
          elements.set(id, { app: action.app, pid: app.pid, fingerprint, element });
          const sensitive = /secure|password|otp|one.?time|verification|credit.?card|payment/i.test(
            `${element.role} ${element.name ?? ""}`,
          );
          nodes.push({
            id,
            role: element.role,
            name: element.name,
            value: sensitive ? null : element.value,
            ...(parentId ? { parentId } : {}),
          });
          for (const child of await element.children()) await visit(child, depth + 1, id);
        };
        await visit(app.asElement(), 0);
        return { app: action.app, pid: app.pid, nodes, truncated: nodes.length >= 200 };
      }
      const handle = elements.get(action.element);
      if (
        !handle ||
        handle.app !== action.app ||
        handle.pid !== app.pid ||
        handle.fingerprint !== fingerprint
      ) {
        elements.clear();
        throw new Error("Element handle expired, observe the application again");
      }
      if (!options.isConsentActive()) throw new Error("Host consent expired");
      if (
        /secure|password|otp|one.?time|verification|credit.?card|payment|reset.?password/i.test(
          `${handle.element.role} ${handle.element.name ?? ""}`,
        )
      )
        throw new Error("Sensitive field automation requires a separate human workflow");
      if (action.kind === "press") await handle.element.press();
      else await handle.element.setValue(action.value);
      elements.clear();
      return { performed: action.kind };
    },
  };
};
