// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, expect, it, vi } from "vite-plus/test";
import { AgentDocumentView } from "./AgentDocumentView";
import type { AgentDocument } from "@t3tools/contracts";
let root: Root;
let container: HTMLDivElement;
beforeEach(() => {
  vi.stubGlobal("IS_REACT_ACT_ENVIRONMENT", true);
  container = document.createElement("div");
  document.body.append(container);
  root = createRoot(container);
});
afterEach(async () => {
  await act(async () => root.unmount());
  container.remove();
  vi.unstubAllGlobals();
});
const snapshot: AgentDocument = {
  id: "doc",
  projectId: "project",
  ownerThreadId: "thread",
  title: "Chooser",
  revision: 1,
  placement: "end",
  lifetime: "persistent",
  closed: false,
  body: {
    kind: "applet",
    state: { enabled: false, mode: "a" },
    view: {
      type: "stack",
      children: [
        { type: "toggle", label: "Enabled", bind: "enabled" },
        {
          type: "select",
          label: "Mode",
          bind: "mode",
          options: [
            { value: "a", label: "First" },
            { value: "b", label: "Second" },
          ],
        },
        { type: "button", label: "Confirm", on_press: { action: "confirm" } },
      ],
    },
  },
};
it("accepts control edits as one action snapshot and blocks disconnected interactions", async () => {
  const accepted = vi.fn(async () => undefined);
  const close = vi.fn(async () => undefined);
  await act(async () =>
    root.render(
      <AgentDocumentView document={snapshot} connected onAction={accepted} onClose={close} />,
    ),
  );
  await act(async () => container.querySelector<HTMLElement>('[role="switch"]')!.click());
  const button = (text: string) =>
    [...container.querySelectorAll<HTMLButtonElement>("button")].find(
      (button) => button.textContent === text,
    )!;
  await act(async () => button("Second").click());
  await act(async () => button("Confirm").click());
  expect(accepted).toHaveBeenCalledWith({ action: "confirm" }, { enabled: true, mode: "b" });
  await act(async () =>
    root.render(
      <AgentDocumentView
        document={snapshot}
        connected={false}
        onAction={accepted}
        onClose={close}
      />,
    ),
  );
  await act(async () => button("Confirm").click());
  expect(accepted).toHaveBeenCalledTimes(1);
});
it("reports a failed close and allows a later retry without unhandled rejection", async () => {
  const close = vi
    .fn()
    .mockRejectedValueOnce(new Error("Revision changed"))
    .mockResolvedValueOnce(undefined);
  await act(async () =>
    root.render(
      <AgentDocumentView
        document={snapshot}
        connected
        onAction={async () => undefined}
        onClose={close}
      />,
    ),
  );
  const button = () =>
    [...container.querySelectorAll<HTMLButtonElement>("button")].find(
      (button) => button.textContent === "Close",
    )!;
  await act(async () => button().click());
  expect(container.querySelector('[role="alert"]')?.textContent).toBe("Revision changed");
  expect(button().disabled).toBe(false);
  await act(async () => button().click());
  expect(close).toHaveBeenCalledTimes(2);
  expect(container.querySelector('[role="alert"]')).toBeNull();
});
