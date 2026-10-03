import { describe, expect, it } from "vite-plus/test";
import {
  createDocumentActionInput,
  effectiveDocumentPlacement,
  mergeDocumentSnapshot,
  safeDocumentUrl,
} from "./model.ts";
import type { AgentDocument } from "@t3tools/contracts";
const original: AgentDocument = {
  id: "doc",
  ownerThreadId: "thread",
  projectId: "project",
  revision: 2,
  title: "Document",
  body: { kind: "markdown", content: "new" },
  placement: "panel",
  lifetime: "persistent",
  closed: false,
};
describe("agent document model", () => {
  it("does not regress when reconnect delivers an older revision", () => {
    expect(
      mergeDocumentSnapshot(
        [original],
        [{ ...original, revision: 1, body: { kind: "markdown", content: "old" } }],
      ),
    ).toEqual([original]);
  });
  it("applies a newer closed revision", () => {
    expect(
      mergeDocumentSnapshot([original], [{ ...original, revision: 3, closed: true }])[0]?.closed,
    ).toBe(true);
  });
  it("snapshots input state before later edits", () => {
    const state = { choice: "accepted" };
    const action = createDocumentActionInput(
      original,
      "client",
      "action",
      { action: "submit" },
      state,
    );
    state.choice = "later";
    expect(action.state.choice).toBe("accepted");
  });
  it.each(["javascript:alert(1)", "file:///etc/passwd", "https://user:secret@example.com/"])(
    "rejects unsafe client URL %s",
    (url) => expect(safeDocumentUrl(url)).toBeNull(),
  );
  it("normalizes an HTTPS destination", () =>
    expect(safeDocumentUrl("https://example.com")).toBe("https://example.com/"));
  it("reports mobile sheet fallback explicitly", () =>
    expect(effectiveDocumentPlacement("sidebar", "mobile").actualPlacement).toBe("sheet"));
  it("reports unavailable web inline anchor", () =>
    expect(effectiveDocumentPlacement("inline", "web").reason).not.toBeNull());
});
