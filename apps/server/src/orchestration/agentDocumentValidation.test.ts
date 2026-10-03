import { describe, expect, it } from "vite-plus/test";
import type { AgentDocument } from "@t3tools/contracts";
import { patchDocument, validateDocumentBudget } from "./agentDocumentValidation.ts";
const document: AgentDocument = {
  id: "doc",
  ownerThreadId: "thread",
  projectId: "project",
  revision: 1,
  title: "Controls",
  placement: "end",
  lifetime: "persistent",
  closed: false,
  body: {
    kind: "applet",
    state: {},
    view: { type: "stack", children: [{ type: "text", text: "first" }] },
  },
};
describe("document patch behavior", () => {
  it("inserts, replaces and removes array controls without mutating the original", () => {
    const updated = patchDocument(document, [
      { op: "add", path: "/view/children/-", value: { type: "text", text: "second" } },
      { op: "replace", path: "/view/children/0/text", value: "changed" },
      { op: "remove", path: "/view/children/1" },
    ]);
    expect(updated.body.kind === "applet" && updated.body.view.children?.[0]?.text).toBe("changed");
    expect(document.body.kind === "applet" && document.body.view.children?.[0]?.text).toBe("first");
  });
  it.each(["/view/children/01", "/view/children/5", "/state/__proto__/polluted", "/revision"])(
    "rejects illegal target %s",
    (path) => {
      expect(() => patchDocument(document, [{ op: "remove", path }])).toThrow();
      expect({}).not.toHaveProperty("polluted");
    },
  );
  it("rejects excessive nesting before recursive schema decoding", () => {
    let state: unknown = {};
    for (let index = 0; index < 30; index++) state = { state };
    try {
      validateDocumentBudget(state);
      throw new Error("Expected structural rejection");
    } catch (error) {
      expect(error).toMatchObject({
        code: "quota",
        detail: "Document structure exceeds depth or node budget.",
      });
    }
  });
});
