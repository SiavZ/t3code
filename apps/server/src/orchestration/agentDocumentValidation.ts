import * as Schema from "effect/Schema";
import {
  AgentDocument,
  AgentDocumentError,
  type AgentDocumentWriteInput,
} from "../../../../packages/contracts/src/agentDocuments.ts";

export function patchDocument(
  document: AgentDocument,
  patches: Extract<AgentDocumentWriteInput, { operation: "patch" }>["patches"],
): AgentDocument {
  const body = document.body;
  const editable: Record<string, unknown> =
    body.kind === "applet"
      ? {
          title: document.title,
          view: structuredClone(body.view),
          state: structuredClone(body.state),
        }
      : { title: document.title };
  for (const patch of patches) {
    const keys = patch.path
      .split("/")
      .slice(1)
      .map((key) => key.replace(/~1/g, "/").replace(/~0/g, "~"));
    if (
      !patch.path.startsWith("/") ||
      !["title", "view", "state"].includes(keys[0] ?? "") ||
      keys.some((key) => ["__proto__", "constructor", "prototype"].includes(key))
    )
      throw new AgentDocumentError({ code: "invalid", detail: "Patch path is not editable." });
    let parent = editable;
    for (const key of keys.slice(0, -1)) {
      const value = parent[key];
      if (typeof value !== "object" || value === null)
        throw new AgentDocumentError({
          code: "invalid",
          detail: "Patch parent must be a container.",
        });
      parent = value as Record<string, unknown>;
    }
    const key = keys.at(-1)!;
    if (Array.isArray(parent)) {
      const index =
        key === "-" && patch.op === "add"
          ? parent.length
          : /^(0|[1-9]\d*)$/.test(key)
            ? Number(key)
            : -1;
      if (index < 0 || index > parent.length || (patch.op !== "add" && index === parent.length))
        throw new AgentDocumentError({ code: "invalid", detail: "Patch array index is invalid." });
      if (patch.op === "add") parent.splice(index, 0, patch.value);
      else if (patch.op === "remove") parent.splice(index, 1);
      else parent[index] = patch.value;
    } else {
      if (patch.op !== "add" && !Object.hasOwn(parent, key))
        throw new AgentDocumentError({ code: "invalid", detail: "Patch target does not exist." });
      if (patch.op === "remove") delete parent[key];
      else parent[key] = patch.value;
    }
  }
  return Schema.decodeUnknownSync(AgentDocument)({
    ...document,
    title: editable.title,
    body:
      body.kind === "applet"
        ? { kind: "applet", view: editable.view, state: editable.state }
        : body,
  });
}

export function validateDocumentBudget(value: unknown): void {
  let count = 0;
  let textLength = 0;
  const walk = (item: unknown, depth: number): void => {
    if (++count > 4096 || depth > 24)
      throw new AgentDocumentError({
        code: "quota",
        detail: "Document structure exceeds depth or node budget.",
      });
    if (typeof item === "string") {
      textLength += item.length;
      if (item.length > 65_536 || textLength > 262_144)
        throw new AgentDocumentError({ code: "quota", detail: "Document text exceeds budget." });
    }
    if (typeof item === "object" && item !== null)
      for (const child of Object.values(item)) walk(child, depth + 1);
  };
  walk(value, 0);
  if (new TextEncoder().encode(JSON.stringify(value)).byteLength > 262_144)
    throw new AgentDocumentError({ code: "quota", detail: "Document exceeds byte budget." });
}
