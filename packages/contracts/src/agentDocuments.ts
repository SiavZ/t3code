import * as Schema from "effect/Schema";

const id = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160));
const revision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
const text = Schema.String.check(Schema.isMaxLength(65_536));
export const AgentDocumentPlacement = Schema.Literals([
  "inline",
  "end",
  "panel",
  "sidebar",
  "overlay",
  "composer",
]);
export const AgentDocumentHostAction = Schema.Union([
  Schema.Struct({ action: Schema.Literal("host.copy"), text }),
  Schema.Struct({
    action: Schema.Literal("host.open_url"),
    url: Schema.String.check(Schema.isMaxLength(4096)),
  }),
  Schema.Struct({ action: Schema.Literals(["host.send_prompt", "host.start_chat"]), prompt: text }),
  Schema.Struct({ action: Schema.Literal("host.close") }),
  Schema.Struct({ action: Schema.Literal("host.set_state"), key: id, value: Schema.Json }),
]);
export type AgentDocumentHostAction = typeof AgentDocumentHostAction.Type;
export const AgentDocumentAction = Schema.Union([
  AgentDocumentHostAction,
  Schema.Struct({
    action: id.check(Schema.isPattern(/^(?!host\.)/)),
    args: Schema.optional(Schema.Json),
  }),
]);
export type AgentDocumentAction = typeof AgentDocumentAction.Type;
export interface AgentDocumentNode {
  readonly type:
    | "stack"
    | "grid"
    | "scroll"
    | "card"
    | "tabs"
    | "spacer"
    | "divider"
    | "text"
    | "markdown"
    | "code"
    | "image"
    | "icon"
    | "key_value"
    | "table"
    | "progress"
    | "empty"
    | "error"
    | "button"
    | "chip"
    | "toggle"
    | "input"
    | "select"
    | "list"
    | "list_item";
  readonly text?: string | undefined;
  readonly label?: string | undefined;
  readonly title?: string | undefined;
  readonly bind?: string | undefined;
  readonly children?: readonly AgentDocumentNode[] | undefined;
  readonly on_press?: AgentDocumentAction | undefined;
  readonly on_submit?: AgentDocumentAction | undefined;
  readonly source?:
    | { readonly url?: string | undefined; readonly assetId?: string | undefined }
    | undefined;
  readonly alt?: string | undefined;
  readonly value?: number | undefined;
  readonly direction?: "vertical" | "horizontal" | undefined;
  readonly placeholder?: string | undefined;
  readonly multiline?: boolean | undefined;
  readonly rows?: readonly (readonly string[])[] | undefined;
  readonly columns?: readonly string[] | undefined;
  readonly tabs?:
    | readonly {
        readonly id: string;
        readonly label: string;
        readonly children: readonly AgentDocumentNode[];
      }[]
    | undefined;
  readonly options?: readonly { readonly value: string; readonly label: string }[] | undefined;
}
export const AgentDocumentNode: Schema.Codec<AgentDocumentNode> = Schema.Struct({
  type: Schema.Literals([
    "stack",
    "grid",
    "scroll",
    "card",
    "tabs",
    "spacer",
    "divider",
    "text",
    "markdown",
    "code",
    "image",
    "icon",
    "key_value",
    "table",
    "progress",
    "empty",
    "error",
    "button",
    "chip",
    "toggle",
    "input",
    "select",
    "list",
    "list_item",
  ]),
  text: Schema.optional(text),
  label: Schema.optional(text),
  title: Schema.optional(text),
  bind: Schema.optional(id),
  children: Schema.optional(
    Schema.Array(Schema.suspend(() => AgentDocumentNode)).check(Schema.isMaxLength(128)),
  ),
  on_press: Schema.optional(AgentDocumentAction),
  on_submit: Schema.optional(AgentDocumentAction),
  source: Schema.optional(
    Schema.Struct({ url: Schema.optional(text), assetId: Schema.optional(id) }),
  ),
  alt: Schema.optional(text),
  value: Schema.optional(Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 }))),
  direction: Schema.optional(Schema.Literals(["vertical", "horizontal"])),
  placeholder: Schema.optional(text),
  multiline: Schema.optional(Schema.Boolean),
  rows: Schema.optional(
    Schema.Array(Schema.Array(text).check(Schema.isMaxLength(32))).check(Schema.isMaxLength(128)),
  ),
  columns: Schema.optional(Schema.Array(text).check(Schema.isMaxLength(32))),
  tabs: Schema.optional(
    Schema.Array(
      Schema.Struct({
        id,
        label: text,
        children: Schema.Array(Schema.suspend(() => AgentDocumentNode)).check(
          Schema.isMaxLength(128),
        ),
      }),
    ).check(Schema.isMaxLength(32)),
  ),
  options: Schema.optional(
    Schema.Array(Schema.Struct({ value: id, label: text })).check(Schema.isMaxLength(128)),
  ),
});
export const AgentDocumentBody = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("applet"),
    view: AgentDocumentNode,
    state: Schema.Record(Schema.String, Schema.Json),
  }),
  Schema.Struct({ kind: Schema.Literal("markdown"), content: text }),
  Schema.Struct({ kind: Schema.Literal("pdf"), assetId: id }),
]);
export type AgentDocumentBody = typeof AgentDocumentBody.Type;
export const AgentDocument = Schema.Struct({
  id,
  ownerThreadId: id,
  projectId: id,
  revision: revision,
  title: text,
  body: AgentDocumentBody,
  placement: AgentDocumentPlacement,
  lifetime: Schema.Literals(["ephemeral", "session", "persistent"]),
  closed: Schema.Boolean,
});
export type AgentDocument = typeof AgentDocument.Type;
const scope = { ownerThreadId: id, projectId: id };
const target = { ...scope, documentId: id };
export const AgentDocumentReadInput = Schema.Union([
  Schema.Struct({ ...target, operation: Schema.Literal("get") }),
  Schema.Struct({
    ...scope,
    operation: Schema.Literal("list"),
    includeClosed: Schema.optional(Schema.Boolean),
  }),
]);
export type AgentDocumentReadInput = typeof AgentDocumentReadInput.Type;
const mutation = { ...target, operationId: id, expectedRevision: revision };
export const AgentDocumentWriteInput = Schema.Union([
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("mount"),
    title: text,
    body: AgentDocumentBody,
    placement: AgentDocumentPlacement,
    lifetime: AgentDocument.fields.lifetime,
  }),
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("acknowledge"),
    throughSequence: revision,
  }),
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("patch"),
    patches: Schema.Array(
      Schema.Struct({
        op: Schema.Literals(["add", "replace", "remove"]),
        path: id,
        value: Schema.optional(Schema.Json),
      }),
    ).check(Schema.isMaxLength(64)),
  }),
  Schema.Struct({ ...mutation, operation: Schema.Literal("replace"), body: AgentDocumentBody }),
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("move"),
    placement: AgentDocumentPlacement,
  }),
  Schema.Struct({ ...mutation, operation: Schema.Literals(["close", "reopen"]) }),
]);
export type AgentDocumentWriteInput = typeof AgentDocumentWriteInput.Type;
export const AgentDocumentActionInput = Schema.Struct({
  ...target,
  expectedRevision: revision,
  actionId: id,
  clientId: id,
  action: AgentDocumentAction,
  state: Schema.Record(Schema.String, Schema.Json),
});
export type AgentDocumentActionInput = typeof AgentDocumentActionInput.Type;
export const AgentDocumentAcceptedAction = Schema.Struct({
  sequence: Schema.Int,
  input: AgentDocumentActionInput,
});
export type AgentDocumentAcceptedAction = typeof AgentDocumentAcceptedAction.Type;
export const AgentDocumentWaitInput = Schema.Struct({ ...target, afterSequence: revision });
export type AgentDocumentWaitInput = typeof AgentDocumentWaitInput.Type;
export const AgentDocumentAssetPrepareInput = Schema.Struct({
  ownerThreadId: id,
  relativePath: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(4096)),
});
export type AgentDocumentAssetPrepareInput = typeof AgentDocumentAssetPrepareInput.Type;
export const AgentDocumentAsset = Schema.Struct({
  assetId: id,
  ownerThreadId: id,
  projectId: id,
  byteLength: Schema.Int,
});
export type AgentDocumentAsset = typeof AgentDocumentAsset.Type;
export class AgentDocumentError extends Schema.TaggedError<AgentDocumentError>()(
  "AgentDocumentError",
  {
    code: Schema.Literals(["invalid", "conflict", "notFound", "closed", "quota", "storage"]),
    detail: text,
  },
) {}
