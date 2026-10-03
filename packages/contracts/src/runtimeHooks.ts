import * as Schema from "effect/Schema";
import { ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
const text = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));
export const RuntimeHookEvent = Schema.Literals([
  "session.start",
  "session.end",
  "turn.start",
  "turn.end",
  "host-tool.before",
  "host-tool.after",
  "native.observed",
]);
export type RuntimeHookEvent = typeof RuntimeHookEvent.Type;
export const RuntimeHook = Schema.Struct({
  id: text(120),
  projectId: ProjectId,
  enabled: Schema.Boolean,
  event: RuntimeHookEvent,
  command: text(4096),
  args: Schema.Array(Schema.String.check(Schema.isMaxLength(4096))).check(Schema.isMaxLength(32)),
  timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 30000 })),
  failurePolicy: Schema.Literals(["open", "closed"]),
  coverage: Schema.Literals(["host-tools", "native-approval-only"]),
});
export type RuntimeHook = typeof RuntimeHook.Type;
export const RuntimeHookContext = Schema.Struct({
  projectId: ProjectId,
  threadId: ThreadId,
  event: RuntimeHookEvent,
  receiptId: text(200),
  toolName: Schema.optional(text(200)),
  recursive: Schema.optional(Schema.Boolean),
});
export type RuntimeHookContext = typeof RuntimeHookContext.Type;
export const RuntimeHookResult = Schema.Struct({
  hookId: Schema.String,
  outcome: Schema.Literals(["allowed", "denied", "failed", "skipped"]),
  coverage: Schema.Literals(["host-tools", "native-approval-only"]),
  detail: Schema.String,
});
export type RuntimeHookResult = typeof RuntimeHookResult.Type;
export class RuntimeHooksError extends Schema.TaggedError<RuntimeHooksError>()(
  "RuntimeHooksError",
  { code: Schema.Literals(["invalid", "forbidden", "denied", "storage"]), detail: Schema.String },
) {}
