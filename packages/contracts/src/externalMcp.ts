import * as Schema from "effect/Schema";
import { PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const ExternalMcpConfig = Schema.Union([
  Schema.Struct({
    transport: Schema.Literal("stdio"),
    command: TrimmedNonEmptyString,
    args: Schema.Array(Schema.String),
    cwd: Schema.optional(Schema.String),
  }),
  Schema.Struct({ transport: Schema.Literal("http"), url: TrimmedNonEmptyString }),
]);
export type ExternalMcpConfig = typeof ExternalMcpConfig.Type;
export const ExternalMcpConfigureInput = Schema.Struct({
  id: TrimmedNonEmptyString,
  name: TrimmedNonEmptyString,
  config: ExternalMcpConfig,
  approved: Schema.Boolean,
});
export type ExternalMcpConfigureInput = typeof ExternalMcpConfigureInput.Type;
export const ExternalMcpIdInput = Schema.Struct({ id: TrimmedNonEmptyString });
export const ExternalMcpSnapshot = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  transport: Schema.Literals(["stdio", "http"]),
  state: Schema.Literals(["disconnected", "connecting", "connected", "failed"]),
  generation: Schema.Number,
});
export type ExternalMcpSnapshot = typeof ExternalMcpSnapshot.Type;
export const ExternalMcpListResult = Schema.Struct({
  connections: Schema.Array(ExternalMcpSnapshot),
});
export const ExternalMcpTool = Schema.Struct({
  connectionId: Schema.String,
  name: Schema.String,
  description: Schema.optional(Schema.String),
  inputSchema: Schema.Unknown,
  generation: Schema.Number,
});
export type ExternalMcpTool = typeof ExternalMcpTool.Type;
export const ExternalMcpSearchInput = Schema.Struct({
  query: Schema.String,
  connectionId: Schema.optional(Schema.String),
  limit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(100))),
});
export type ExternalMcpSearchInput = typeof ExternalMcpSearchInput.Type;
export const ExternalMcpSearchResult = Schema.Struct({
  tools: Schema.Array(ExternalMcpTool),
  truncated: Schema.Boolean,
});
export type ExternalMcpSearchResult = typeof ExternalMcpSearchResult.Type;
export const ExternalMcpCallInput = Schema.Struct({
  id: TrimmedNonEmptyString,
  name: TrimmedNonEmptyString,
  generation: Schema.Number,
  invocationId: TrimmedNonEmptyString,
  arguments: Schema.Record(Schema.String, Schema.Unknown),
  timeoutMs: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(120000))),
});
export type ExternalMcpCallInput = typeof ExternalMcpCallInput.Type;
export const ExternalMcpCallResult = Schema.Struct({ result: Schema.Unknown });
export type ExternalMcpCallResult = typeof ExternalMcpCallResult.Type;
export const ExternalMcpCancelInput = Schema.Struct({ invocationId: TrimmedNonEmptyString });
export class ExternalMcpError extends Schema.TaggedError<ExternalMcpError>()("ExternalMcpError", {
  reason: Schema.Literals([
    "not-found",
    "not-approved",
    "not-connected",
    "stale-generation",
    "transport-failed",
    "limit-exceeded",
    "invalid-config",
    "conflict",
    "canceled",
    "unsupported-auth",
  ]),
  id: Schema.optional(Schema.String),
}) {
  override get message() {
    return `External MCP operation failed: ${this.reason}.`;
  }
}
