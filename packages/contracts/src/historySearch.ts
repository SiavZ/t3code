import * as Schema from "effect/Schema";
import {
  IsoDateTime,
  PositiveInt,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";

export const HistorySource = Schema.Literals([
  "t3",
  "claudeAgent",
  "codex",
  "pi",
  "opencode",
  "cursor",
]);
export type HistorySource = typeof HistorySource.Type;
export const HistoryMessage = Schema.Struct({
  role: Schema.Literals(["user", "assistant", "system", "tool"]),
  text: Schema.String,
  createdAt: Schema.String,
});
export type HistoryMessage = typeof HistoryMessage.Type;
export const NormalizedHistorySession = Schema.Struct({
  source: HistorySource,
  sourceSessionId: Schema.String,
  sourceRef: TrimmedNonEmptyString,
  cwd: Schema.NullOr(Schema.String),
  model: Schema.NullOr(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  messages: Schema.Array(HistoryMessage),
});
export type NormalizedHistorySession = typeof NormalizedHistorySession.Type;
export const HistoryReadInput = Schema.Struct({
  projectId: ProjectId,
  sourceRef: TrimmedNonEmptyString,
});
export type HistoryReadInput = typeof HistoryReadInput.Type;
export const HistorySearchInput = Schema.Struct({
  projectId: ProjectId,
  query: Schema.String.check(Schema.isNonEmpty(), Schema.isMaxLength(256)),
  sources: Schema.optional(Schema.Array(HistorySource)),
  role: Schema.optional(Schema.Literals(["user", "assistant", "system", "tool"])),
  after: Schema.optional(IsoDateTime),
  before: Schema.optional(IsoDateTime),
  currentThreadId: Schema.optional(ThreadId),
  includeCurrent: Schema.optional(Schema.Boolean),
  includeTools: Schema.optional(Schema.Boolean),
  includeSystem: Schema.optional(Schema.Boolean),
  contextBefore: Schema.optional(Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 5 }))),
  contextAfter: Schema.optional(Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 5 }))),
  maxSessions: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(200))),
  limit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(100))),
  perSession: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(20))),
});
export type HistorySearchInput = typeof HistorySearchInput.Type;
export const HistorySearchHit = Schema.Struct({
  source: HistorySource,
  sourceRef: Schema.String,
  sourceSessionId: Schema.String,
  threadId: Schema.optional(ThreadId),
  messageIndex: Schema.optional(Schema.Number),
  messageId: Schema.optional(Schema.String),
  message: HistoryMessage,
  before: Schema.Array(HistoryMessage),
  after: Schema.Array(HistoryMessage),
});
export type HistorySearchHit = typeof HistorySearchHit.Type;
export const HistorySearchResult = Schema.Struct({
  hits: Schema.Array(HistorySearchHit),
  truncated: Schema.Boolean,
  warnings: Schema.Array(Schema.String),
});
export type HistorySearchResult = typeof HistorySearchResult.Type;
export class HistorySearchError extends Schema.TaggedError<HistorySearchError>()(
  "HistorySearchError",
  {
    reason: Schema.Literals([
      "project-not-found",
      "read-failed",
      "not-found",
      "outside-scope",
      "unsupported-source",
      "limit-exceeded",
      "invalid-input",
    ]),
  },
) {
  override get message() {
    return `History search failed: ${this.reason}.`;
  }
}
