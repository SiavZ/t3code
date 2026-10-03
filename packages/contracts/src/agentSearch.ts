import * as Schema from "effect/Schema";
import { ProjectId, PositiveInt, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProjectContentMatch } from "./project.ts";

const Limit = PositiveInt.check(Schema.isLessThanOrEqualTo(200));
export const AgentSearchInput = Schema.Struct({
  projectId: ProjectId,
  mode: Schema.Literals(["grep", "find", "outline", "trace"]),
  query: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  file: Schema.optional(TrimmedNonEmptyString),
  path: Schema.optional(Schema.String),
  glob: Schema.optional(Schema.String.check(Schema.isMaxLength(256))),
  useRegex: Schema.optional(Schema.Boolean),
  caseSensitive: Schema.optional(Schema.Boolean),
  relation: Schema.optional(Schema.Literals(["declared", "imports", "references", "calls"])),
  limit: Schema.optional(Limit),
});
export type AgentSearchInput = typeof AgentSearchInput.Type;
export const AgentSearchSymbol = Schema.Struct({
  path: Schema.String,
  name: Schema.String,
  kind: Schema.String,
  line: PositiveInt,
  column: PositiveInt,
  endLine: PositiveInt,
  relation: Schema.optional(Schema.String),
  target: Schema.optional(Schema.String),
});
export type AgentSearchSymbol = typeof AgentSearchSymbol.Type;
export const AgentSearchResult = Schema.Struct({
  mode: Schema.Literals(["grep", "find", "outline", "trace"]),
  paths: Schema.Array(Schema.String),
  matches: Schema.Array(ProjectContentMatch),
  symbols: Schema.Array(AgentSearchSymbol),
  truncated: Schema.Boolean,
  warnings: Schema.Array(Schema.String),
  parser: Schema.NullOr(Schema.String),
});
export type AgentSearchResult = typeof AgentSearchResult.Type;
export class AgentSearchError extends Schema.TaggedError<AgentSearchError>()("AgentSearchError", {
  reason: Schema.Literals([
    "project-not-found",
    "invalid-input",
    "outside-workspace",
    "unsupported-language",
    "read-failed",
    "search-failed",
    "limit-exceeded",
    "invalid-regex",
  ]),
}) {
  override get message() {
    return `Agent search failed: ${this.reason}.`;
  }
}
