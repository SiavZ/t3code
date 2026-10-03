import * as Schema from "effect/Schema";
import { ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
const text = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));
export const MemoryId = text(120);
export const MemoryScope = Schema.Literals(["project", "global"]);
export const MemoryCategory = Schema.Literals(["fact", "preference", "entity", "correction"]);
export const MemoryTags = Schema.Array(text(64)).check(Schema.isMaxLength(32));
export const MemoryEntry = Schema.Struct({
  id: MemoryId,
  scope: MemoryScope,
  projectId: Schema.NullOr(ProjectId),
  category: MemoryCategory,
  content: text(8192),
  tags: MemoryTags,
  sourceThreadId: Schema.NullOr(ThreadId),
  sourceMessageId: Schema.NullOr(text(120)),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  revision: Schema.Int,
});
export type MemoryEntry = typeof MemoryEntry.Type;
export const MemoryRememberInput = Schema.Struct({
  id: MemoryId,
  operationId: text(120),
  scope: MemoryScope,
  category: MemoryCategory,
  content: text(8192),
  tags: MemoryTags,
  sourceMessageId: Schema.optional(text(120)),
});
export type MemoryRememberInput = typeof MemoryRememberInput.Type;
export const MemoryReadInput = Schema.Struct({
  query: Schema.String.check(Schema.isMaxLength(8192)),
  scope: Schema.optional(MemoryScope),
  category: Schema.optional(MemoryCategory),
  tags: Schema.optional(MemoryTags),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 }))),
});
export type MemoryReadInput = typeof MemoryReadInput.Type;
export const MemoryTurnRecallInput = Schema.Struct({
  ...MemoryReadInput.fields,
  budget: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 16000 }))),
});
export type MemoryTurnRecallInput = typeof MemoryTurnRecallInput.Type;
export const MemoryMutationInput = Schema.Struct({
  id: MemoryId,
  operationId: text(120),
  expectedRevision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
});
export type MemoryMutationInput = typeof MemoryMutationInput.Type;
export const MemoryTagInput = Schema.Struct({ ...MemoryMutationInput.fields, tags: MemoryTags });
export type MemoryTagInput = typeof MemoryTagInput.Type;
export const MemoryLinkInput = Schema.Struct({
  ...MemoryMutationInput.fields,
  toId: MemoryId,
  relation: Schema.Literals(["related", "supports", "contradicts", "supersedes"]),
});
export type MemoryLinkInput = typeof MemoryLinkInput.Type;
export const MemoryRelatedInput = Schema.Struct({
  id: MemoryId,
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 20 }))),
});
export type MemoryRelatedInput = typeof MemoryRelatedInput.Type;
const GlobalReadQuery = Schema.Struct({
  query: MemoryReadInput.fields.query,
  category: MemoryReadInput.fields.category,
  tags: MemoryReadInput.fields.tags,
  limit: MemoryReadInput.fields.limit,
});
const GlobalRememberInput = Schema.Struct({
  id: MemoryRememberInput.fields.id,
  operationId: MemoryRememberInput.fields.operationId,
  category: MemoryRememberInput.fields.category,
  content: MemoryRememberInput.fields.content,
  tags: MemoryRememberInput.fields.tags,
});
export const GlobalMemoryReadInput = Schema.Union([
  Schema.Struct({ operation: Schema.Literals(["recall", "search"]), input: GlobalReadQuery }),
  Schema.Struct({ operation: Schema.Literal("related"), input: MemoryRelatedInput }),
]);
export type GlobalMemoryReadInput = typeof GlobalMemoryReadInput.Type;
export const GlobalMemoryWriteInput = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("remember"), input: GlobalRememberInput }),
  Schema.Struct({ operation: Schema.Literal("forget"), input: MemoryMutationInput }),
  Schema.Struct({ operation: Schema.Literal("tag"), input: MemoryTagInput }),
  Schema.Struct({ operation: Schema.Literal("link"), input: MemoryLinkInput }),
]);
export type GlobalMemoryWriteInput = typeof GlobalMemoryWriteInput.Type;
export const MemoryResult = Schema.Struct({
  entries: Schema.Array(MemoryEntry).check(Schema.isMaxLength(20)),
  mode: Schema.Literal("lexical-context"),
  truncated: Schema.Boolean,
});
export type MemoryResult = typeof MemoryResult.Type;
export const MemoryMutationResult = Schema.Struct({
  id: MemoryId,
  revision: Schema.Int,
  status: Schema.Literals(["remembered", "tagged", "linked", "forgotten"]),
});
export type MemoryMutationResult = typeof MemoryMutationResult.Type;
export class MemoryError extends Schema.TaggedError<MemoryError>()("MemoryError", {
  code: Schema.Literals(["invalid", "forbidden", "notFound", "conflict", "storage"]),
  detail: text(2048),
}) {}
