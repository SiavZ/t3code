import * as Schema from "effect/Schema";

import { PositiveInt, ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

const boundedText = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));

export const MEMORY_CONTENT_MAX_LENGTH = 4_000;
export const MEMORY_RESULT_LIMIT_MAX = 20;
/** Characters of entry content returned by one recall or search, across all entries. */
export const MEMORY_RESULT_CHARACTER_BUDGET = 16_000;

export const MemoryEntryId = TrimmedNonEmptyString.pipe(Schema.brand("MemoryEntryId"));
export type MemoryEntryId = typeof MemoryEntryId.Type;

export const MemoryCategory = Schema.Literals(["fact", "preference", "decision", "correction"]);
export type MemoryCategory = typeof MemoryCategory.Type;

export const MemoryEntry = Schema.Struct({
  id: MemoryEntryId,
  projectId: ProjectId,
  category: MemoryCategory,
  content: Schema.String,
  sourceThreadId: Schema.NullOr(ThreadId),
  createdAt: Schema.String,
});
export type MemoryEntry = typeof MemoryEntry.Type;

export const MemoryRememberInput = Schema.Struct({
  category: MemoryCategory.annotate({
    description:
      "fact: how something works. preference: how the user wants things done. decision: a choice made and why. correction: something previously believed that turned out wrong.",
  }),
  content: boundedText(MEMORY_CONTENT_MAX_LENGTH).annotate({
    description: `One self-contained note, at most ${MEMORY_CONTENT_MAX_LENGTH} characters. Write it so a future agent with no context understands it.`,
  }),
});
export type MemoryRememberInput = typeof MemoryRememberInput.Type;

export const MemoryQueryInput = Schema.Struct({
  query: boundedText(1_000).annotate({
    description: "Words to match against stored notes. Matching is lexical, not semantic.",
  }),
  category: Schema.optional(MemoryCategory),
  limit: Schema.optional(
    PositiveInt.check(Schema.isLessThanOrEqualTo(MEMORY_RESULT_LIMIT_MAX)).annotate({
      description: `Maximum entries to return, 1 to ${MEMORY_RESULT_LIMIT_MAX}. Defaults to 8.`,
    }),
  ),
});
export type MemoryQueryInput = typeof MemoryQueryInput.Type;

export const MemoryForgetInput = Schema.Struct({
  id: MemoryEntryId.annotate({ description: "The id of an entry from recall or search." }),
});
export type MemoryForgetInput = typeof MemoryForgetInput.Type;

export const MemoryQueryResult = Schema.Struct({
  entries: Schema.Array(MemoryEntry),
  truncated: Schema.Boolean.annotate({
    description: "True when more entries matched than were returned.",
  }),
});
export type MemoryQueryResult = typeof MemoryQueryResult.Type;

export const MemoryListInput = Schema.Struct({ projectId: ProjectId });
export type MemoryListInput = typeof MemoryListInput.Type;

export const MemoryListResult = Schema.Struct({
  entries: Schema.Array(MemoryEntry),
  truncated: Schema.Boolean,
});
export type MemoryListResult = typeof MemoryListResult.Type;

export const MemoryDeleteInput = Schema.Struct({ projectId: ProjectId, id: MemoryEntryId });
export type MemoryDeleteInput = typeof MemoryDeleteInput.Type;

export const MemoryDeleteResult = Schema.Struct({ deleted: Schema.Boolean });
export type MemoryDeleteResult = typeof MemoryDeleteResult.Type;

export class MemoryStorageError extends Schema.TaggedError<MemoryStorageError>()(
  "MemoryStorageError",
  { cause: Schema.Defect() },
) {
  override get message(): string {
    return "Project memory could not be read or written.";
  }
}
