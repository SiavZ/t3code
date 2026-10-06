import {
  MEMORY_RESULT_CHARACTER_BUDGET,
  MemoryEntry,
  MemoryEntryId,
  type MemoryForgetInput,
  type MemoryQueryInput,
  type MemoryQueryResult,
  type MemoryRememberInput,
  MemoryStorageError,
  type ProjectId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlSchema from "effect/sql/SqlSchema";

/** Entries a project may hold. Remember refuses past it, so the store and every scan stay bounded. */
export const MAX_MEMORY_ENTRIES_PER_PROJECT = 500;
const DEFAULT_QUERY_LIMIT = 8;

/** Who is asking: the project comes from the caller's credential or route, never from tool input. */
export interface MemoryAuthority {
  readonly projectId: ProjectId;
  readonly threadId?: ThreadId | undefined;
}

export class MemoryProjectFullError extends Schema.TaggedError<MemoryProjectFullError>()(
  "MemoryProjectFullError",
  { limit: Schema.Int },
) {
  override get message(): string {
    return `This project already holds ${this.limit} memory entries. Forget outdated ones first.`;
  }
}

export class MemoryService extends Context.Service<
  MemoryService,
  {
    readonly remember: (
      input: MemoryRememberInput,
      authority: MemoryAuthority,
    ) => Effect.Effect<MemoryEntry, MemoryProjectFullError | MemoryStorageError>;
    /** Entries matching any query term, best match first, within the result character budget. */
    readonly search: (
      input: MemoryQueryInput,
      authority: MemoryAuthority,
    ) => Effect.Effect<MemoryQueryResult, MemoryStorageError>;
    /** Whether an entry was deleted. Another project's id deletes nothing, so ids leak nothing. */
    readonly forget: (
      input: MemoryForgetInput,
      authority: MemoryAuthority,
    ) => Effect.Effect<boolean, MemoryStorageError>;
    /** Newest first, for the settings list. */
    readonly list: (
      authority: MemoryAuthority,
    ) => Effect.Effect<MemoryQueryResult, MemoryStorageError>;
  }
>()("t3/memory/MemoryService") {}

/**
 * Lowercased words of 1 to 64 letters, digits, `_` or `-`, at most 32 per query. One-character
 * terms stay searchable so names like `R` or `C` can be recalled.
 */
export const queryTerms = (text: string): ReadonlyArray<string> =>
  [...new Set(text.toLocaleLowerCase().match(/[\p{L}\p{N}_-]{1,64}/gu) ?? [])].slice(0, 32);

/**
 * Longer terms match anywhere in the content. A one-character term matches only a whole word,
 * otherwise `r` would match nearly every note.
 */
const matchesTerm = (content: string, words: ReadonlySet<string>, term: string) =>
  term.length === 1 ? words.has(term) : content.includes(term);

const ProjectRequest = Schema.Struct({ projectId: Schema.String });

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const crypto = yield* Crypto.Crypto;
  const storageError = (cause: unknown) => new MemoryStorageError({ cause });

  // Fork build: the table is created here, not by a numbered migration. Upstream owns the migration
  // id sequence, and a fork-only id recorded in a user's database would make later upstream
  // migrations with the same id skip silently. Agent-written entries are immutable; there is no
  // foreign key to projects, so a removed project's notes are simply never listed again.
  yield* sql`
      CREATE TABLE IF NOT EXISTS agent_memory_entries (
        id TEXT PRIMARY KEY,
        project_id TEXT NOT NULL,
        category TEXT NOT NULL,
        content TEXT NOT NULL,
        source_thread_id TEXT,
        created_at TEXT NOT NULL
      )
    `.pipe(
    Effect.andThen(
      sql`
      CREATE INDEX IF NOT EXISTS idx_agent_memory_entries_project_created
      ON agent_memory_entries(project_id, created_at DESC, id)
    `,
    ),
    Effect.mapError(storageError),
  );

  // Every query reads the whole project, which the per-project cap keeps small.
  const projectRows = SqlSchema.findAll({
    Request: ProjectRequest,
    Result: MemoryEntry,
    execute: ({ projectId }) => sql`
      SELECT
        id AS "id",
        project_id AS "projectId",
        category AS "category",
        content AS "content",
        source_thread_id AS "sourceThreadId",
        created_at AS "createdAt"
      FROM agent_memory_entries
      WHERE project_id = ${projectId}
      ORDER BY created_at DESC, id
      LIMIT ${MAX_MEMORY_ENTRIES_PER_PROJECT + 1}
    `,
  });
  const readProject = (projectId: ProjectId) =>
    projectRows({ projectId }).pipe(Effect.mapError(storageError));

  /** Keep entries in order until the content budget or the limit runs out. */
  const withinBudget = (entries: ReadonlyArray<MemoryEntry>, limit: number) => {
    const kept: Array<MemoryEntry> = [];
    let characters = 0;
    for (const entry of entries) {
      if (kept.length === limit) break;
      if (characters + entry.content.length > MEMORY_RESULT_CHARACTER_BUDGET) break;
      kept.push(entry);
      characters += entry.content.length;
    }
    return { entries: kept, truncated: kept.length < entries.length };
  };

  const remember = Effect.fn("MemoryService.remember")(function* (
    input: MemoryRememberInput,
    authority: MemoryAuthority,
  ) {
    const entry: MemoryEntry = {
      id: MemoryEntryId.make(`mem_${yield* crypto.randomUUIDv4.pipe(Effect.orDie)}`),
      projectId: authority.projectId,
      category: input.category,
      content: input.content,
      sourceThreadId: authority.threadId ?? null,
      createdAt: DateTime.formatIso(yield* DateTime.now),
    };
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const [row] = yield* sql<{ readonly count: number }>`
            SELECT COUNT(*) AS "count" FROM agent_memory_entries
            WHERE project_id = ${authority.projectId}
          `;
          if ((row?.count ?? 0) >= MAX_MEMORY_ENTRIES_PER_PROJECT) {
            return yield* new MemoryProjectFullError({ limit: MAX_MEMORY_ENTRIES_PER_PROJECT });
          }
          yield* sql`
            INSERT INTO agent_memory_entries
              (id, project_id, category, content, source_thread_id, created_at)
            VALUES (${entry.id}, ${entry.projectId}, ${entry.category}, ${entry.content},
              ${entry.sourceThreadId}, ${entry.createdAt})
          `;
        }),
      )
      .pipe(Effect.catchTag("SqlError", (cause) => Effect.fail(storageError(cause))));
    return entry;
  });

  const search = Effect.fn("MemoryService.search")(function* (
    input: MemoryQueryInput,
    authority: MemoryAuthority,
  ) {
    const terms = queryTerms(input.query);
    if (terms.length === 0) return { entries: [], truncated: false };
    const ranked = (yield* readProject(authority.projectId))
      .filter((entry) => input.category === undefined || entry.category === input.category)
      .map((entry) => {
        const content = entry.content.toLocaleLowerCase();
        const words = new Set(content.match(/[\p{L}\p{N}_-]+/gu) ?? []);
        return { entry, score: terms.filter((term) => matchesTerm(content, words, term)).length };
      })
      .filter(({ score }) => score > 0)
      // Stable sort keeps newest-first among equal scores.
      .sort((a, b) => b.score - a.score)
      .map(({ entry }) => entry);
    return withinBudget(ranked, input.limit ?? DEFAULT_QUERY_LIMIT);
  });

  const forget = Effect.fn("MemoryService.forget")(function* (
    input: MemoryForgetInput,
    authority: MemoryAuthority,
  ) {
    const deleted = yield* sql<{ readonly id: string }>`
      DELETE FROM agent_memory_entries
      WHERE id = ${input.id} AND project_id = ${authority.projectId}
      RETURNING id AS "id"
    `.pipe(Effect.mapError(storageError));
    return deleted.length > 0;
  });

  const list = Effect.fn("MemoryService.list")(function* (authority: MemoryAuthority) {
    const rows = yield* readProject(authority.projectId);
    return {
      entries: rows.slice(0, MAX_MEMORY_ENTRIES_PER_PROJECT),
      truncated: rows.length > MAX_MEMORY_ENTRIES_PER_PROJECT,
    };
  });

  return MemoryService.of({ remember, search, forget, list });
});

export const layer = Layer.effect(MemoryService, make);
