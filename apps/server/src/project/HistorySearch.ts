import * as NodeCrypto from "node:crypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ThreadId } from "@t3tools/contracts";
import {
  HistorySearchError,
  type HistorySearchInput,
  type HistorySearchResult,
  type HistorySearchHit,
  type HistoryReadInput,
  type NormalizedHistorySession,
} from "../../../../packages/contracts/src/historySearch.ts";
import * as ExternalHistoryReaders from "./ExternalHistoryReaders.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";

export class HistorySearch extends Context.Service<
  HistorySearch,
  {
    readonly search: (
      input: HistorySearchInput,
    ) => Effect.Effect<HistorySearchResult, HistorySearchError>;
    readonly readHistory: (
      input: HistoryReadInput,
    ) => Effect.Effect<NormalizedHistorySession, HistorySearchError>;
  }
>()("t3/project/HistorySearch") {}

const sourceRef = (source: string, sessionId: string, path: string) =>
  `${source}:${NodeCrypto.createHash("sha256").update(`${sessionId}\0${path}`).digest("hex")}`;

/** Apply filters to both the hit and quoted context, preserving transcript order. */
export function searchHistorySession(
  session: NormalizedHistorySession,
  input: HistorySearchInput,
): HistorySearchHit[] {
  const visible = session.messages
    .map((message, index) => ({ message, index }))
    .filter(
      ({ message }) =>
        (message.role !== "tool" || input.includeTools) &&
        (message.role !== "system" || input.includeSystem) &&
        (!input.role || message.role === input.role) &&
        (!input.after || message.createdAt >= input.after) &&
        (!input.before || message.createdAt <= input.before),
    );
  const query = input.query.toLocaleLowerCase();
  const hits: HistorySearchHit[] = [];
  for (let index = 0; index < visible.length; index++) {
    const item = visible[index]!;
    if (!item.message.text.toLocaleLowerCase().includes(query)) continue;
    hits.push({
      source: session.source,
      sourceRef: session.sourceRef,
      sourceSessionId: session.sourceSessionId,
      messageIndex: item.index,
      message: { ...item.message, text: item.message.text.slice(0, 4096) },
      before: visible
        .slice(Math.max(0, index - (input.contextBefore ?? 0)), index)
        .map(({ message }) => ({ ...message, text: message.text.slice(0, 4096) })),
      after: visible
        .slice(index + 1, index + 1 + (input.contextAfter ?? 0))
        .map(({ message }) => ({ ...message, text: message.text.slice(0, 4096) })),
    });
    if (hits.length >= (input.perSession ?? 5)) break;
  }
  return hits;
}

const make = Effect.gen(function* () {
  const readers = yield* Effect.serviceOption(ExternalHistoryReaders.ExternalHistoryReaders);
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  const workspace = Effect.fn(function* (projectId: HistoryReadInput["projectId"]) {
    const project = yield* snapshots
      .getProjectShellById(projectId)
      .pipe(Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })));
    if (Option.isNone(project))
      return yield* new HistorySearchError({ reason: "project-not-found" });
    return project.value.workspaceRoot;
  });
  const external = (cwd: string, max: number) =>
    scanner.recentThreads(cwd).pipe(
      Stream.take(max),
      Stream.filter(
        (
          item,
        ): item is Extract<AgentSessionScanner.AgentSessionRecentThread, { _tag: "Importable" }> =>
          item._tag === "Importable",
      ),
      Stream.map((item): NormalizedHistorySession => {
        const thread = item.thread;
        return {
          source: thread.source,
          sourceSessionId: thread.providerSessionId,
          sourceRef: sourceRef(thread.source, thread.providerSessionId, item.source.filePath),
          cwd,
          model: thread.model,
          createdAt: thread.createdAt,
          updatedAt: thread.updatedAt,
          messages: thread.messages,
        };
      }),
      Stream.runCollect,
      Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })),
    );
  const readHistory = Effect.fn("HistorySearch.readHistory")(function* (input: HistoryReadInput) {
    const cwd = yield* workspace(input.projectId);
    if (input.sourceRef.startsWith("t3:")) {
      const id = input.sourceRef.slice(3);
      const budget = yield* sql<{
        count: number;
        bytes: number;
      }>`SELECT count(*) AS count, coalesce(sum(length(CAST(m.text AS BLOB))), 0) AS bytes FROM projection_thread_messages m JOIN projection_threads t ON t.thread_id = m.thread_id WHERE t.project_id = ${input.projectId} AND t.thread_id = ${id} AND t.deleted_at IS NULL`.pipe(
        Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })),
      );
      if ((budget[0]?.count ?? 0) > 2000 || (budget[0]?.bytes ?? 0) > 4194304)
        return yield* new HistorySearchError({ reason: "limit-exceeded" });
      const rows = yield* sql<{
        role: "user" | "assistant" | "system" | "tool";
        text: string;
        created_at: string;
      }>`SELECT m.role, m.text, m.created_at FROM projection_thread_messages m JOIN projection_threads t ON t.thread_id = m.thread_id WHERE t.project_id = ${input.projectId} AND t.thread_id = ${id} AND t.deleted_at IS NULL ORDER BY m.created_at, m.message_id LIMIT 2001`.pipe(
        Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })),
      );
      if (!rows.length) return yield* new HistorySearchError({ reason: "not-found" });
      if (
        rows.length > 2000 ||
        rows.reduce((sum, row) => sum + Buffer.byteLength(row.text), 0) > 4_194_304
      )
        return yield* new HistorySearchError({ reason: "limit-exceeded" });
      return {
        source: "t3" as const,
        sourceSessionId: id,
        sourceRef: input.sourceRef,
        cwd,
        model: null,
        createdAt: rows[0]!.created_at,
        updatedAt: rows.at(-1)!.created_at,
        messages: rows.map((row) => ({
          role: row.role,
          text: row.text,
          createdAt: row.created_at,
        })),
      };
    }
    const sessions = yield* external(cwd, 200);
    const newer = Option.isSome(readers) ? yield* readers.value.list(cwd, 200) : { sessions: [] };
    const session = [...sessions, ...newer.sessions].find(
      (item) => item.sourceRef === input.sourceRef,
    );
    if (!session) return yield* new HistorySearchError({ reason: "not-found" });
    return session;
  });
  const search = Effect.fn("HistorySearch.search")(function* (input: HistorySearchInput) {
    const cwd = yield* workspace(input.projectId);
    const sources = input.sources ?? ["t3", "claudeAgent", "codex"];
    const warnings = sources
      .filter((source) => source === "cursor")
      .map((source) => `${source}: unsupported store format until a verified reader is configured`);
    const hits: HistorySearchHit[] = [];
    const limit = Math.min(input.limit ?? 30, 100);
    if (sources.includes("t3")) {
      const rows = yield* sql<{
        message_id: string;
        thread_id: string;
        role: "user" | "assistant";
        text: string;
        created_at: string;
      }>`SELECT m.message_id, m.thread_id, m.role, substr(m.text, 1, 4096) AS text, m.created_at FROM projection_thread_messages m JOIN projection_threads t ON t.thread_id = m.thread_id WHERE t.project_id = ${input.projectId} AND t.deleted_at IS NULL AND instr(lower(m.text), lower(${input.query})) > 0 AND (${input.role ?? null} IS NULL OR m.role = ${input.role ?? null}) AND (${input.after ?? null} IS NULL OR m.created_at >= ${input.after ?? null}) AND (${input.before ?? null} IS NULL OR m.created_at <= ${input.before ?? null}) AND (${input.includeCurrent ? null : (input.currentThreadId ?? null)} IS NULL OR t.thread_id != ${input.includeCurrent ? null : (input.currentThreadId ?? null)}) ORDER BY m.created_at DESC, m.message_id LIMIT ${limit + 1}`.pipe(
        Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })),
      );
      const counts = new Map<string, number>();
      for (const row of rows) {
        const count = counts.get(row.thread_id) ?? 0;
        if (count >= (input.perSession ?? 5)) continue;
        counts.set(row.thread_id, count + 1);
        const before =
          (input.contextBefore ?? 0) > 0
            ? yield* sql<{
                role: "user" | "assistant";
                text: string;
                created_at: string;
              }>`SELECT m.role, substr(m.text, 1, 4096) AS text, m.created_at FROM projection_thread_messages m JOIN projection_threads t ON t.thread_id = m.thread_id WHERE t.project_id = ${input.projectId} AND t.thread_id = ${row.thread_id} AND t.deleted_at IS NULL AND (m.created_at < ${row.created_at} OR (m.created_at = ${row.created_at} AND m.message_id < ${row.message_id})) AND (m.role != 'system' OR ${input.includeSystem ? 1 : 0}) AND (m.role != 'tool' OR ${input.includeTools ? 1 : 0}) ORDER BY m.created_at DESC, m.message_id DESC LIMIT ${Math.min(input.contextBefore ?? 0, 5)}`.pipe(
                Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })),
              )
            : [];
        const after =
          (input.contextAfter ?? 0) > 0
            ? yield* sql<{
                role: "user" | "assistant";
                text: string;
                created_at: string;
              }>`SELECT m.role, substr(m.text, 1, 4096) AS text, m.created_at FROM projection_thread_messages m JOIN projection_threads t ON t.thread_id = m.thread_id WHERE t.project_id = ${input.projectId} AND t.thread_id = ${row.thread_id} AND t.deleted_at IS NULL AND (m.created_at > ${row.created_at} OR (m.created_at = ${row.created_at} AND m.message_id > ${row.message_id})) AND (m.role != 'system' OR ${input.includeSystem ? 1 : 0}) AND (m.role != 'tool' OR ${input.includeTools ? 1 : 0}) ORDER BY m.created_at, m.message_id LIMIT ${Math.min(input.contextAfter ?? 0, 5)}`.pipe(
                Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })),
              )
            : [];
        const map = (message: (typeof before)[number]) => ({
          role: message.role,
          text: message.text,
          createdAt: message.created_at,
        });
        hits.push({
          source: "t3",
          sourceRef: `t3:${row.thread_id}`,
          sourceSessionId: row.thread_id,
          threadId: ThreadId.make(row.thread_id),
          messageId: row.message_id,
          message: { role: row.role, text: row.text, createdAt: row.created_at },
          before: [...before].reverse().map(map),
          after: after.map(map),
        });
      }
    }
    if (sources.includes("claudeAgent") || sources.includes("codex")) {
      const sessions = yield* external(cwd, Math.min(input.maxSessions ?? 50, 200));
      for (const session of sessions) {
        if (!sources.includes(session.source)) continue;
        hits.push(...searchHistorySession(session, input));
        if (hits.length > limit) break;
      }
      if (sessions.length >= (input.maxSessions ?? 50))
        warnings.push("external session scan reached session budget");
    }
    if (sources.includes("pi") || sources.includes("opencode")) {
      if (Option.isNone(readers))
        warnings.push("pi/opencode: approved history store roots are not configured");
      else {
        const newer = yield* readers.value.list(cwd, Math.min(input.maxSessions ?? 50, 200));
        warnings.push(...newer.warnings);
        if (newer.truncated) warnings.push("external session scan reached session budget");
        for (const session of newer.sessions)
          if (sources.includes(session.source)) hits.push(...searchHistorySession(session, input));
      }
    }
    return {
      hits: hits.slice(0, limit),
      truncated: hits.length > limit || warnings.some((warning) => warning.includes("budget")),
      warnings,
    };
  });
  return HistorySearch.of({ search, readHistory });
});
export const layer = Layer.effect(HistorySearch, make);
