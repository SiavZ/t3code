import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  HistorySearchError,
  type NormalizedHistorySession,
  type HistoryMessage,
} from "../../../../packages/contracts/src/historySearch.ts";

export type HistoryStore =
  | { readonly source: "pi"; readonly path: string }
  | { readonly source: "opencode"; readonly path: string };
export class ExternalHistoryStores extends Context.Service<
  ExternalHistoryStores,
  { readonly get: Effect.Effect<ReadonlyArray<HistoryStore>> }
>()("t3/project/ExternalHistoryStores") {}
export const storesLayer = Layer.succeed(ExternalHistoryStores, { get: Effect.succeed([]) });
export class ExternalHistoryReaders extends Context.Service<
  ExternalHistoryReaders,
  {
    readonly list: (
      cwd: string,
      max: number,
    ) => Effect.Effect<
      { sessions: NormalizedHistorySession[]; warnings: string[]; truncated: boolean },
      HistorySearchError
    >;
  }
>()("t3/project/ExternalHistoryReaders") {}
const TextPart = Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) });
const PiRecord = Schema.Struct({
  type: Schema.String,
  version: Schema.optional(Schema.Number),
  id: Schema.optional(Schema.String),
  parentId: Schema.optional(Schema.NullOr(Schema.String)),
  timestamp: Schema.optional(Schema.String),
  cwd: Schema.optional(Schema.String),
  modelId: Schema.optional(Schema.String),
  message: Schema.optional(
    Schema.Struct({
      role: Schema.String,
      content: Schema.Union([Schema.String, Schema.Array(TextPart)]),
    }),
  ),
});
const decodePi = Schema.decodeUnknownOption(PiRecord);
const ref = (source: string, path: string, id: string) =>
  `${source}:${NodeCrypto.createHash("sha256").update(`${path}\0${id}`).digest("hex")}`;

/** Pi v3 JSONL, following the last leaf's parent chain rather than merging branches. */
export function parsePiSession(contents: string, file: string): NormalizedHistorySession | null {
  if (Buffer.byteLength(contents) > 4194304) return null;
  const records = contents
    .split("\n")
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const decoded = decodePi(JSON.parse(line));
        return decoded._tag === "Some" ? [decoded.value] : [];
      } catch {
        return [];
      }
    });
  if (records.length > 20000) return null;
  const header = records[0];
  if (
    !header ||
    header.type !== "session" ||
    header.version !== 3 ||
    !header.id ||
    !header.cwd ||
    !header.timestamp
  )
    return null;
  const entries = new Map(
    records
      .slice(1)
      .filter((record) => record.id)
      .map((record) => [record.id!, record]),
  );
  const chain: typeof records = [];
  let leaf = records.at(-1);
  const seen = new Set<string>();
  while (leaf && leaf.type !== "session" && leaf.id) {
    if (seen.has(leaf.id)) return null;
    seen.add(leaf.id);
    chain.push(leaf);
    leaf = leaf.parentId ? entries.get(leaf.parentId) : undefined;
  }
  chain.reverse();
  const messages: HistoryMessage[] = [];
  let model: string | null = null;
  for (const record of chain) {
    if (record.type === "model_change" && record.modelId) model = record.modelId;
    if (record.type !== "message" || !record.message) continue;
    const role = record.message.role === "toolResult" ? "tool" : record.message.role;
    if (role !== "user" && role !== "assistant" && role !== "system" && role !== "tool") continue;
    const content = record.message.content;
    const text =
      typeof content === "string"
        ? content
        : content
            .filter((part) => part.type === "text")
            .map((part) => part.text ?? "")
            .join("\n");
    if (text) messages.push({ role, text, createdAt: record.timestamp ?? header.timestamp });
  }
  return {
    source: "pi",
    sourceRef: ref("pi", file, header.id),
    sourceSessionId: header.id,
    cwd: header.cwd,
    model,
    createdAt: header.timestamp,
    updatedAt: messages.at(-1)?.createdAt ?? header.timestamp,
    messages,
  };
}

const make = Effect.gen(function* () {
  const stores = yield* ExternalHistoryStores;
  const list = (cwd: string, max: number) =>
    stores.get.pipe(
      Effect.flatMap((configured) =>
        Effect.tryPromise({
          try: async (signal) => {
            const sessions: NormalizedHistorySession[] = [];
            const warnings: string[] = [];
            let truncated = false;
            let bytes = 0;
            for (const store of configured.slice(0, 10)) {
              if (signal.aborted) break;
              try {
                if (store.source === "pi") {
                  const root = await NodeFSP.realpath(store.path);
                  const directories = [root];
                  let visited = 0;
                  for (let index = 0; index < directories.length && index < 201; index++) {
                    const directory = await NodeFSP.opendir(directories[index]!);
                    for await (const entry of directory) {
                      if (
                        signal.aborted ||
                        ++visited > 2000 ||
                        sessions.length >= max ||
                        bytes > 8388608
                      ) {
                        truncated = true;
                        break;
                      }
                      const file = NodePath.join(directories[index]!, entry.name);
                      if (entry.isDirectory() && index === 0) directories.push(file);
                      if (!entry.isFile() || !entry.name.endsWith(".jsonl")) continue;
                      const handle = await NodeFSP.open(file, "r");
                      try {
                        const stat = await handle.stat();
                        if (!stat.isFile() || stat.size > 4194304) {
                          warnings.push("pi: oversized transcript skipped");
                          continue;
                        }
                        bytes += stat.size;
                        const buffer = Buffer.alloc(4194305);
                        const read = await handle.read(buffer, 0, buffer.length, 0);
                        if (read.bytesRead > 4194304) {
                          warnings.push("pi: transcript grew beyond read budget");
                          continue;
                        }
                        const contents = buffer.toString("utf8", 0, read.bytesRead);
                        const session = parsePiSession(contents, file);
                        if (
                          session &&
                          session.cwd &&
                          NodePath.resolve(session.cwd) === NodePath.resolve(cwd)
                        )
                          sessions.push(session);
                      } finally {
                        await handle.close();
                      }
                    }
                    if (truncated) break;
                  }
                } else {
                  const { DatabaseSync } = await import("node:sqlite");
                  const db = new DatabaseSync(store.path, { readOnly: true });
                  try {
                    db.exec("PRAGMA query_only = ON");
                    const rows = db
                      .prepare(
                        "SELECT id, directory, time_created, time_updated FROM session WHERE directory = ? ORDER BY time_updated DESC LIMIT ?",
                      )
                      .all(cwd, max + 1);
                    if (rows.length > max) truncated = true;
                    for (const row of rows.slice(0, max)) {
                      if (signal.aborted || sessions.length >= max) {
                        truncated = true;
                        break;
                      }
                      if (
                        typeof row.id !== "string" ||
                        typeof row.directory !== "string" ||
                        typeof row.time_created !== "number" ||
                        typeof row.time_updated !== "number"
                      )
                        continue;
                      const parts = db
                        .prepare(
                          "SELECT m.id AS message_id, json_extract(m.data, '$.role') AS role, m.time_created, json_extract(p.data, '$.text') AS text FROM message m JOIN part p ON p.message_id = m.id WHERE m.session_id = ? AND json_extract(p.data, '$.type') = 'text' AND length(json_extract(p.data, '$.text')) <= 65536 ORDER BY m.time_created, m.id, p.id LIMIT 2001",
                        )
                        .all(row.id);
                      if (parts.length > 2000) {
                        warnings.push("opencode: session message budget exceeded");
                        continue;
                      }
                      const messages: HistoryMessage[] = [];
                      for (const part of parts) {
                        if (
                          (part.role !== "user" && part.role !== "assistant") ||
                          typeof part.text !== "string" ||
                          typeof part.time_created !== "number"
                        )
                          continue;
                        bytes += Buffer.byteLength(part.text);
                        if (bytes > 8388608) {
                          truncated = true;
                          break;
                        }
                        messages.push({
                          role: part.role,
                          text: part.text,
                          createdAt: new Date(part.time_created).toISOString(),
                        });
                      }
                      sessions.push({
                        source: "opencode",
                        sourceRef: ref("opencode", store.path, row.id),
                        sourceSessionId: row.id,
                        cwd: row.directory,
                        model: null,
                        createdAt: new Date(row.time_created).toISOString(),
                        updatedAt: new Date(row.time_updated).toISOString(),
                        messages,
                      });
                    }
                  } finally {
                    db.close();
                  }
                }
              } catch {
                warnings.push(`${store.source}: unreadable or unsupported store schema`);
              }
            }
            return { sessions, warnings, truncated };
          },
          catch: () => new HistorySearchError({ reason: "read-failed" }),
        }),
      ),
    );
  return ExternalHistoryReaders.of({ list });
});
export const layer = Layer.effect(ExternalHistoryReaders, make);
