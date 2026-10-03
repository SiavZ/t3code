import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as ExternalHistoryReaders from "./ExternalHistoryReaders.ts";

it.layer(NodeServices.layer)("ExternalHistoryReaders", (it) => {
  it.effect(
    "reads Pi v3 active branch and OpenCode 1.3.15 text parts without mutating fixtures",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped();
        const cwd = `${root}/workspace`;
        yield* fs.makeDirectory(cwd);
        const pi = `${root}/pi`;
        yield* fs.makeDirectory(pi);
        const now = "2026-10-03T00:00:00.000Z";
        const contents = [
          { type: "session", version: 3, id: "pi-session", timestamp: now, cwd },
          {
            type: "message",
            id: "user",
            parentId: null,
            timestamp: now,
            message: { role: "user", content: "needle pi fixture" },
          },
          {
            type: "message",
            id: "abandoned",
            parentId: "user",
            timestamp: now,
            message: { role: "assistant", content: [{ type: "text", text: "abandoned branch" }] },
          },
          {
            type: "message",
            id: "active",
            parentId: "user",
            timestamp: now,
            message: {
              role: "assistant",
              content: [
                { type: "text", text: "active branch" },
                { type: "image", data: "ignored" },
              ],
            },
          },
        ]
          .map((record) => JSON.stringify(record))
          .join("\n");
        const piFile = `${pi}/session.jsonl`;
        yield* fs.writeFileString(piFile, contents);
        const dbPath = `${root}/opencode.db`;
        yield* Effect.sync(() => {
          const db = new NodeSqlite.DatabaseSync(dbPath);
          try {
            db.exec(
              "CREATE TABLE session(id TEXT, directory TEXT, time_created INTEGER, time_updated INTEGER); CREATE TABLE message(id TEXT, session_id TEXT, time_created INTEGER, data TEXT); CREATE TABLE part(id TEXT, message_id TEXT, data TEXT)",
            );
            db.prepare("INSERT INTO session VALUES (?, ?, ?, ?)").run(
              "opencode",
              cwd,
              Date.parse(now),
              Date.parse(now),
            );
            db.prepare("INSERT INTO message VALUES (?, ?, ?, ?)").run(
              "m",
              "opencode",
              Date.parse(now),
              JSON.stringify({ role: "user", time: { created: Date.parse(now) } }),
            );
            db.prepare("INSERT INTO part VALUES (?, ?, ?)").run(
              "p",
              "m",
              JSON.stringify({ type: "text", text: "needle opencode fixture" }),
            );
            db.prepare("INSERT INTO part VALUES (?, ?, ?)").run(
              "image",
              "m",
              JSON.stringify({ type: "file", url: "data:ignored" }),
            );
          } finally {
            db.close();
          }
        });
        const before = yield* fs.readFile(dbPath);
        const stores = Layer.succeed(ExternalHistoryReaders.ExternalHistoryStores, {
          get: Effect.succeed([
            { source: "pi" as const, path: pi },
            { source: "opencode" as const, path: dbPath },
          ]),
        });
        yield* Effect.gen(function* () {
          const readers = yield* ExternalHistoryReaders.ExternalHistoryReaders;
          const result = yield* readers.list(cwd, 20);
          expect(result.warnings).toEqual([]);
          expect(result.sessions.map((session) => session.source)).toEqual(["pi", "opencode"]);
          expect(result.sessions[0]?.messages.map((message) => message.text)).toEqual([
            "needle pi fixture",
            "active branch",
          ]);
          expect(result.sessions[1]?.messages[0]?.text).toBe("needle opencode fixture");
          expect((yield* readers.list("/foreign", 20)).sessions).toEqual([]);
        }).pipe(Effect.provide(ExternalHistoryReaders.layer.pipe(Layer.provide(stores))));
        expect(yield* fs.readFile(dbPath)).toEqual(before);
        expect(yield* fs.readFileString(piFile)).toBe(contents);
      }),
  );
});
