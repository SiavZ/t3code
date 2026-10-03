import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId } from "@t3tools/contracts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import * as HistorySearch from "./HistorySearch.ts";

it.layer(Layer.mergeAll(NodeServices.layer, NodeSqliteClient.layer({ filename: ":memory:" })))(
  "HistorySearch",
  (it) => {
    it.effect("searches only project messages and rejects foreign history reads", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* sql`CREATE TABLE projection_threads (thread_id TEXT, project_id TEXT, deleted_at TEXT)`;
        yield* sql`CREATE TABLE projection_thread_messages (message_id TEXT, thread_id TEXT, role TEXT, text TEXT, created_at TEXT)`;
        yield* sql`INSERT INTO projection_threads VALUES ('own', 'project', NULL), ('foreign', 'other', NULL)`;
        yield* sql`INSERT INTO projection_thread_messages VALUES ('before', 'own', 'assistant', 'earlier context', '2026-10-02T23:59:59Z'), ('one', 'own', 'user', 'needle fixture', '2026-10-03T00:00:00Z'), ('after', 'own', 'assistant', 'later context', '2026-10-03T00:00:01Z'), ('two', 'foreign', 'user', 'needle secret', '2026-10-03T00:00:00Z')`;
        const snapshots = Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
          getProjectShellById: () => Effect.succeed(Option.some({ workspaceRoot: "/fixture" })),
        } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
        const scanner = Layer.succeed(AgentSessionScanner.AgentSessionScanner, {
          scan: Effect.succeed({ candidates: [], scannedAt: "2026-10-03T00:00:00.000Z" }),
          recentThreads: () => Stream.empty,
        });
        yield* Effect.gen(function* () {
          const service = yield* HistorySearch.HistorySearch;
          const projectId = ProjectId.make("project");
          const result = yield* service.search({
            projectId,
            query: "needle",
            sources: ["t3", "cursor"],
            contextBefore: 1,
            contextAfter: 1,
          });
          expect(result.hits.map((hit) => hit.message.text)).toEqual(["needle fixture"]);
          expect(result.hits[0]?.messageId).toBe("one");
          expect(result.hits[0]?.before.map((message) => message.text)).toEqual([
            "earlier context",
          ]);
          expect(result.hits[0]?.after.map((message) => message.text)).toEqual(["later context"]);
          expect(result.warnings[0]).toContain("cursor: unsupported");
          const error = yield* service
            .readHistory({ projectId, sourceRef: "t3:foreign" })
            .pipe(Effect.flip);
          expect(error.reason).toBe("not-found");
          const session = yield* service.readHistory({ projectId, sourceRef: "t3:own" });
          expect(session.messages.map((message) => message.text)).toEqual([
            "earlier context",
            "needle fixture",
            "later context",
          ]);
        }).pipe(
          Effect.provide(
            HistorySearch.layer.pipe(Layer.provide(snapshots), Layer.provide(scanner)),
          ),
        );
      }),
    );
  },
);
