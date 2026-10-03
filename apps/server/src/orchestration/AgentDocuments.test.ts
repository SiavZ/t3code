import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { migrateAgentDocuments as migrate } from "../testUtils/agentDocumentsSchema.ts";
import * as AgentDocuments from "./AgentDocuments.ts";
const database = NodeSqliteClient.layer({ filename: ":memory:" });
it.layer(Layer.merge(database, AgentDocuments.layer.pipe(Layer.provide(database))))(
  "AgentDocuments",
  (it) => {
    it.effect(
      "replays exact operations, rejects stale revisions and retains closed documents",
      () =>
        Effect.gen(function* () {
          yield* migrate;
          const service = yield* AgentDocuments.AgentDocuments;
          const scope = { projectId: "project", ownerThreadId: "thread", documentId: "doc" };
          const mount = {
            ...scope,
            operation: "mount" as const,
            operationId: "mount",
            expectedRevision: 0,
            title: "Report",
            body: { kind: "markdown" as const, content: "original" },
            placement: "panel" as const,
            lifetime: "persistent" as const,
          };
          assert.equal((yield* service.write(mount)).revision, 1);
          assert.equal((yield* service.write(mount)).revision, 1);
          yield* service.write({
            ...scope,
            operation: "close",
            operationId: "close",
            expectedRevision: 1,
          });
          assert.deepEqual(
            yield* service.read({
              projectId: "project",
              ownerThreadId: "thread",
              operation: "list",
            }),
            [],
          );
          assert.equal((yield* service.read({ ...scope, operation: "get" }))[0]?.closed, true);
          const stale = yield* service
            .write({ ...scope, operation: "reopen", operationId: "stale", expectedRevision: 1 })
            .pipe(Effect.flip);
          assert.equal(stale.code, "conflict");
          assert.equal(
            (yield* service.write({
              ...scope,
              operation: "reopen",
              operationId: "reopen",
              expectedRevision: 2,
            })).closed,
            false,
          );
          yield* service.write({
            ...scope,
            operation: "replace",
            operationId: "applet",
            expectedRevision: 3,
            body: {
              kind: "applet",
              view: { type: "button", label: "Submit", on_press: { action: "submit" } },
              state: { q: "before" },
            },
          });
          const action = {
            ...scope,
            expectedRevision: 4,
            actionId: "click",
            clientId: "client",
            action: { action: "submit" },
            state: { q: "accepted" },
          };
          const sequence = yield* service.action(action);
          assert.equal(yield* service.action(action), sequence);
          yield* service.write({
            ...scope,
            operation: "patch",
            operationId: "state",
            expectedRevision: 5,
            patches: [{ op: "replace", path: "/state/q", value: "after" }],
          });
          const accepted = yield* service.wait({ ...scope, afterSequence: 0 });
          assert.equal(accepted[0]!.input.state.q, "accepted");
          const invalid = yield* service
            .write({
              ...scope,
              operation: "patch",
              operationId: "invalid",
              expectedRevision: 6,
              patches: [
                { op: "replace", path: "/title", value: "changed" },
                { op: "remove", path: "/view/type" },
              ],
            })
            .pipe(Effect.flip);
          assert.equal(invalid.code, "invalid");
          assert.equal((yield* service.read({ ...scope, operation: "get" }))[0]?.title, "Report");
          const denied = yield* service
            .read({ ...scope, ownerThreadId: "foreign", operation: "get" })
            .pipe(Effect.flip);
          assert.equal(denied.code, "notFound");
          const ephemeral = {
            ...mount,
            documentId: "ephemeral",
            operationId: "ephemeral-mount",
            lifetime: "ephemeral" as const,
          };
          yield* service.write(ephemeral);
          yield* service.recover;
          assert.equal(
            (yield* service.read({ ...scope, documentId: "ephemeral", operation: "get" }))[0]
              ?.closed,
            true,
          );
          assert.equal((yield* service.read({ ...scope, operation: "get" }))[0]?.closed, false);
          yield* service.write({
            ...scope,
            documentId: "ephemeral",
            operation: "reopen",
            expectedRevision: 2,
            operationId: "ephemeral-reopen",
          });
          yield* service.releaseOwner({ ...scope, operationId: "terminal", reason: "turnEnded" });
          assert.equal(
            (yield* service.read({ ...scope, documentId: "ephemeral", operation: "get" }))[0]
              ?.closed,
            true,
          );
          assert.equal((yield* service.read({ ...scope, operation: "get" }))[0]?.closed, false);
          yield* service.releaseOwner({ ...scope, operationId: "deleted", reason: "deleted" });
          assert.equal((yield* service.read({ ...scope, operation: "get" }))[0]?.closed, true);
          yield* service.releaseOwner({ ...scope, operationId: "deleted", reason: "deleted" });
        }),
    );
    it.effect(
      "closes only ephemeral documents mounted before the turn end and never replays a release",
      () =>
        Effect.gen(function* () {
          const sql = yield* SqlClient.SqlClient;
          yield* sql`DELETE FROM agent_document_operations`;
          yield* sql`DELETE FROM agent_documents`;
          const service = yield* AgentDocuments.AgentDocuments;
          const owner = { projectId: "project", ownerThreadId: "lifecycle" };
          const event = (sequence: number) =>
            sql`INSERT INTO orchestration_events (sequence, event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at, actor_kind, payload_json, metadata_json) VALUES (${sequence}, ${`event-${sequence}`}, 'thread', 'lifecycle', ${sequence}, 'thread.session-set', '2026-10-03T00:00:00Z', 'server', '{}', '{}')`;
          const mount = (documentId: string, lifetime: "ephemeral" | "persistent") =>
            service.write({
              ...owner,
              documentId,
              operation: "mount",
              operationId: `mount-${documentId}`,
              expectedRevision: 0,
              title: documentId,
              body: { kind: "markdown", content: documentId },
              placement: "panel",
              lifetime,
            });
          const closed = (documentId: string) =>
            service
              .read({ ...owner, documentId, operation: "get" })
              .pipe(Effect.map((docs) => docs[0]!.closed));
          const turnEnded = (sequence: number) =>
            service.releaseOwner({
              ...owner,
              operationId: `lifecycle:${sequence}`,
              reason: "turnEnded",
              throughSequence: sequence,
            });

          yield* event(100);
          yield* mount("old", "ephemeral");
          yield* mount("kept", "persistent");
          yield* event(101);
          yield* mount("new", "ephemeral");
          // The terminal event for the old turn is processed after the new mount.
          yield* turnEnded(101);
          assert.equal(yield* closed("old"), true);
          assert.equal(yield* closed("new"), false);
          assert.equal(yield* closed("kept"), false);

          // Reopen after the release, then replay the same terminal event.
          yield* service.write({
            ...owner,
            documentId: "old",
            operation: "reopen",
            operationId: "reopen-old",
            expectedRevision: 2,
          });
          yield* turnEnded(101);
          assert.equal(yield* closed("old"), false);

          yield* event(102);
          yield* turnEnded(102);
          assert.equal(yield* closed("new"), true);
          assert.equal(yield* closed("old"), true);
          assert.equal(yield* closed("kept"), false);
          yield* service.releaseOwner({
            ...owner,
            operationId: "lifecycle:103",
            reason: "deleted",
          });
          assert.equal(yield* closed("kept"), true);
        }),
    );
  },
);
