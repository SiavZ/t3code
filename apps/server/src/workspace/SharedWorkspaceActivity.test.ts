import * as NodeFSP from "node:fs/promises";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import {
  CoordinationError,
  EventId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  type CoordinationMailbox,
  type CoordinationMailboxWriteInput,
  type ProviderRuntimeEvent,
} from "@t3tools/contracts";
import * as CoordinationPlans from "../orchestration/CoordinationPlans.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as SharedWorkspaceActivity from "./SharedWorkspaceActivity.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";

const completion = (
  thread: string,
  id: string,
  paths: string[],
  status = "completed",
): ProviderRuntimeEvent => ({
  eventId: EventId.make(id),
  provider: ProviderDriverKind.make("codex"),
  threadId: ThreadId.make(thread),
  createdAt: "2026-10-03T00:00:00Z",
  type: "item.completed",
  payload: {
    itemType: "file_change",
    status: status === "failed" ? "failed" : "completed",
    data: {
      completedAtMs: 1_791_000_000_000,
      threadId: thread,
      turnId: "turn",
      item: {
        type: "fileChange",
        id,
        status,
        changes: paths.map((path) => ({ path, kind: { type: "update" }, diff: "@@ fixture" })),
      },
    },
  },
});

it.layer(NodeServices.layer)("SharedWorkspaceActivity", (it) => {
  it.effect(
    "warns only prior sibling owners in the same canonical checkout and ignores failed/native-less edits",
    () =>
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const root = yield* fs.makeTempDirectoryScoped();
        const other = yield* fs.makeTempDirectoryScoped();
        yield* fs.writeFileString(`${root}/file.ts`, "fixture");
        const writes: CoordinationMailboxWriteInput[] = [];
        let revision = 0;
        let queries = 0;
        let conflictOnce = true;
        const attempts: CoordinationMailboxWriteInput[] = [];
        const snapshots = Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
          getThreadShellById: (id: ThreadId) => {
            queries++;
            return Effect.succeed(
              Option.some({
                projectId: ProjectId.make("project"),
                worktreePath: id === "isolated" ? other : null,
                worker: { rootThreadId: ThreadId.make("root") },
              }),
            );
          },
          getProjectShellById: () => Effect.succeed(Option.some({ workspaceRoot: root })),
        } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
        const mailbox = Layer.succeed(CoordinationPlans.CoordinationPlans, {
          mailboxRead: () => Effect.succeed({ revision } as CoordinationMailbox),
          mailboxWrite: (input: CoordinationMailboxWriteInput) =>
            Effect.gen(function* () {
              attempts.push(input);
              if (conflictOnce) {
                conflictOnce = false;
                revision++;
                return yield* new CoordinationError({
                  code: "conflict",
                  detail: "fixture concurrent revision",
                });
              }
              return yield* Effect.sync(() => {
                writes.push(input);
                revision++;
                return { revision } as CoordinationMailbox;
              });
            }),
          read: () => Effect.die("unused"),
          write: () => Effect.die("unused"),
        });
        yield* Effect.gen(function* () {
          const service = yield* SharedWorkspaceActivity.SharedWorkspaceActivity;
          const failed = yield* service.record(completion("a", "failed", ["file.ts"], "failed"));
          expect(failed.touchedFiles).toEqual([]);
          expect(queries).toBe(0);
          const first = yield* service.record(completion("a", "first", ["./file.ts"]));
          expect(first.touchedFiles).toEqual(["file.ts"]);
          expect(writes).toHaveLength(0);
          yield* service.record(completion("isolated", "isolation", ["file.ts"]));
          expect(writes).toHaveLength(0);
          const second = yield* service.record(completion("b", "second", [`${root}/file.ts`]));
          expect(second.notifiedThreadIds).toEqual(["a"]);
          expect(writes[0]?.operation).toBe("message");
          expect(writes[0]?.callerThreadId).toBe("root");
          expect(attempts).toHaveLength(2);
          expect(attempts[0]?.commandId).not.toBe(attempts[1]?.commandId);
          const bounded = yield* service
            .record(
              completion(
                "c",
                "oversize",
                Array.from({ length: 129 }, (_, i) => `file-${i}.ts`),
              ),
            )
            .pipe(Effect.flip);
          expect(bounded._tag === "SharedWorkspaceActivityError" && bounded.reason).toBe(
            "limit-exceeded",
          );
          expect("text" in writes[0]!).toBe(true);
          yield* service.record(completion("b", "second", ["file.ts"]));
          expect(writes).toHaveLength(1);
          const unsupported = yield* service.record({
            ...completion("c", "claude", ["file.ts"]),
            provider: ProviderDriverKind.make("claudeAgent"),
          });
          expect(unsupported.supported).toBe(false);
          const escape = yield* service
            .record(completion("c", "escape", ["../outside.ts"]))
            .pipe(Effect.flip);
          expect(escape._tag === "SharedWorkspaceActivityError" && escape.reason).toBe(
            "invalid-path",
          );
          yield* Effect.promise(() => NodeFSP.symlink(other, `${root}/outside-link`));
          const symlink = yield* service
            .record(completion("c", "symlink", ["outside-link/new.ts"]))
            .pipe(Effect.flip);
          expect(symlink._tag === "SharedWorkspaceActivityError" && symlink.reason).toBe(
            "invalid-path",
          );
          expect(writes).toHaveLength(1);
          const renameEvent = completion("c", "rename", ["deleted.ts"]);
          if (renameEvent.type !== "item.completed")
            return yield* Effect.die("fixture event mismatch");
          const rename = yield* service.record({
            ...renameEvent,
            payload: {
              ...renameEvent.payload,
              data: {
                completedAtMs: 1_791_000_000_000,
                threadId: "c",
                turnId: "turn",
                item: {
                  type: "fileChange",
                  id: "rename",
                  status: "completed",
                  changes: [
                    {
                      path: "deleted.ts",
                      kind: { type: "update", move_path: "new/nested.ts" },
                      diff: "@@ fixture",
                    },
                  ],
                },
              },
            },
          });
          expect(rename.touchedFiles).toEqual(["deleted.ts", "new/nested.ts"]);
          expect(writes).toHaveLength(1);
        }).pipe(
          Effect.provide(
            SharedWorkspaceActivity.layer.pipe(
              Layer.provide(snapshots),
              Layer.provide(mailbox),
              Layer.provide(WorkspacePaths.layer),
            ),
          ),
        );
      }),
  );
});
