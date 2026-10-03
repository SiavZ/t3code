import { expect, it } from "@effect/vitest";
import { Effect } from "effect";
import * as NodeFSP from "node:fs/promises";
import * as NodeChildProcess from "node:child_process";
import * as NodeUtil from "node:util";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as Snapshots from "./SourceSnapshots.ts";
const execute = NodeUtil.promisify(NodeChildProcess.execFile);
it.effect(
  "freezes real test-owned git source bytes and excludes tracked secrets, ignored files and symlinks",
  () =>
    Effect.acquireUseRelease(
      Effect.promise(() =>
        NodeFSP.mkdtemp(
          NodePath.join(
            process.env.JCODE_SCRATCH_DIR ?? NodeOS.tmpdir(),
            "source-snapshot-fixture-",
          ),
        ),
      ),
      (root) =>
        Effect.gen(function* () {
          yield* Effect.promise(async () => {
            await execute("git", ["init", "-q", root]);
            await NodeFSP.mkdir(NodePath.join(root, "src"));
            await NodeFSP.writeFile(NodePath.join(root, "src/main.ts"), "original");
            await NodeFSP.writeFile(NodePath.join(root, ".env"), "fixture-secret");
            await NodeFSP.writeFile(NodePath.join(root, ".gitignore"), "ignored.txt\n");
            await NodeFSP.writeFile(NodePath.join(root, "ignored.txt"), "ignored");
            await NodeFSP.symlink("src/main.ts", NodePath.join(root, "linked.ts"));
            await execute("git", [
              "-C",
              root,
              "add",
              "src/main.ts",
              ".env",
              ".gitignore",
              "linked.ts",
            ]);
          });
          const snapshots = yield* Snapshots.SourceSnapshots;
          const before = yield* snapshots.prepare(root);
          expect(before.files.map((file) => file.path)).toEqual([".gitignore", "src/main.ts"]);
          expect(before.excluded).toEqual([".env", "linked.ts"]);
          yield* Effect.promise(() =>
            NodeFSP.writeFile(NodePath.join(root, "src/main.ts"), "changed"),
          );
          expect(Buffer.from(before.files[1]!.content_base64, "base64").toString()).toBe(
            "original",
          );
          const after = yield* snapshots.prepare(root);
          expect(after.digest).not.toBe(before.digest);
          yield* Effect.promise(() =>
            NodeFSP.writeFile(
              NodePath.join(root, "oversized.bin"),
              Buffer.alloc(2 * 1024 * 1024 + 1),
            ),
          );
          expect((yield* Effect.flip(snapshots.prepare(root))).reason).toBe("too-large");
        }).pipe(Effect.provide(Snapshots.layer)),
      (root) => Effect.promise(() => NodeFSP.rm(root, { recursive: true, force: true })),
    ),
);
