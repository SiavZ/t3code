import { Context, Effect, Layer, Schema } from "effect";
import * as NodeFS from "node:fs";
const { constants } = NodeFS;
import * as NodeFSP from "node:fs/promises";
const { lstat, open, realpath } = NodeFSP;
import * as NodeChildProcess from "node:child_process";
const { execFile } = NodeChildProcess;
import * as NodeUtil from "node:util";
const { promisify } = NodeUtil;
import * as NodePath from "node:path";
const { resolve, relative, sep } = NodePath;
import * as NodeCrypto from "node:crypto";
const { createHash, randomUUID } = NodeCrypto;
const execute = promisify(execFile);
export class SnapshotError extends Schema.TaggedError<SnapshotError>()("SnapshotError", {
  reason: Schema.Literals([
    "invalid-root",
    "unsafe-path",
    "changed-source",
    "too-large",
    "read-failed",
  ]),
}) {}
export interface SourceSnapshot {
  readonly snapshotId: string;
  readonly root: string;
  readonly digest: string;
  readonly files: ReadonlyArray<{ readonly path: string; readonly content_base64: string }>;
  readonly excluded: ReadonlyArray<string>;
  readonly bytes: number;
}
export class SourceSnapshots extends Context.Service<
  SourceSnapshots,
  {
    /** Caller resolves project root from authenticated environment, not an agent absolute path. */
    readonly prepare: (root: string) => Effect.Effect<SourceSnapshot, SnapshotError>;
  }
>()("t3/integrations/SourceSnapshots") {}
const credentialPath =
  /(^|\/)(\.git|\.env(?:\..*)?|id_rsa|id_ed25519|credentials(?:\..*)?|secrets?(?:\..*)?|node_modules|dist|target)(\/|$)|\.(pem|key|p12|pfx)$/i;
export const layer = Layer.succeed(SourceSnapshots, {
  prepare: (root) =>
    Effect.tryPromise({
      try: async () => {
        const canonical = await realpath(root);
        const top = await execute("git", ["-C", canonical, "rev-parse", "--show-toplevel"], {
          maxBuffer: 1_000_000,
        });
        if (top.stdout.trim() !== canonical) throw new SnapshotError({ reason: "invalid-root" });
        const listing = await execute(
          "git",
          ["-C", canonical, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
          { maxBuffer: 4_000_000 },
        );
        const paths = [...new Set(listing.stdout.split("\0").filter(Boolean))].sort();
        if (paths.length > 10_000) throw new SnapshotError({ reason: "too-large" });
        const files: { path: string; content_base64: string }[] = [];
        const excluded: string[] = [];
        let bytes = 0;
        for (const path of paths) {
          if (credentialPath.test(path)) {
            excluded.push(path);
            continue;
          }
          if (
            /[\x00-\x1f\x7f]/.test(path) ||
            path.startsWith("/") ||
            path.split("/").includes("..")
          )
            throw new SnapshotError({ reason: "unsafe-path" });
          const absolute = resolve(canonical, path);
          const stat = await lstat(absolute).catch((error: NodeJS.ErrnoException) => {
            if (error.code === "ENOENT") return undefined;
            throw error;
          });
          if (!stat) {
            excluded.push(path);
            continue;
          }
          if (stat.isSymbolicLink() || !stat.isFile()) {
            excluded.push(path);
            continue;
          }
          const actual = await realpath(absolute);
          if (relative(canonical, actual).startsWith(`..${sep}`) || actual !== absolute)
            throw new SnapshotError({ reason: "unsafe-path" });
          if (stat.size > 2 * 1024 * 1024 || bytes + stat.size > 20 * 1024 * 1024)
            throw new SnapshotError({ reason: "too-large" });
          const descriptor = await open(absolute, constants.O_RDONLY | constants.O_NOFOLLOW);
          try {
            const before = await descriptor.stat();
            if (before.ino !== stat.ino || before.dev !== stat.dev || before.size !== stat.size)
              throw new SnapshotError({ reason: "changed-source" });
            if (before.size > 2 * 1024 * 1024) throw new SnapshotError({ reason: "too-large" });
            const bounded = Buffer.alloc(before.size + 1);
            let length = 0;
            for (;;) {
              const chunk = await descriptor.read(bounded, length, bounded.length - length, null);
              length += chunk.bytesRead;
              if (length > before.size) throw new SnapshotError({ reason: "changed-source" });
              if (chunk.bytesRead === 0) break;
            }
            const content = bounded.subarray(0, length);
            const after = await descriptor.stat();
            if (
              before.size !== after.size ||
              before.mtimeMs !== after.mtimeMs ||
              before.ctimeMs !== after.ctimeMs ||
              content.byteLength !== before.size
            )
              throw new SnapshotError({ reason: "changed-source" });
            bytes += content.byteLength;
            files.push({ path, content_base64: content.toString("base64") });
          } finally {
            await descriptor.close();
          }
        }
        const digest = createHash("sha256").update(JSON.stringify(files)).digest("hex");
        return { snapshotId: randomUUID(), root: canonical, digest, files, excluded, bytes };
      },
      catch: (error) =>
        error instanceof SnapshotError ? error : new SnapshotError({ reason: "read-failed" }),
    }),
});
