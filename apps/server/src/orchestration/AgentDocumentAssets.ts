// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ThreadId } from "@t3tools/contracts";
import {
  AgentDocumentAssetPrepareInput,
  AgentDocumentError,
  type AgentDocumentAsset,
} from "../../../../packages/contracts/src/agentDocuments.ts";
import * as ServerConfig from "../config.ts";
import * as ProjectionSnapshotQuery from "./Services/ProjectionSnapshotQuery.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import { createAttachmentId } from "../attachmentStore.ts";
import { resolveAttachmentRelativePath } from "../attachmentPaths.ts";

export class AgentDocumentAssets extends Context.Service<
  AgentDocumentAssets,
  {
    readonly prepare: (
      input: AgentDocumentAssetPrepareInput,
    ) => Effect.Effect<AgentDocumentAsset, AgentDocumentError>;
  }
>()("t3/orchestration/AgentDocumentAssets") {}
const invalid = () =>
  new AgentDocumentError({
    code: "invalid",
    detail: "PDF must be a regular non-symlink file inside the owning workspace, at most 20 MiB.",
  });
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const config = yield* ServerConfig.ServerConfig;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const paths = yield* WorkspacePaths.WorkspacePaths;
  const prepare = Effect.fn("AgentDocumentAssets.prepare")(function* (
    raw: AgentDocumentAssetPrepareInput,
  ) {
    const input = yield* Schema.decodeUnknownEffect(AgentDocumentAssetPrepareInput)(raw).pipe(
      Effect.mapError(invalid),
    );
    const thread = yield* snapshots.getThreadShellById(ThreadId.make(input.ownerThreadId));
    if (Option.isNone(thread))
      return yield* new AgentDocumentError({
        code: "notFound",
        detail: "Owning thread not found.",
      });
    const project = yield* snapshots.getProjectShellById(thread.value.projectId);
    if (Option.isNone(project))
      return yield* new AgentDocumentError({
        code: "notFound",
        detail: "Owning project not found.",
      });
    const root = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(thread.value.worktreePath ?? project.value.workspaceRoot),
      catch: invalid,
    });
    const source = yield* paths
      .resolveRelativePathWithinRoot({ workspaceRoot: root, relativePath: input.relativePath })
      .pipe(Effect.mapError(invalid));
    const canonical = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(source.absolutePath),
      catch: invalid,
    });
    if (canonical !== source.absolutePath || !canonical.startsWith(`${root}${NodePath.sep}`))
      return yield* invalid();
    const bytes = yield* Effect.tryPromise({
      try: async () => {
        const handle = await NodeFSP.open(
          canonical,
          NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW,
        );
        try {
          const stat = await handle.stat();
          if (!stat.isFile() || stat.size < 8 || stat.size > 20 * 1024 * 1024) throw invalid();
          const buffer = Buffer.alloc(stat.size + 1);
          let offset = 0;
          while (offset < buffer.length) {
            const result = await handle.read(buffer, offset, buffer.length - offset, offset);
            if (!result.bytesRead) break;
            offset += result.bytesRead;
          }
          if (offset !== stat.size || buffer.subarray(0, 5).toString("ascii") !== "%PDF-")
            throw invalid();
          return buffer.subarray(0, offset);
        } finally {
          await handle.close();
        }
      },
      catch: invalid,
    });
    const assetId = createAttachmentId(input.ownerThreadId, ".pdf");
    if (!assetId) return yield* invalid();
    const destination = resolveAttachmentRelativePath({
      attachmentsDir: config.attachmentsDir,
      relativePath: `${assetId}.pdf`,
    });
    if (!destination) return yield* invalid();
    const asset = {
      assetId,
      ownerThreadId: input.ownerThreadId,
      projectId: thread.value.projectId,
      byteLength: bytes.byteLength,
    };
    yield* Effect.tryPromise({
      try: () => NodeFSP.mkdir(config.attachmentsDir, { recursive: true }),
      catch: invalid,
    });
    yield* Effect.tryPromise({
      try: () => NodeFSP.writeFile(destination, bytes, { flag: "wx", mode: 0o600 }),
      catch: invalid,
    });
    yield* sql`INSERT INTO agent_document_assets (asset_id, project_id, owner_thread_id, byte_length) VALUES (${assetId}, ${asset.projectId}, ${asset.ownerThreadId}, ${asset.byteLength})`.pipe(
      Effect.onError(() =>
        Effect.promise(() => NodeFSP.unlink(destination).catch(() => undefined)),
      ),
    );
    return asset;
  });
  return AgentDocumentAssets.of({
    prepare: (input) =>
      prepare(input).pipe(
        Effect.mapError((error) =>
          error instanceof AgentDocumentError
            ? error
            : new AgentDocumentError({ code: "storage", detail: "PDF snapshot failed." }),
        ),
      ),
  });
});
export const layer = Layer.effect(AgentDocumentAssets, make);
