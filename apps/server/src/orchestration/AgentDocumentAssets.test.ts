// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as ServerConfig from "../config.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import * as ThreadBackgroundLiveness from "./ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "./ThreadPlanProgress.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import { runMigrations } from "../persistence/Migrations.ts";
import migrate from "../persistence/Migrations/062_AgentDocuments.ts";
import * as AgentDocumentAssets from "./AgentDocumentAssets.ts";
import * as AgentDocuments from "./AgentDocuments.ts";
const database = NodeSqliteClient.layer({ filename: ":memory:" });
const projections = OrchestrationProjectionSnapshotQueryLive.pipe(
  Layer.provide(ThreadBackgroundLiveness.layer),
  Layer.provide(ThreadPlanProgress.layer),
  Layer.provide(
    Layer.succeed(RepositoryIdentityResolver.RepositoryIdentityResolver, {
      resolve: () => Effect.succeed(null),
    }),
  ),
  Layer.provide(database),
);
const dependencies = Layer.mergeAll(
  database,
  projections,
  WorkspacePaths.layer.pipe(Layer.provide(NodeServices.layer)),
  ServerConfig.layerTest(process.cwd(), { prefix: "t3-agent-document-assets-" }).pipe(
    Layer.provide(NodeServices.layer),
  ),
  NodeServices.layer,
);
const testLayer = Layer.merge(AgentDocumentAssets.layer, AgentDocuments.layer).pipe(
  Layer.provideMerge(dependencies),
);
it.layer(testLayer)("AgentDocumentAssets", (it) => {
  it.effect(
    "snapshots a scoped PDF and denies symlinks traversal and foreign asset references",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        const config = yield* ServerConfig.ServerConfig;
        yield* runMigrations({ toMigrationInclusive: 55 });
        yield* migrate;
        const workspace = NodePath.join(config.stateDir, "fixture-workspace");
        yield* Effect.promise(() => NodeFSP.mkdir(workspace, { recursive: true }));
        const pdf = "%PDF-1.7\nimmutable fixture\n%%EOF";
        yield* Effect.promise(() => NodeFSP.writeFile(NodePath.join(workspace, "report.pdf"), pdf));
        yield* sql`INSERT INTO projection_projects (project_id, title, workspace_root, scripts_json, created_at, updated_at) VALUES ('project', 'Fixture', ${workspace}, '[]', '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z')`;
        yield* sql`INSERT INTO projection_threads (thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at) VALUES ('thread', 'project', 'Fixture', '{"instanceId":"codex","model":"gpt-5.4"}', 'approval-required', '2026-10-03T00:00:00Z', '2026-10-03T00:00:00Z')`;
        const assets = yield* AgentDocumentAssets.AgentDocumentAssets;
        const asset = yield* assets.prepare({
          ownerThreadId: "thread",
          relativePath: "report.pdf",
        });
        yield* Effect.promise(() =>
          NodeFSP.writeFile(NodePath.join(workspace, "report.pdf"), "changed source"),
        );
        assert.equal(
          yield* Effect.promise(() =>
            NodeFSP.readFile(NodePath.join(config.attachmentsDir, `${asset.assetId}.pdf`), "utf8"),
          ),
          pdf,
        );
        assert.equal(
          (yield* assets
            .prepare({ ownerThreadId: "thread", relativePath: "../outside.pdf" })
            .pipe(Effect.flip)).code,
          "invalid",
        );
        yield* Effect.promise(() =>
          NodeFSP.symlink(
            NodePath.join(config.attachmentsDir, `${asset.assetId}.pdf`),
            NodePath.join(workspace, "link.pdf"),
          ),
        );
        assert.equal(
          (yield* assets
            .prepare({ ownerThreadId: "thread", relativePath: "link.pdf" })
            .pipe(Effect.flip)).code,
          "invalid",
        );
        assert.equal(
          (yield* assets
            .prepare({ ownerThreadId: "thread", relativePath: "report.pdf" })
            .pipe(Effect.flip)).code,
          "invalid",
        );
        const documents = yield* AgentDocuments.AgentDocuments;
        const mount = {
          documentId: "pdf-doc",
          ownerThreadId: "thread",
          projectId: "project",
          operation: "mount" as const,
          operationId: "mount",
          expectedRevision: 0,
          title: "PDF",
          body: { kind: "pdf" as const, assetId: asset.assetId },
          placement: "panel" as const,
          lifetime: "persistent" as const,
        };
        assert.equal((yield* documents.write(mount)).body.kind, "pdf");
        assert.equal(
          (yield* documents
            .write({ ...mount, documentId: "foreign", ownerThreadId: "other" })
            .pipe(Effect.flip)).code,
          "notFound",
        );
      }),
  );
});
