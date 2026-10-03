import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import { ProjectId } from "@t3tools/contracts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";
import * as WorkspaceAgentSearch from "./WorkspaceAgentSearch.ts";

it.layer(NodeServices.layer)("WorkspaceAgentSearch", (it) => {
  it.effect("uses real index search and AST syntax, not comments or string lookalikes", () =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const cwd = yield* fs.makeTempDirectoryScoped();
      yield* fs.writeFileString(
        `${cwd}/fixture.ts`,
        `import { readFile } from "node:fs";\n// function fake() {}\nconst lookalike = "function stringFake() {}";\nfunction outer() { function inner() {} return readFile("file", () => inner()); }`,
      );
      const snapshots = Layer.succeed(ProjectionSnapshotQuery.ProjectionSnapshotQuery, {
        getProjectShellById: () => Effect.succeed(Option.some({ workspaceRoot: cwd })),
      } as unknown as ProjectionSnapshotQuery.ProjectionSnapshotQueryShape);
      yield* Effect.gen(function* () {
        const service = yield* WorkspaceAgentSearch.WorkspaceAgentSearch;
        const projectId = ProjectId.make("fixture");
        const outline = yield* service.search({ projectId, mode: "outline", file: "fixture.ts" });
        expect(outline.symbols.map((symbol) => symbol.name)).toEqual([
          "lookalike",
          "outer",
          "inner",
        ]);
        expect(outline.parser).toBe("typescript/6.0.3");
        const calls = yield* service.search({
          projectId,
          mode: "trace",
          file: "fixture.ts",
          relation: "calls",
        });
        expect(calls.symbols.map((symbol) => symbol.name)).toEqual(["readFile", "inner"]);
        const grep = yield* service.search({
          projectId,
          mode: "grep",
          query: "function outer",
          glob: "*.ts",
        });
        expect(grep.matches[0]?.path).toBe("fixture.ts");
        const find = yield* service.search({ projectId, mode: "find", query: "fixture" });
        expect(find.paths).toContain("fixture.ts");
        const unsupported = yield* service
          .search({ projectId, mode: "outline", file: "fixture.py" })
          .pipe(Effect.flip);
        expect(unsupported.reason).toBe("unsupported-language");
        const escaped = yield* service
          .search({ projectId, mode: "outline", file: "../outside.ts" })
          .pipe(Effect.flip);
        expect(escaped.reason).toBe("outside-workspace");
      }).pipe(
        Effect.provide(
          WorkspaceAgentSearch.layer.pipe(
            Layer.provide(snapshots),
            Layer.provide(WorkspacePaths.layer),
          ),
        ),
      );
    }),
  );
});
