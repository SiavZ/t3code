import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as NodePath from "node:path";
import type ts from "typescript";
import {
  AgentSearchError,
  type AgentSearchInput,
  type AgentSearchResult,
  type AgentSearchSymbol,
} from "../../../../packages/contracts/src/agentSearch.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspaceSearchIndex from "./WorkspaceSearchIndex.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";

export class WorkspaceAgentSearch extends Context.Service<
  WorkspaceAgentSearch,
  {
    readonly search: (
      input: AgentSearchInput,
    ) => Effect.Effect<AgentSearchResult, AgentSearchError>;
  }
>()("t3/workspace/WorkspaceAgentSearch") {}

/** Syntax-only relationships, not type-resolved call targets. */
export async function structuralSearch(
  file: string,
  contents: string,
  input: AgentSearchInput,
): Promise<AgentSearchResult> {
  const { default: ts } = await import("typescript");
  const source = ts.createSourceFile(file, contents, ts.ScriptTarget.Latest, true);
  const symbols: AgentSearchSymbol[] = [];
  let visited = 0;
  let truncated = false;
  const limit = Math.min(input.limit ?? 50, 200);
  const record = (node: ts.Node, name: string, relation?: string, target?: string) => {
    if (input.query && !name.includes(input.query) && !target?.includes(input.query)) return;
    if (symbols.length >= limit) {
      truncated = true;
      return;
    }
    const start = source.getLineAndCharacterOfPosition(node.getStart(source));
    const end = source.getLineAndCharacterOfPosition(node.end);
    symbols.push({
      path: file,
      name,
      kind: ts.SyntaxKind[node.kind]!,
      line: start.line + 1,
      column: start.character + 1,
      endLine: end.line + 1,
      ...(relation ? { relation } : {}),
      ...(target ? { target } : {}),
    });
  };
  const visit = (node: ts.Node, depth: number) => {
    if (++visited > 100000 || depth > 128) {
      truncated = true;
      return;
    }
    if (input.mode === "outline" || input.relation === "declared") {
      if (
        (ts.isFunctionDeclaration(node) ||
          ts.isClassDeclaration(node) ||
          ts.isInterfaceDeclaration(node) ||
          ts.isTypeAliasDeclaration(node) ||
          ts.isEnumDeclaration(node) ||
          ts.isMethodDeclaration(node) ||
          ts.isVariableDeclaration(node)) &&
        node.name
      )
        record(node, node.name.getText(source), "declared");
    } else if (input.relation === "imports") {
      if (ts.isImportDeclaration(node))
        record(
          node,
          node.importClause?.getText(source) ?? "side-effect",
          "imports",
          node.moduleSpecifier.getText(source),
        );
    } else if (input.relation === "calls") {
      if (ts.isCallExpression(node) || ts.isNewExpression(node))
        record(node, node.expression.getText(source), "calls", node.expression.getText(source));
    } else if (ts.isIdentifier(node)) record(node, node.text, "references");
    ts.forEachChild(node, (child) => visit(child, depth + 1));
  };
  visit(source, 0);
  return {
    mode: input.mode,
    paths: [],
    matches: [],
    symbols,
    truncated,
    parser: `typescript/${ts.version}`,
    warnings:
      input.mode === "trace"
        ? [
            "Relationships are syntax-only. References include declaration identifiers and calls are not type-resolved.",
          ]
        : [],
  };
}

const make = Effect.gen(function* () {
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const indexes = yield* WorkspaceSearchIndex.WorkspaceSearchIndexMap;
  const paths = yield* WorkspacePaths.WorkspacePaths;
  const fs = yield* FileSystem.FileSystem;
  const search = Effect.fn("WorkspaceAgentSearch.search")(function* (input: AgentSearchInput) {
    const limit = Math.min(input.limit ?? 50, 200);
    const project = yield* snapshots
      .getProjectShellById(input.projectId)
      .pipe(Effect.mapError(() => new AgentSearchError({ reason: "project-not-found" })));
    if (Option.isNone(project)) return yield* new AgentSearchError({ reason: "project-not-found" });
    const cwd = project.value.workspaceRoot;
    const filter = (file: string) =>
      (!input.path ||
        file === input.path ||
        file.startsWith(`${input.path.replace(/\/$/, "")}/`)) &&
      (!input.glob || NodePath.matchesGlob(file, input.glob));
    if (input.path)
      yield* paths
        .resolveRelativePathWithinRoot({ workspaceRoot: cwd, relativePath: input.path })
        .pipe(Effect.mapError(() => new AgentSearchError({ reason: "outside-workspace" })));
    if (input.mode === "grep" || input.mode === "find") {
      if (input.mode === "grep" && !input.query)
        return yield* new AgentSearchError({ reason: "invalid-input" });
      const variant = input.mode === "grep" ? "content" : "paths";
      return yield* Effect.gen(function* () {
        const index = yield* WorkspaceSearchIndex.WorkspaceSearchIndex;
        if (input.mode === "find") {
          const found = yield* index.search(input.query ?? "", 200);
          const matches = found.entries.filter((entry) => filter(entry.path));
          return {
            mode: input.mode,
            paths: matches.slice(0, limit).map((entry) => entry.path),
            matches: [],
            symbols: [],
            truncated: found.truncated || matches.length > limit,
            warnings: [],
            parser: null,
          };
        }
        const found = yield* index.searchContents({
          query: input.query!,
          limit: 200,
          useRegex: input.useRegex ?? false,
          caseSensitive: input.caseSensitive ?? false,
          wholeWord: false,
        });
        if (found.regexFallbackError)
          return yield* new AgentSearchError({ reason: "invalid-regex" });
        const matches = found.matches.filter((match) => filter(match.path));
        return {
          mode: input.mode,
          paths: [],
          matches: matches.slice(0, limit),
          symbols: [],
          truncated: found.truncated || matches.length > limit,
          warnings: [],
          parser: null,
        };
      }).pipe(
        Effect.provide(indexes.get(WorkspaceSearchIndex.workspaceSearchIndexKey(cwd, variant))),
        Effect.mapError((error) =>
          error instanceof AgentSearchError
            ? error
            : new AgentSearchError({ reason: "search-failed" }),
        ),
      );
    }
    if (!input.file) return yield* new AgentSearchError({ reason: "invalid-input" });
    if (!/\.(?:[cm]?[jt]sx?)$/i.test(input.file))
      return yield* new AgentSearchError({ reason: "unsupported-language" });
    const resolved = yield* paths
      .resolveRelativePathWithinRoot({ workspaceRoot: cwd, relativePath: input.file })
      .pipe(Effect.mapError(() => new AgentSearchError({ reason: "outside-workspace" })));
    const actual = yield* fs
      .realPath(resolved.absolutePath)
      .pipe(Effect.mapError(() => new AgentSearchError({ reason: "read-failed" })));
    const actualRoot = yield* fs
      .realPath(cwd)
      .pipe(Effect.mapError(() => new AgentSearchError({ reason: "read-failed" })));
    if (
      NodePath.relative(actualRoot, actual).startsWith("..") ||
      NodePath.isAbsolute(NodePath.relative(actualRoot, actual))
    )
      return yield* new AgentSearchError({ reason: "outside-workspace" });
    const stat = yield* fs
      .stat(actual)
      .pipe(Effect.mapError(() => new AgentSearchError({ reason: "read-failed" })));
    if (stat.type !== "File" || stat.size > 1048576n)
      return yield* new AgentSearchError({ reason: "limit-exceeded" });
    const contents = yield* fs
      .readFileString(actual)
      .pipe(Effect.mapError(() => new AgentSearchError({ reason: "read-failed" })));
    if (Buffer.byteLength(contents) > 1048576)
      return yield* new AgentSearchError({ reason: "limit-exceeded" });
    return yield* Effect.tryPromise({
      try: () => structuralSearch(input.file!, contents, input),
      catch: () => new AgentSearchError({ reason: "read-failed" }),
    });
  });
  return WorkspaceAgentSearch.of({ search });
});
export const layer = Layer.effect(WorkspaceAgentSearch, make).pipe(
  Layer.provide(WorkspaceSearchIndex.WorkspaceSearchIndexMap.layer),
);
