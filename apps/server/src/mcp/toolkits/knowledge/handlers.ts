import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { HistorySearchError } from "../../../../../../packages/contracts/src/historySearch.ts";
import * as WorkspaceAgentSearch from "../../../workspace/WorkspaceAgentSearch.ts";
import * as HistorySearch from "../../../project/HistorySearch.ts";
import * as SkillManagement from "../../../provider/SkillManagement.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { KnowledgeToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const search = yield* WorkspaceAgentSearch.WorkspaceAgentSearch;
  const history = yield* HistorySearch.HistorySearch;
  const skills = yield* SkillManagement.SkillManagement;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const projectId = Effect.fn(function* () {
    const scope = yield* McpInvocationContext.requireMcpCapability("knowledge");
    const thread = yield* snapshots
      .getThreadShellById(scope.threadId)
      .pipe(Effect.mapError(() => new HistorySearchError({ reason: "read-failed" })));
    if (Option.isNone(thread))
      return yield* new HistorySearchError({ reason: "project-not-found" });
    return thread.value.projectId;
  });
  return KnowledgeToolkit.of({
    knowledge_code_search: Effect.fn(function* (input) {
      return yield* search.search({ ...input, projectId: yield* projectId() });
    }),
    knowledge_history_search: Effect.fn(function* (input) {
      const scope = yield* McpInvocationContext.requireMcpCapability("knowledge");
      return yield* history.search({
        ...input,
        projectId: yield* projectId(),
        currentThreadId: scope.threadId,
      });
    }),
    knowledge_history_read: Effect.fn(function* (input) {
      return yield* history.readHistory({ ...input, projectId: yield* projectId() });
    }),
    knowledge_skills_list: Effect.fn(function* (input) {
      return yield* skills.list({ ...input, projectId: yield* projectId() });
    }),
    knowledge_skills_read: Effect.fn(function* (input) {
      return yield* skills.read({ ...input, projectId: yield* projectId() });
    }),
    knowledge_skills_load: Effect.fn(function* (input) {
      return yield* skills.load({ ...input, projectId: yield* projectId() });
    }),
    knowledge_skills_reload: Effect.fn(function* (input) {
      return yield* skills.reload({ ...input, projectId: yield* projectId() });
    }),
  });
});
export const KnowledgeToolkitHandlersLive = KnowledgeToolkit.toLayer(make);
