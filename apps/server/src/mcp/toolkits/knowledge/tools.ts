import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import {
  AgentSearchInput,
  AgentSearchResult,
  AgentSearchError,
} from "../../../../../../packages/contracts/src/agentSearch.ts";
import {
  HistorySearchInput,
  HistorySearchResult,
  HistoryReadInput,
  NormalizedHistorySession,
  HistorySearchError,
} from "../../../../../../packages/contracts/src/historySearch.ts";
import {
  SkillListInput,
  SkillListResult,
  SkillReadInput,
  SkillReadResult,
  SkillManagementError,
} from "../../../../../../packages/contracts/src/skillManagement.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspaceAgentSearch from "../../../workspace/WorkspaceAgentSearch.ts";
import * as HistorySearch from "../../../project/HistorySearch.ts";
import * as SkillManagement from "../../../provider/SkillManagement.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [
  McpInvocationContext.McpInvocationContext,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
  WorkspaceAgentSearch.WorkspaceAgentSearch,
  HistorySearch.HistorySearch,
  SkillManagement.SkillManagement,
];
const { projectId: _project, ...search } = AgentSearchInput.fields;
const {
  projectId: _historyProject,
  currentThreadId: _current,
  ...history
} = HistorySearchInput.fields;
const { projectId: _readProject, ...read } = HistoryReadInput.fields;
const { projectId: _listProject, ...skills } = SkillListInput.fields;
const { projectId: _skillProject, ...skill } = SkillReadInput.fields;
const searchFailure = Schema.Union([
  McpCapabilityUnavailableError,
  AgentSearchError,
  HistorySearchError,
]);
const historyFailure = Schema.Union([McpCapabilityUnavailableError, HistorySearchError]);
const skillFailure = Schema.Union([
  McpCapabilityUnavailableError,
  SkillManagementError,
  HistorySearchError,
]);
export const KnowledgeToolkit = Toolkit.make(
  Tool.make("knowledge_code_search", {
    description:
      "Search this project with indexed grep/find or TS/JS AST outline/trace. Trace requires file and returns syntax relationships, not resolved call targets.",
    parameters: Schema.Struct(search),
    success: AgentSearchResult,
    failure: searchFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("knowledge_history_search", {
    description:
      "Read-only bounded search of this project's T3 and configured external transcript stores. Does not import or resume.",
    parameters: Schema.Struct(history),
    success: HistorySearchResult,
    failure: historyFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("knowledge_history_read", {
    description: "Read bounded history returned by a project-scoped history search.",
    parameters: Schema.Struct(read),
    success: NormalizedHistorySession,
    failure: historyFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("knowledge_skills_list", {
    description: "List common provider skill registry inventory and workspace overlays.",
    parameters: Schema.Struct(skills),
    success: SkillListResult,
    failure: skillFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("knowledge_skills_read", {
    description: "Read an enabled registered skill by name.",
    parameters: Schema.Struct(skill),
    success: SkillReadResult,
    failure: skillFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("knowledge_skills_load", {
    description:
      "Load skill instructions for use in this turn without rewriting native directories.",
    parameters: Schema.Struct(skill),
    success: SkillReadResult,
    failure: skillFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
  Tool.make("knowledge_skills_reload", {
    description:
      "Refresh provider-native registry discovery for this project, not native process resume.",
    parameters: Schema.Struct(skills),
    success: SkillListResult,
    failure: skillFailure,
    dependencies,
  }).annotate(Tool.Readonly, true),
);
