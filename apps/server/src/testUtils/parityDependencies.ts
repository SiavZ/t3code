import * as Layer from "effect/Layer";
import * as AmbientWork from "../orchestration/AmbientWork.ts";
import * as BackgroundJobs from "../background/BackgroundJobs.ts";
import * as BackgroundJobAuthority from "../background/BackgroundJobAuthority.ts";
import * as ScheduledWork from "../orchestration/ScheduledWork.ts";
import * as ScheduledWorkActivation from "../orchestration/ScheduledWorkActivation.ts";
import * as UnattendedGrants from "../orchestration/UnattendedGrants.ts";
import * as OwnedWorkers from "../orchestration/OwnedWorkers.ts";
import * as CoordinationPlans from "../orchestration/CoordinationPlans.ts";
import * as CoordinationPlanStore from "../orchestration/CoordinationPlanStore.ts";
import * as CoordinationReactor from "../orchestration/CoordinationReactor.ts";
import * as WorkspaceAgentSearch from "../workspace/WorkspaceAgentSearch.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as HistorySearch from "../project/HistorySearch.ts";
import * as AgentSessionScanner from "../project/AgentSessionScanner.ts";
import * as SkillManagement from "../provider/SkillManagement.ts";
import * as Memory from "../memory/Memory.ts";
import * as GlobalMemory from "../memory/GlobalMemory.ts";
import * as QualityRecords from "../orchestration/QualityRecords.ts";
import * as AgentDocuments from "../orchestration/AgentDocuments.ts";
import * as DocumentLifecycle from "../orchestration/DocumentLifecycle.ts";
import * as AgentDocumentAssets from "../orchestration/AgentDocumentAssets.ts";
import * as ProcessRunner from "../processRunner.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";

// Supply the caller's isolated persistence, engine, query, settings and provider registry
// outside this graph so every domain service captures the same test dependencies.
export const parityStartupDependenciesLayer = Layer.mergeAll(
  AmbientWork.layer,
  BackgroundJobs.layer.pipe(Layer.provide(BackgroundJobAuthority.layer)),
  DocumentLifecycle.layer,
).pipe(
  Layer.provideMerge(AgentDocuments.layer),
  Layer.provideMerge(ScheduledWork.layer),
  Layer.provideMerge(ScheduledWorkActivation.layer),
  Layer.provideMerge(UnattendedGrants.layer),
  Layer.provideMerge(OwnedWorkers.layer),
  Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
  Layer.provide(WorkspacePaths.layer),
  Layer.provide(ProcessRunner.layer),
);

export const parityDependenciesLayer = parityStartupDependenciesLayer.pipe(
  Layer.provideMerge(GlobalMemory.layer),
  Layer.provideMerge(
    Layer.mergeAll(
      CoordinationPlans.layer.pipe(Layer.provide(CoordinationPlanStore.layer)),
      CoordinationReactor.layer,
      WorkspaceAgentSearch.layer,
      HistorySearch.layer.pipe(Layer.provide(AgentSessionScanner.layer)),
      SkillManagement.layer,
      Memory.layer,
      QualityRecords.layer,
      AgentDocumentAssets.layer,
    ),
  ),
  Layer.provide(WorkspacePaths.layer),
);
