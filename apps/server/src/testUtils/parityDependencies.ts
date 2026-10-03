import * as Layer from "effect/Layer";
import * as UnattendedGrants from "../orchestration/UnattendedGrants.ts";
import * as OwnedWorkers from "../orchestration/OwnedWorkers.ts";
import * as CoordinationPlans from "../orchestration/CoordinationPlans.ts";
import * as CoordinationPlanStore from "../orchestration/CoordinationPlanStore.ts";
import * as CoordinationReactor from "../orchestration/CoordinationReactor.ts";
import * as WorkspacePaths from "../workspace/WorkspacePaths.ts";
import * as ProcessRunner from "../processRunner.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";

// Supply the caller's isolated persistence, engine, query, settings and provider registry
// outside this graph so every domain service captures the same test dependencies.
export const parityDependenciesLayer = UnattendedGrants.layer.pipe(
  Layer.provideMerge(OwnedWorkers.layer),
  Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
  Layer.provideMerge(
    Layer.mergeAll(
      CoordinationPlans.layer.pipe(Layer.provide(CoordinationPlanStore.layer)),
      CoordinationReactor.layer,
    ),
  ),
  Layer.provide(WorkspacePaths.layer),
  Layer.provide(ProcessRunner.layer),
);
