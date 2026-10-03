import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import {
  SkillManagementError,
  type SkillListInput,
  type SkillListResult,
  type SkillReadInput,
  type SkillReadResult,
} from "../../../../packages/contracts/src/skillManagement.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderRegistry from "./Services/ProviderRegistry.ts";

export class SkillManagement extends Context.Service<
  SkillManagement,
  {
    readonly list: (input: SkillListInput) => Effect.Effect<SkillListResult, SkillManagementError>;
    readonly read: (input: SkillReadInput) => Effect.Effect<SkillReadResult, SkillManagementError>;
    readonly load: (input: SkillReadInput) => Effect.Effect<SkillReadResult, SkillManagementError>;
    readonly reload: (
      input: SkillListInput,
    ) => Effect.Effect<SkillListResult, SkillManagementError>;
  }
>()("t3/provider/SkillManagement") {}

const make = Effect.gen(function* () {
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const fs = yield* FileSystem.FileSystem;
  const list = Effect.fn("SkillManagement.list")(function* (input: SkillListInput) {
    const project = yield* snapshots
      .getProjectShellById(input.projectId)
      .pipe(Effect.mapError(() => new SkillManagementError({ reason: "project-not-found" })));
    if (Option.isNone(project))
      return yield* new SkillManagementError({ reason: "project-not-found" });
    const providers = yield* registry.refreshWorkspaceSnapshot({
      instanceId: input.instanceId,
      cwd: project.value.workspaceRoot,
      fresh: input.fresh ?? false,
    });
    const provider = providers.find((item) => item.instanceId === input.instanceId);
    if (!provider) return yield* new SkillManagementError({ reason: "provider-not-found" });
    const workspace = provider.workspaceSnapshots?.find(
      (item) => item.cwd === project.value.workspaceRoot,
    );
    const skills = new Map(provider.skills.map((skill) => [skill.name, skill]));
    for (const skill of workspace?.skills ?? []) skills.set(skill.name, skill);
    return { skills: [...skills.values()] };
  });
  const read = Effect.fn("SkillManagement.read")(function* (input: SkillReadInput) {
    const inventory = yield* list(input);
    const skill = inventory.skills.find((item) => item.name === input.name);
    if (!skill) return yield* new SkillManagementError({ reason: "skill-not-found" });
    if (!skill.enabled) return yield* new SkillManagementError({ reason: "skill-disabled" });
    const stat = yield* fs
      .stat(skill.path)
      .pipe(Effect.mapError(() => new SkillManagementError({ reason: "skill-unreadable" })));
    if (stat.type !== "File")
      return yield* new SkillManagementError({ reason: "skill-unreadable" });
    if (stat.size > 262144n) return yield* new SkillManagementError({ reason: "limit-exceeded" });
    const content = yield* fs
      .readFileString(skill.path)
      .pipe(Effect.mapError(() => new SkillManagementError({ reason: "skill-unreadable" })));
    if (Buffer.byteLength(content) > 262144)
      return yield* new SkillManagementError({ reason: "limit-exceeded" });
    return { skill, content };
  });
  return SkillManagement.of({
    list,
    read,
    load: read,
    reload: (input) => list({ ...input, fresh: true }),
  });
});
export const layer = Layer.effect(SkillManagement, make);
