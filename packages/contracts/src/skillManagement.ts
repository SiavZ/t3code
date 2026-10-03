import * as Schema from "effect/Schema";
import { ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ServerProviderSkill } from "./server.ts";

export const SkillListInput = Schema.Struct({
  projectId: ProjectId,
  instanceId: ProviderInstanceId,
  fresh: Schema.optional(Schema.Boolean),
});
export type SkillListInput = typeof SkillListInput.Type;
export const SkillReadInput = Schema.Struct({
  projectId: ProjectId,
  instanceId: ProviderInstanceId,
  name: TrimmedNonEmptyString,
});
export type SkillReadInput = typeof SkillReadInput.Type;
export const SkillListResult = Schema.Struct({ skills: Schema.Array(ServerProviderSkill) });
export type SkillListResult = typeof SkillListResult.Type;
export const SkillReadResult = Schema.Struct({
  skill: ServerProviderSkill,
  content: Schema.String,
});
export type SkillReadResult = typeof SkillReadResult.Type;
export class SkillManagementError extends Schema.TaggedError<SkillManagementError>()(
  "SkillManagementError",
  {
    reason: Schema.Literals([
      "project-not-found",
      "provider-not-found",
      "skill-not-found",
      "skill-disabled",
      "skill-unreadable",
      "limit-exceeded",
    ]),
  },
) {
  override get message() {
    return `Skill management failed: ${this.reason}.`;
  }
}
