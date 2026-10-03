import * as Schema from "effect/Schema";
import { IsoDateTime, ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
import { UnattendedCeiling } from "./scheduledWork.ts";

export const UnattendedGrantCreateInput = Schema.Struct({
  id: TrimmedNonEmptyString.check(Schema.isMaxLength(160)),
  callerThreadId: ThreadId,
  ceiling: UnattendedCeiling,
  hostJobs: Schema.Boolean,
});
export type UnattendedGrantCreateInput = typeof UnattendedGrantCreateInput.Type;
export const UnattendedGrantReadInput = Schema.Struct({
  callerThreadId: ThreadId,
  id: Schema.String,
});
export type UnattendedGrantReadInput = typeof UnattendedGrantReadInput.Type;
export const UnattendedGrant = Schema.Struct({
  id: Schema.String,
  ownerThreadId: ThreadId,
  projectId: ProjectId,
  revision: Schema.Int,
  revoked: Schema.Boolean,
  ceiling: UnattendedCeiling,
  hostJobs: Schema.Boolean,
  createdAt: IsoDateTime,
});
export type UnattendedGrant = typeof UnattendedGrant.Type;
