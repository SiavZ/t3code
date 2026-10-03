import * as Schema from "effect/Schema";
import {
  CommandId,
  IsoDateTime,
  MessageId,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection, RuntimeMode, WorkerMcpCapability } from "./orchestration.ts";

export const ScheduledWorkId = TrimmedNonEmptyString.check(Schema.isMaxLength(160));
export type ScheduledWorkId = typeof ScheduledWorkId.Type;
export const UnattendedCeiling = Schema.Struct({
  runtimeMode: RuntimeMode,
  mcpCapabilities: Schema.Array(WorkerMcpCapability).check(Schema.isMaxLength(32)),
});
export type UnattendedCeiling = typeof UnattendedCeiling.Type;
export const ScheduledWorkTarget = Schema.Union([
  Schema.Struct({ type: Schema.Literal("resume"), threadId: ThreadId }),
  Schema.Struct({
    type: Schema.Literal("spawn"),
    label: TrimmedNonEmptyString.check(Schema.isMaxLength(120)),
    modelSelection: ModelSelection,
  }),
  Schema.Struct({ type: Schema.Literal("ambient"), threadId: ThreadId }),
]);
export const ScheduledWorkCreateInput = Schema.Struct({
  id: ScheduledWorkId,
  callerThreadId: ThreadId,
  target: ScheduledWorkTarget,
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(64_000)),
  dueAt: Schema.optional(IsoDateTime),
  delayMs: Schema.optional(
    Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 31_536_000_000 })),
  ),
  latestStartAt: Schema.optional(IsoDateTime),
  onBusy: Schema.Literals(["wait", "fail"]),
  grantId: TrimmedNonEmptyString.check(Schema.isMaxLength(160)),
});
export type ScheduledWorkCreateInput = typeof ScheduledWorkCreateInput.Type;
export const ScheduledWorkReadInput = Schema.Struct({
  callerThreadId: ThreadId,
  id: ScheduledWorkId,
});
export type ScheduledWorkReadInput = typeof ScheduledWorkReadInput.Type;
export const ScheduledWorkListInput = Schema.Struct({ callerThreadId: ThreadId });
export type ScheduledWorkListInput = typeof ScheduledWorkListInput.Type;
export const ScheduledWorkState = Schema.Literals([
  "queued",
  "dispatching",
  "blocked",
  "accepted",
  "running",
  "stopping",
  "completed",
  "failed",
  "interrupted",
  "cancelled",
]);
export const ScheduledWorkRecord = Schema.Struct({
  id: ScheduledWorkId,
  ownerThreadId: ThreadId,
  projectId: ProjectId,
  target: ScheduledWorkTarget,
  prompt: Schema.String,
  dueAt: IsoDateTime,
  latestStartAt: Schema.NullOr(IsoDateTime),
  onBusy: Schema.Literals(["wait", "fail"]),
  grantId: Schema.String,
  grantRevision: Schema.Int,
  ceiling: UnattendedCeiling,
  commandId: CommandId,
  messageId: MessageId,
  state: ScheduledWorkState,
  reason: Schema.NullOr(Schema.String),
  cancelRequested: Schema.Boolean,
  acceptedSequence: Schema.NullOr(Schema.Int),
  executionThreadId: Schema.NullOr(ThreadId),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type ScheduledWorkRecord = typeof ScheduledWorkRecord.Type;
export class ScheduledWorkError extends Schema.TaggedError<ScheduledWorkError>()(
  "ScheduledWorkError",
  {
    code: Schema.Literals(["invalid", "forbidden", "not-found", "conflict", "busy", "internal"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export const AmbientWorkConfigureInput = Schema.Struct({
  callerThreadId: ThreadId,
  enabled: Schema.Boolean,
  grantId: Schema.String,
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(16_000)),
  idleDelayMs: Schema.Int.check(Schema.isBetween({ minimum: 60_000, maximum: 86_400_000 })),
  minimumCycleMs: Schema.Int.check(Schema.isBetween({ minimum: 60_000, maximum: 86_400_000 })),
  maxCyclesPerDay: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 24 })),
  timezone: Schema.String.check(Schema.isMaxLength(120)),
  allowedHourStart: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 23 })),
  allowedHourEnd: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 24 })),
  allowUnknownQuota: Schema.Boolean,
});
export type AmbientWorkConfigureInput = typeof AmbientWorkConfigureInput.Type;
export const AmbientWorkRecord = Schema.Struct({
  config: AmbientWorkConfigureInput,
  lastInteractionAt: Schema.Number,
  lastCycleAt: Schema.NullOr(Schema.Number),
  day: Schema.String,
  cycles: Schema.Int,
  activeScheduleId: Schema.NullOr(Schema.String),
  reason: Schema.NullOr(Schema.String),
});
export type AmbientWorkRecord = typeof AmbientWorkRecord.Type;
