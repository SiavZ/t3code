import * as Schema from "effect/Schema";
import { IsoDateTime, ProjectId, ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";

export const BackgroundJobId = TrimmedNonEmptyString.check(Schema.isMaxLength(160));
export const BackgroundJobStartInput = Schema.Struct({
  id: BackgroundJobId,
  callerThreadId: ThreadId,
  command: TrimmedNonEmptyString.check(Schema.isMaxLength(4096)),
  args: Schema.Array(Schema.String.check(Schema.isMaxLength(8192))).check(Schema.isMaxLength(128)),
  timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 86_400_000 })),
  maxOutputBytes: Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 1_048_576 })),
});
export type BackgroundJobStartInput = typeof BackgroundJobStartInput.Type;
export const BackgroundJobReadInput = Schema.Struct({
  callerThreadId: ThreadId,
  id: BackgroundJobId,
});
export type BackgroundJobReadInput = typeof BackgroundJobReadInput.Type;
export const BackgroundJobOutputInput = Schema.Struct({
  ...BackgroundJobReadInput.fields,
  cursor: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  limitBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
});
export type BackgroundJobOutputInput = typeof BackgroundJobOutputInput.Type;
export const BackgroundJobWaitInput = Schema.Struct({
  ...BackgroundJobReadInput.fields,
  timeoutMs: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 600_000 })),
});
export type BackgroundJobWaitInput = typeof BackgroundJobWaitInput.Type;
export const BackgroundJobState = Schema.Literals([
  "pending",
  "running",
  "cancelling",
  "completed",
  "failed",
  "cancelled",
  "interrupted",
]);
export const BackgroundJobRecord = Schema.Struct({
  id: BackgroundJobId,
  ownerThreadId: ThreadId,
  projectId: ProjectId,
  command: Schema.String,
  args: Schema.Array(Schema.String),
  cwd: Schema.String,
  state: BackgroundJobState,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
  exitCode: Schema.NullOr(Schema.Int),
  reason: Schema.NullOr(Schema.String),
  maxOutputBytes: Schema.Int,
  outputBytes: Schema.Int,
  truncated: Schema.Boolean,
  progress: Schema.NullOr(
    Schema.Struct({
      value: Schema.Number.check(Schema.isBetween({ minimum: 0, maximum: 1 })),
      label: Schema.String.check(Schema.isMaxLength(500)),
    }),
  ),
});
export type BackgroundJobRecord = typeof BackgroundJobRecord.Type;
export const BackgroundJobOutput = Schema.Struct({
  chunks: Schema.Array(
    Schema.Struct({
      cursor: Schema.Int,
      stream: Schema.Literals(["stdout", "stderr"]),
      text: Schema.String,
    }),
  ),
  nextCursor: Schema.Int,
  truncated: Schema.Boolean,
});
export type BackgroundJobOutput = typeof BackgroundJobOutput.Type;
export class BackgroundJobError extends Schema.TaggedError<BackgroundJobError>()(
  "BackgroundJobError",
  {
    code: Schema.Literals(["invalid", "forbidden", "not-found", "conflict", "running", "internal"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
