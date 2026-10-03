import * as Schema from "effect/Schema";

import {
  CommandId,
  IsoDateTime,
  MessageId,
  NonNegativeInt,
  PositiveInt,
  ThreadId,
  TrimmedNonEmptyString,
  TurnId,
} from "./baseSchemas.ts";
import {
  ModelSelection,
  OrchestrationThreadDetailSnapshot,
  PROVIDER_SEND_TURN_MAX_INPUT_CHARS,
  RuntimeMode,
  ThreadWorkerMetadata,
} from "./orchestration.ts";

export const WORKER_MAX_DEPTH = 2;
export const WORKER_MAX_LIVE_PER_ROOT = 4;
export const WORKER_MAX_LIVE_PER_ENVIRONMENT = 16;

export const WorkerTargetInput = Schema.Struct({
  callerThreadId: ThreadId,
  workerThreadId: ThreadId,
});
export type WorkerTargetInput = typeof WorkerTargetInput.Type;

export const WorkerSpawnInput = Schema.Struct({
  commandId: CommandId,
  callerThreadId: ThreadId,
  label: ThreadWorkerMetadata.fields.label,
  prompt: TrimmedNonEmptyString.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
  modelSelection: ModelSelection,
});
export type WorkerSpawnInput = typeof WorkerSpawnInput.Type;

export const WorkerListInput = Schema.Struct({ callerThreadId: ThreadId });
export type WorkerListInput = typeof WorkerListInput.Type;

export const WorkerGetInput = Schema.Struct({
  ...WorkerTargetInput.fields,
  turnLimit: Schema.optional(PositiveInt.check(Schema.isLessThanOrEqualTo(20))),
});
export type WorkerGetInput = typeof WorkerGetInput.Type;

export const WorkerSendInput = Schema.Struct({
  ...WorkerTargetInput.fields,
  commandId: CommandId,
  text: TrimmedNonEmptyString.check(Schema.isMaxLength(PROVIDER_SEND_TURN_MAX_INPUT_CHARS)),
});
export type WorkerSendInput = typeof WorkerSendInput.Type;

export const WorkerStopInput = Schema.Struct({
  ...WorkerTargetInput.fields,
  commandId: CommandId,
});
export type WorkerStopInput = typeof WorkerStopInput.Type;

export const WorkerWaitInput = Schema.Struct({
  callerThreadId: ThreadId,
  workerThreadIds: Schema.Array(ThreadId).check(Schema.isNonEmpty(), Schema.isMaxLength(16)),
  mode: Schema.Literals(["any", "all"]),
  timeoutMs: PositiveInt.check(Schema.isLessThanOrEqualTo(600_000)),
});
export type WorkerWaitInput = typeof WorkerWaitInput.Type;

export const WorkerStatus = Schema.Literals([
  "pending",
  "running",
  "waiting",
  "idle",
  "completed",
  "failed",
  "interrupted",
  "stopping",
  "stopped",
]);
export type WorkerStatus = typeof WorkerStatus.Type;

export const WorkerSummary = Schema.Struct({
  threadId: ThreadId,
  ownerThreadId: ThreadId,
  rootThreadId: ThreadId,
  label: ThreadWorkerMetadata.fields.label,
  depth: ThreadWorkerMetadata.fields.depth,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  status: WorkerStatus,
  pendingMessageId: Schema.NullOr(MessageId),
  turnId: Schema.NullOr(TurnId),
  result: Schema.NullOr(
    Schema.Struct({
      assistantMessageId: MessageId,
      turnId: TurnId,
      completedAt: IsoDateTime,
    }),
  ),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type WorkerSummary = typeof WorkerSummary.Type;

export const WorkerOperationResult = Schema.Struct({
  workerThreadId: ThreadId,
  sequence: NonNegativeInt,
});
export type WorkerOperationResult = typeof WorkerOperationResult.Type;

export const WorkerListResult = Schema.Struct({
  workers: Schema.Array(WorkerSummary),
  truncated: Schema.Boolean,
});
export type WorkerListResult = typeof WorkerListResult.Type;

export const WorkerGetResult = Schema.Struct({
  worker: WorkerSummary,
  detail: OrchestrationThreadDetailSnapshot,
});
export type WorkerGetResult = typeof WorkerGetResult.Type;

export const WorkerWaitResult = Schema.Struct({
  timedOut: Schema.Boolean,
  workers: Schema.Array(WorkerSummary),
});
export type WorkerWaitResult = typeof WorkerWaitResult.Type;

export class WorkerOperationError extends Schema.TaggedError<WorkerOperationError>()(
  "WorkerOperationError",
  {
    operation: Schema.Literals(["spawn", "list", "get", "send", "stop", "wait"]),
    code: Schema.Literals([
      "invalid-input",
      "not-found",
      "forbidden",
      "busy",
      "limit",
      "conflict",
      "provider",
      "internal",
    ]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}
