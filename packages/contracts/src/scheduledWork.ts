import * as Schema from "effect/Schema";
import { ThreadId } from "./baseSchemas.ts";
import { RuntimeMode, WorkerMcpCapability } from "./orchestration.ts";

export const UnattendedCeiling = Schema.Struct({
  runtimeMode: RuntimeMode,
  mcpCapabilities: Schema.Array(WorkerMcpCapability).check(Schema.isMaxLength(32)),
});
export type UnattendedCeiling = typeof UnattendedCeiling.Type;
export const ScheduledWorkListInput = Schema.Struct({ callerThreadId: ThreadId });
export type ScheduledWorkListInput = typeof ScheduledWorkListInput.Type;
export class ScheduledWorkError extends Schema.TaggedError<ScheduledWorkError>()(
  "ScheduledWorkError",
  {
    code: Schema.Literals(["invalid", "forbidden", "not-found", "conflict", "busy", "internal"]),
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}
