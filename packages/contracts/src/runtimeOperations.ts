import * as Schema from "effect/Schema";
import {
  ThreadId,
  MessageId,
  CommandId,
  IsoDateTime,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection } from "./orchestration.ts";
const text = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));
export const RuntimeTranscriptSeed = Schema.Struct({
  text: Schema.String.check(Schema.isMaxLength(65536)),
  sourceMessageIds: Schema.Array(MessageId).check(Schema.isMaxLength(256)),
  omittedMessages: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  omittedAttachments: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  hiddenStatePreserved: Schema.Literal(false),
});
export type RuntimeTranscriptSeed = typeof RuntimeTranscriptSeed.Type;
export const RuntimeHandoffInput = Schema.Struct({
  threadId: ThreadId,
  operationId: text(120),
  expectedUpdatedAt: IsoDateTime,
  targetModelSelection: ModelSelection,
});
export type RuntimeHandoffInput = typeof RuntimeHandoffInput.Type;
export const RuntimeHandoffCommand = Schema.Struct({
  type: Schema.Literal("thread.runtime.handoff"),
  commandId: CommandId,
  ...RuntimeHandoffInput.fields,
  epochId: text(120),
  seed: RuntimeTranscriptSeed,
  createdAt: IsoDateTime,
});
export type RuntimeHandoffCommand = typeof RuntimeHandoffCommand.Type;
export const RuntimeForkInput = Schema.Struct({
  sourceThreadId: ThreadId,
  operationId: text(120),
  expectedUpdatedAt: IsoDateTime,
  throughMessageId: MessageId,
  targetModelSelection: Schema.optional(ModelSelection),
  title: Schema.optional(text(512)),
});
export type RuntimeForkInput = typeof RuntimeForkInput.Type;
export const RuntimeOperationReceipt = Schema.Struct({
  operationId: text(120),
  threadId: ThreadId,
  kind: Schema.Literals(["handoff", "fork"]),
  status: Schema.Literals(["accepted", "completed", "failed", "cancelled"]),
  epochId: Schema.NullOr(text(120)),
  detail: Schema.String,
  createdAt: IsoDateTime,
});
export type RuntimeOperationReceipt = typeof RuntimeOperationReceipt.Type;
export const ProviderDoctorInput = Schema.Struct({
  instanceId: text(200),
  tier: Schema.Literals(["offline", "catalog", "full"]),
  runId: text(120),
  model: Schema.optional(text(200)),
});
export type ProviderDoctorInput = typeof ProviderDoctorInput.Type;
export const ProviderDoctorApprovedInput = Schema.Struct({
  input: ProviderDoctorInput,
  approvalId: text(200),
});
export type ProviderDoctorApprovedInput = typeof ProviderDoctorApprovedInput.Type;
export const providerDoctorApprovalReview = (input: ProviderDoctorInput) => ({
  operation: "provider.doctor.full",
  review: JSON.stringify({
    runId: input.runId,
    instanceId: input.instanceId,
    model: input.model ?? null,
    tier: input.tier,
    potentialCost: "quota-or-billing",
    workspace: "disposable",
    toolAuthority: "no approvals or arbitrary shell tools",
  }),
});
export const ProviderDoctorResult = Schema.Struct({
  instanceId: Schema.String,
  tier: Schema.Literals(["offline", "catalog", "full"]),
  runId: Schema.String,
  checkedAt: IsoDateTime,
  potentialCost: Schema.Literals(["none", "network-or-process", "quota-or-billing"]),
  stages: Schema.Array(
    Schema.Struct({
      name: Schema.String,
      status: Schema.Literals(["passed", "failed", "skipped", "unavailable"]),
      detail: Schema.String,
    }),
  ),
});
export type ProviderDoctorResult = typeof ProviderDoctorResult.Type;
export class RuntimeOperationError extends Schema.TaggedError<RuntimeOperationError>()(
  "RuntimeOperationError",
  {
    code: Schema.Literals([
      "invalid",
      "forbidden",
      "busy",
      "conflict",
      "notFound",
      "unavailable",
      "storage",
      "provider",
    ]),
    detail: Schema.String,
  },
) {}
