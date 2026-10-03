import * as Schema from "effect/Schema";
import {
  CommandId,
  MessageId,
  NonNegativeInt,
  ThreadId,
  TrimmedNonEmptyString,
  TurnId,
} from "./baseSchemas.ts";
import { ModelSelection, ThreadUnattendedAuthority } from "./orchestration.ts";

const text = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));
const strings = (max: number, length: number) =>
  Schema.Array(text(length)).check(Schema.isMaxLength(max));
export const CoordinationId = text(120);
export const CoordinationPolicy = Schema.Struct({
  mode: Schema.Literals(["adHoc", "light", "deep"]),
  maxConcurrent: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 4 })),
  retainWorkers: Schema.Boolean,
});
export type CoordinationPolicy = typeof CoordinationPolicy.Type;
export const CoordinationArtifact = Schema.Struct({
  version: Schema.Literal(1),
  summary: text(4096),
  findings: strings(32, 512),
  evidence: Schema.Array(
    Schema.Struct({
      kind: Schema.Literals(["test", "source", "file", "checkpoint"]),
      reference: text(512),
      detail: text(512),
    }),
  ).check(Schema.isMaxLength(32)),
  validation: Schema.Struct({
    status: Schema.Literals(["notRun", "passed", "failed", "blocked"]),
    detail: text(2048),
  }),
  unchecked: strings(32, 512),
  confidence: Schema.Literals(["low", "medium", "high"]),
  outcome: Schema.Literals(["completed", "blocked"]),
  verdict: Schema.optional(Schema.Literals(["pass", "repair", "blocked"])),
});
export type CoordinationArtifact = typeof CoordinationArtifact.Type;
export const CoordinationNodeInput = Schema.Struct({
  id: CoordinationId,
  kind: Schema.Literals(["work", "critique", "verify"]),
  prompt: text(16_384),
  dependsOn: Schema.Array(CoordinationId).check(Schema.isMaxLength(32)),
  gateScope: Schema.Array(CoordinationId).check(Schema.isMaxLength(32)),
  attemptLimit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 5 })),
  modelSelection: Schema.suspend(() => ModelSelection),
});
export type CoordinationNodeInput = typeof CoordinationNodeInput.Type;
export const CoordinationAttempt = Schema.Struct({
  number: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 5 })),
  workerThreadId: ThreadId,
  dispatchMessageId: MessageId,
  turnId: Schema.NullOr(TurnId),
  status: Schema.Literals(["accepted", "succeeded", "failed", "interrupted", "superseded"]),
  dependencyVersions: Schema.Array(
    Schema.Struct({
      nodeId: CoordinationId,
      attemptNumber: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 5 })),
    }),
  ).check(Schema.isMaxLength(32)),
  artifact: Schema.NullOr(CoordinationArtifact),
  pendingArtifact: Schema.NullOr(CoordinationArtifact),
  salvage: Schema.optional(
    Schema.Struct({ actorThreadId: ThreadId, artifact: CoordinationArtifact }),
  ),
  handoffRequested: Schema.optional(Schema.Boolean),
  initialDispatchMessageId: Schema.optional(MessageId),
  failureCode: Schema.NullOr(text(120)),
});
export type CoordinationAttempt = typeof CoordinationAttempt.Type;
export const CoordinationNode = Schema.Struct({
  ...CoordinationNodeInput.fields,
  retired: Schema.Boolean,
  repairRound: NonNegativeInt.check(Schema.isLessThanOrEqualTo(5)),
  attempts: Schema.Array(CoordinationAttempt).check(Schema.isMaxLength(5)),
});
export type CoordinationNode = typeof CoordinationNode.Type;
export const CoordinationPlan = Schema.Struct({
  executionAuthority: Schema.optional(
    Schema.NullOr(Schema.suspend(() => ThreadUnattendedAuthority)),
  ),
  id: CoordinationId,
  rootThreadId: ThreadId,
  revision: NonNegativeInt,
  policy: CoordinationPolicy,
  paused: Schema.Boolean,
  cancelled: Schema.Boolean,
  nodes: Schema.Array(CoordinationNode).check(Schema.isNonEmpty(), Schema.isMaxLength(128)),
});
export type CoordinationPlan = typeof CoordinationPlan.Type;
const target = { callerThreadId: ThreadId, rootThreadId: ThreadId, planId: CoordinationId };
const mutation = { ...target, commandId: CommandId, expectedRevision: NonNegativeInt };
export const CoordinationReadInput = Schema.Struct(target);
export type CoordinationReadInput = typeof CoordinationReadInput.Type;
export const CoordinationWriteInput = Schema.Union([
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("create"),
    policy: CoordinationPolicy,
    nodes: Schema.Array(CoordinationNodeInput).check(Schema.isNonEmpty(), Schema.isMaxLength(128)),
  }),
  Schema.Struct({ ...mutation, operation: Schema.Literal("run") }),
  Schema.Struct({ ...mutation, operation: Schema.Literal("pause") }),
  Schema.Struct({ ...mutation, operation: Schema.Literal("cancel") }),
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("salvage"),
    nodeId: CoordinationId,
    artifact: CoordinationArtifact,
  }),
  Schema.Struct({ ...mutation, operation: Schema.Literal("retry"), nodeId: CoordinationId }),
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("repair"),
    nodeId: CoordinationId,
    successorGateId: CoordinationId,
    repairs: Schema.Array(CoordinationNodeInput).check(Schema.isNonEmpty(), Schema.isMaxLength(16)),
  }),
  Schema.Struct({
    ...mutation,
    operation: Schema.Literal("complete"),
    nodeId: CoordinationId,
    attemptNumber: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 5 })),
    workerThreadId: ThreadId,
    turnId: TurnId,
    artifact: CoordinationArtifact,
  }),
]);
export type CoordinationWriteInput = typeof CoordinationWriteInput.Type;
export const CoordinationEnvelope = Schema.Struct({
  id: CoordinationId,
  senderThreadId: ThreadId,
  recipientThreadId: ThreadId,
  channelId: Schema.NullOr(CoordinationId),
  text: text(8192),
  delivery: Schema.Literals(["pending", "read", "cancelled"]),
});
export const CoordinationMailbox = Schema.Struct({
  rootThreadId: ThreadId,
  revision: NonNegativeInt,
  envelopes: Schema.Array(CoordinationEnvelope).check(Schema.isMaxLength(1024)),
  channels: Schema.Array(
    Schema.Struct({
      id: CoordinationId,
      name: text(120),
      members: Schema.Array(ThreadId).check(Schema.isMaxLength(64)),
      closed: Schema.Boolean,
    }),
  ).check(Schema.isMaxLength(32)),
  context: Schema.Array(Schema.Struct({ key: CoordinationId, value: text(8192) })).check(
    Schema.isMaxLength(32),
  ),
});
export type CoordinationMailbox = typeof CoordinationMailbox.Type;
const mailboxTarget = { callerThreadId: ThreadId, rootThreadId: ThreadId };
const mailboxMutation = {
  ...mailboxTarget,
  commandId: CommandId,
  expectedRevision: NonNegativeInt,
};
export const CoordinationMailboxReadInput = Schema.Struct(mailboxTarget);
export type CoordinationMailboxReadInput = typeof CoordinationMailboxReadInput.Type;
export const CoordinationMailboxWriteInput = Schema.Union([
  Schema.Struct({ ...mailboxMutation, operation: Schema.Literal("pruneRead") }),
  Schema.Struct({
    ...mailboxMutation,
    operation: Schema.Literal("message"),
    recipientThreadIds: Schema.Array(ThreadId).check(Schema.isMaxLength(64)),
    channelId: Schema.NullOr(CoordinationId),
    text: text(8192),
  }),
  Schema.Struct({
    ...mailboxMutation,
    operation: Schema.Literal("ack"),
    envelopeIds: Schema.Array(CoordinationId).check(Schema.isNonEmpty(), Schema.isMaxLength(64)),
  }),
  Schema.Struct({
    ...mailboxMutation,
    operation: Schema.Literal("channelCreate"),
    channelId: CoordinationId,
    name: text(120),
    members: Schema.Array(ThreadId).check(Schema.isNonEmpty(), Schema.isMaxLength(64)),
  }),
  Schema.Struct({
    ...mailboxMutation,
    operation: Schema.Literal("channelMembership"),
    channelId: CoordinationId,
    memberThreadId: ThreadId,
    joined: Schema.Boolean,
  }),
  Schema.Struct({
    ...mailboxMutation,
    operation: Schema.Literal("channelClose"),
    channelId: CoordinationId,
    closed: Schema.Boolean,
  }),
  Schema.Struct({
    ...mailboxMutation,
    operation: Schema.Literal("contextWrite"),
    key: CoordinationId,
    value: Schema.NullOr(text(8192)),
  }),
]);
export type CoordinationMailboxWriteInput = typeof CoordinationMailboxWriteInput.Type;
export class CoordinationError extends Schema.TaggedError<CoordinationError>()(
  "CoordinationError",
  {
    code: Schema.Literals(["invalid", "forbidden", "conflict", "notFound", "busy", "exhausted"]),
    detail: text(2048),
  },
) {}
