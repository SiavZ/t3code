import * as Schema from "effect/Schema";
import { ThreadId, TrimmedNonEmptyString } from "./baseSchemas.ts";
const text = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));
export const QualityConfidence = Schema.Literals([
  "speculative",
  "plausible",
  "validated",
  "verified",
]);
export const QualityTodo = Schema.Struct({
  id: text(120),
  content: text(4096),
  group: Schema.NullOr(text(120)),
  status: Schema.Literals(["pending", "in_progress", "completed", "cancelled"]),
  priority: Schema.Literals(["low", "normal", "high"]),
  confidence: QualityConfidence,
  completionConfidence: Schema.optional(QualityConfidence),
});
export type QualityTodo = typeof QualityTodo.Type;
export const QualityGoal = Schema.Struct({
  group: Schema.NullOr(text(120)),
  autonomy: Schema.Literals([
    "requested_only",
    "necessary_followthrough",
    "proactive",
    "stewardship",
  ]),
  difficulty: Schema.Literals([
    "trivial",
    "routine",
    "involved",
    "complex",
    "hard",
    "expert",
    "research",
    "open_ended",
  ]),
  deliveryState: Schema.Literals([
    "change_made",
    "integrated",
    "workflow_validated",
    "outcome_delivered",
  ]),
  feedbackLoop: text(8192),
  closedFeedbackLoop: Schema.Literals(["absent", "weak", "usable", "strong", "closed"]),
  feedbackLoopCoverage: Schema.Literals(["narrow", "main_paths", "edge_and_integration_paths"]),
  feedbackLoopRelevance: Schema.Literals([
    "indirect",
    "synthetic",
    "representative",
    "acceptance_blocked",
    "acceptance_aligned",
  ]),
  feedbackLoopTraceability: Schema.Literals(["unmapped", "partial", "complete"]),
  iterationMaturity: Schema.Literals([
    "not_started",
    "exploring",
    "improving",
    "plateau_unproven",
    "outcome_reached",
    "constraints_exhausted",
    "plateau_confirmed",
    "budget_exhausted",
  ]),
  stoppingEvidence: Schema.NullOr(text(8192)),
});
export type QualityGoal = typeof QualityGoal.Type;
export const QualityUpdateInput = Schema.Struct({
  threadId: ThreadId,
  operationId: text(120),
  expectedRevision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  intention: Schema.optional(text(8192)),
  understanding: Schema.optional(Schema.Literals(["uncertain", "partial", "clear", "complete"])),
  todos: Schema.optional(Schema.Array(QualityTodo).check(Schema.isMaxLength(128))),
  goals: Schema.optional(Schema.Array(QualityGoal).check(Schema.isMaxLength(32))),
  removeGroups: Schema.optional(
    Schema.Array(Schema.NullOr(text(120))).check(Schema.isMaxLength(32)),
  ),
});
export type QualityUpdateInput = typeof QualityUpdateInput.Type;
export const QualityRecordChange = Schema.Struct({
  threadId: ThreadId,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type QualityRecordChange = typeof QualityRecordChange.Type;
export const QualityReadInput = Schema.Struct({ threadId: ThreadId });
export type QualityReadInput = typeof QualityReadInput.Type;
export const QualityConfidenceHistory = Schema.Struct({
  confidence: QualityConfidence,
  revision: Schema.Int,
  changedAt: Schema.String,
  source: Schema.Literals(["agent-reported", "user-reported"]),
});
export const QualityRecord = Schema.Struct({
  threadId: ThreadId,
  revision: Schema.Int,
  intention: Schema.NullOr(text(8192)),
  understanding: Schema.NullOr(Schema.Literals(["uncertain", "partial", "clear", "complete"])),
  todos: Schema.Array(
    Schema.Struct({
      ...QualityTodo.fields,
      confidenceHistory: Schema.Array(QualityConfidenceHistory).check(Schema.isMaxLength(64)),
    }),
  ).check(Schema.isMaxLength(128)),
  goals: Schema.Array(QualityGoal).check(Schema.isMaxLength(32)),
  source: Schema.Literals(["agent-reported", "user-reported"]),
  independentlyVerified: Schema.Literal(false),
  updatedAt: Schema.String,
});
export type QualityRecord = typeof QualityRecord.Type;
export class QualityRecordsError extends Schema.TaggedError<QualityRecordsError>()(
  "QualityRecordsError",
  {
    code: Schema.Literals(["invalid", "forbidden", "conflict", "notFound", "storage"]),
    detail: text(2048),
  },
) {}
