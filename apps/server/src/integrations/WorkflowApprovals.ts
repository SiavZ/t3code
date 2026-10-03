import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeCrypto from "node:crypto";
const { createHash, randomUUID } = NodeCrypto;

export class ApprovalError extends Schema.TaggedError<ApprovalError>()("ApprovalError", {
  reason: Schema.Literals(["required", "expired", "mismatch", "storage"]),
}) {}
export const approvalDigest = (review: string) => createHash("sha256").update(review).digest("hex");

/** Human UI only. Root must not include grant in an agent-accessible toolkit. */
export class WorkflowApprovals extends Context.Service<
  WorkflowApprovals,
  {
    readonly grant: (input: {
      readonly humanSessionId: string;
      readonly operation: string;
      readonly review: string;
    }) => Effect.Effect<string, ApprovalError>;
    readonly consume: (
      id: string,
      operation: string,
      review: string,
    ) => Effect.Effect<void, ApprovalError>;
    readonly consumeForSession: (
      id: string,
      operation: string,
      review: string,
      humanSessionId: string,
    ) => Effect.Effect<void, ApprovalError>;
    readonly revokeSession: (humanSessionId: string) => Effect.Effect<void, ApprovalError>;
  }
>()("t3/integrations/WorkflowApprovals") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const storageError = () => new ApprovalError({ reason: "storage" });
  return WorkflowApprovals.of({
    grant: (input) =>
      Effect.gen(function* () {
        const id = randomUUID();
        yield* sql`DELETE FROM integration_approvals WHERE expires_at < ${Date.now()}`;
        yield* sql`INSERT INTO integration_approvals (approval_id, human_session_id, operation, review_digest, expires_at) VALUES (${id}, ${input.humanSessionId}, ${input.operation}, ${approvalDigest(input.review)}, ${Date.now() + 60_000})`;
        return id;
      }).pipe(Effect.mapError(storageError)),
    consume: (id, operation, review) =>
      Effect.gen(function* () {
        // Atomic delete-and-return prevents two clients consuming the same approval.
        const rows = yield* sql<{
          approval_id: string;
        }>`DELETE FROM integration_approvals WHERE approval_id = ${id} AND operation = ${operation} AND review_digest = ${approvalDigest(review)} AND expires_at >= ${Date.now()} RETURNING approval_id`.pipe(
          Effect.mapError(storageError),
        );
        if (rows.length !== 1) return yield* Effect.fail(new ApprovalError({ reason: "required" }));
      }),
    consumeForSession: (id, operation, review, humanSessionId) =>
      Effect.gen(function* () {
        const rows = yield* sql<{
          approval_id: string;
        }>`DELETE FROM integration_approvals WHERE approval_id = ${id} AND operation = ${operation} AND review_digest = ${approvalDigest(review)} AND human_session_id = ${humanSessionId} AND expires_at >= ${Date.now()} RETURNING approval_id`.pipe(
          Effect.mapError(storageError),
        );
        if (rows.length !== 1) return yield* Effect.fail(new ApprovalError({ reason: "required" }));
      }),
    revokeSession: (session) =>
      sql`DELETE FROM integration_approvals WHERE human_session_id = ${session}`.pipe(
        Effect.asVoid,
        Effect.mapError(storageError),
      ),
  });
});
export const layer = Layer.effect(WorkflowApprovals, make);
