import { Context, Effect, Layer, Schema } from "effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { approvalDigest } from "./WorkflowApprovals.ts";
export class AttemptError extends Schema.TaggedError<AttemptError>()("AttemptError", {
  reason: Schema.Literals(["already-attempted", "storage"]),
}) {}
/** Claims survive restart. Ambiguous external mutations are never silently retried. */
export class WorkflowAttempts extends Context.Service<
  WorkflowAttempts,
  {
    readonly claim: (
      id: string,
      operation: string,
      review: string,
    ) => Effect.Effect<void, AttemptError>;
    readonly settle: (
      id: string,
      state: "completed" | "unknown",
    ) => Effect.Effect<void, AttemptError>;
  }
>()("t3/integrations/WorkflowAttempts") {}
export const layer = Layer.effect(
  WorkflowAttempts,
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    return WorkflowAttempts.of({
      claim: (id, operation, review) =>
        sql`INSERT INTO integration_attempts (operation_id, operation, review_digest, state) VALUES (${id}, ${operation}, ${approvalDigest(review)}, 'unknown') ON CONFLICT DO NOTHING RETURNING operation_id`.pipe(
          Effect.mapError(() => new AttemptError({ reason: "storage" })),
          Effect.flatMap((rows) =>
            rows.length === 1
              ? Effect.void
              : Effect.fail(new AttemptError({ reason: "already-attempted" })),
          ),
        ),
      settle: (id, state) =>
        sql`UPDATE integration_attempts SET state = ${state} WHERE operation_id = ${id}`.pipe(
          Effect.asVoid,
          Effect.mapError(() => new AttemptError({ reason: "storage" })),
        ),
    });
  }),
);
