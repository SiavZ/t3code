import {
  ProviderDoctorInput,
  providerDoctorApprovalReview,
  type ProviderDoctorApprovedInput,
  type ProviderDoctorResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";

const decodeInputSync = Schema.decodeUnknownSync(ProviderDoctorInput);
const decodeInputEffect = Schema.decodeUnknownEffect(ProviderDoctorInput);

/** A diagnostic request the human must review again before it can run. */
export class ProviderDiagnosticRequestError extends Schema.TaggedError<ProviderDiagnosticRequestError>()(
  "ProviderDiagnosticRequestError",
  { reason: Schema.Literals(["changed", "unconfirmed"]) },
) {
  override get message(): string {
    return this.reason === "changed"
      ? "Diagnostic request changed. Review and confirm a new run."
      : "Full diagnostics require human confirmation.";
  }
}

export function createProviderDiagnosticInput(
  instanceId: string,
  model: string,
  tier: ProviderDoctorInput["tier"],
  runId: string,
) {
  const selectedModel = model.trim();
  if (tier === "full" && !selectedModel)
    throw new Error("Choose a model before a full diagnostic.");
  return decodeInputSync({
    instanceId,
    runId,
    tier,
    ...(selectedModel ? { model: selectedModel } : {}),
  });
}

/** A confirmation is valid only for the immutable canonical request the human reviewed. */
export function runConfirmedProviderDiagnostic<R, GrantError, RunError>(
  raw: ProviderDoctorInput,
  confirmedReview: string,
  operations: {
    grant: (
      review: ReturnType<typeof providerDoctorApprovalReview>,
    ) => Effect.Effect<{ approvalId: string }, GrantError, R>;
    run: (request: ProviderDoctorApprovedInput) => Effect.Effect<ProviderDoctorResult, RunError, R>;
  },
) {
  return Effect.gen(function* () {
    const decoded = yield* decodeInputEffect(raw);
    const input = { ...decoded };
    const review = providerDoctorApprovalReview(input);
    if (input.tier !== "full" || !input.model || review.review !== confirmedReview)
      return yield* new ProviderDiagnosticRequestError({ reason: "changed" });
    const grant = yield* operations.grant(review);
    return yield* operations.run({ input, approvalId: grant.approvalId });
  });
}
