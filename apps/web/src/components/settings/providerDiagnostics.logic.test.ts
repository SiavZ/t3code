import {
  providerDoctorApprovalReview,
  type ProviderDoctorApprovedInput,
  type ProviderDoctorInput,
  type ProviderDoctorResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { describe, expect, it } from "@effect/vitest";
import {
  createProviderDiagnosticInput,
  ProviderDiagnosticRequestError,
  runConfirmedProviderDiagnostic,
} from "./providerDiagnostics.logic";

const result: ProviderDoctorResult = {
  instanceId: "provider",
  tier: "full",
  runId: "run",
  checkedAt: "2026-10-03T00:00:00Z",
  potentialCost: "quota-or-billing",
  stages: [],
};
function operations() {
  const grants: ReturnType<typeof providerDoctorApprovalReview>[] = [];
  const runs: ProviderDoctorApprovedInput[] = [];
  return {
    grants,
    runs,
    grant: (review: ReturnType<typeof providerDoctorApprovalReview>) =>
      Effect.sync(() => {
        grants.push(review);
        return { approvalId: `approval-${grants.length}` };
      }),
    run: (request: ProviderDoctorApprovedInput) =>
      Effect.sync(() => {
        runs.push(request);
        return result;
      }),
  };
}

describe("human-approved provider diagnostics", () => {
  it("requires a model for full diagnostics and bounds the actual request", () => {
    expect(() => createProviderDiagnosticInput("provider", "  ", "full", "run")).toThrow(
      "Choose a model",
    );
    expect(() =>
      createProviderDiagnosticInput("provider", "x".repeat(201), "full", "run"),
    ).toThrow();
    expect(createProviderDiagnosticInput("provider", " model ", "full", "run").model).toBe("model");
    expect(createProviderDiagnosticInput("provider", "", "offline", "run").model).toBeUndefined();
  });
  it.effect("grants the exact canonical review before the approved run", () =>
    Effect.gen(function* () {
      const ops = operations();
      const input = createProviderDiagnosticInput("provider", "model", "full", "run");
      const canonical = providerDoctorApprovalReview(input);
      yield* runConfirmedProviderDiagnostic(input, canonical.review, ops);
      expect(ops.grants).toEqual([canonical]);
      expect(ops.runs).toEqual([{ input, approvalId: "approval-1" }]);
      // @effect-diagnostics-next-line preferSchemaOverJson:off - asserts the exact reviewed string.
      expect(JSON.parse(ops.grants[0]!.review)).toMatchObject({
        runId: "run",
        instanceId: "provider",
        model: "model",
        tier: "full",
        potentialCost: "quota-or-billing",
      });
    }),
  );
  it.effect("rejects changed run, provider, model or tier before granting anything", () =>
    Effect.gen(function* () {
      const input = createProviderDiagnosticInput("provider", "model", "full", "run");
      const review = providerDoctorApprovalReview(input).review;
      const changes: ProviderDoctorInput[] = [
        { ...input, runId: "new-run" },
        { ...input, instanceId: "other" },
        { ...input, model: "other-model" },
        { ...input, tier: "catalog" },
      ];
      for (const changed of changes) {
        const ops = operations();
        const error = yield* Effect.flip(runConfirmedProviderDiagnostic(changed, review, ops));
        expect(error.message).toContain("changed");
        expect(ops.grants).toEqual([]);
        expect(ops.runs).toEqual([]);
      }
    }),
  );
  it.effect("never reuses an approval for a new confirmed run", () =>
    Effect.gen(function* () {
      const ops = operations();
      for (const runId of ["first", "second"]) {
        const input = createProviderDiagnosticInput("provider", "model", "full", runId);
        yield* runConfirmedProviderDiagnostic(
          input,
          providerDoctorApprovalReview(input).review,
          ops,
        );
      }
      expect(ops.grants).toHaveLength(2);
      expect(ops.runs.map((run) => run.approvalId)).toEqual(["approval-1", "approval-2"]);
      expect(ops.runs.map((run) => run.input.runId)).toEqual(["first", "second"]);
    }),
  );
  it.effect("does not dispatch a charged run if the grant fails or expires", () =>
    Effect.gen(function* () {
      const ops = operations();
      const input = createProviderDiagnosticInput("provider", "model", "full", "run");
      const error = yield* Effect.flip(
        runConfirmedProviderDiagnostic(input, providerDoctorApprovalReview(input).review, {
          ...ops,
          grant: () => Effect.fail(new ProviderDiagnosticRequestError({ reason: "unconfirmed" })),
        }),
      );
      expect(error).toBeInstanceOf(ProviderDiagnosticRequestError);
      expect(ops.runs).toEqual([]);
    }),
  );
  it.effect("keeps the reviewed request immutable while granting approval", () =>
    Effect.gen(function* () {
      const ops = operations();
      const input = { ...createProviderDiagnosticInput("provider", "model", "full", "run") };
      const review = providerDoctorApprovalReview(input).review;
      yield* runConfirmedProviderDiagnostic(input, review, {
        ...ops,
        grant: (value) =>
          Effect.gen(function* () {
            input.model = "changed-after-review";
            return yield* ops.grant(value);
          }),
      });
      expect(ops.runs[0]?.input.model).toBe("model");
    }),
  );
});
