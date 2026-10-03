import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { ProviderInstanceId } from "@t3tools/contracts";
import * as R from "../../../../packages/contracts/src/runtimeOperations.ts";
import * as ProviderRegistry from "./Services/ProviderRegistry.ts";
import { ProviderDiagnosticRunner } from "./ProviderDiagnosticRunner.ts";
import { WorkflowApprovals, approvalDigest } from "../integrations/WorkflowApprovals.ts";
export interface DiagnosticAuthority {
  readonly trustedOperator: boolean;
  readonly consentRunId?: string;
  readonly consentInstanceId?: string;
  readonly consentModel?: string;
}
export class ProviderDoctor extends Context.Service<
  ProviderDoctor,
  {
    readonly run: (
      input: R.ProviderDoctorInput,
      authority: DiagnosticAuthority,
    ) => Effect.Effect<R.ProviderDoctorResult, R.RuntimeOperationError>;
    readonly get: (
      runId: string,
    ) => Effect.Effect<R.ProviderDoctorResult | null, R.RuntimeOperationError>;
    readonly runApproved: (
      input: R.ProviderDoctorApprovedInput,
      context: { readonly humanSessionId: string },
    ) => Effect.Effect<R.ProviderDoctorResult, R.RuntimeOperationError>;
    readonly cancel: (
      runId: string,
      context: { readonly humanSessionId: string },
    ) => Effect.Effect<boolean, R.RuntimeOperationError>;
    readonly remove: (
      runId: string,
      authority: DiagnosticAuthority,
    ) => Effect.Effect<void, R.RuntimeOperationError>;
  }
>()("t3/provider/ProviderDoctor") {}
const failure = (code: R.RuntimeOperationError["code"], detail: string) =>
  new R.RuntimeOperationError({ code, detail });
const isRuntimeError = Schema.is(R.RuntimeOperationError);
const decodeResult = Schema.decodeUnknownEffect(Schema.fromJsonString(R.ProviderDoctorResult));
const encodeResult = Schema.encodeEffect(Schema.fromJsonString(R.ProviderDoctorResult));
const decodeInput = Schema.decodeUnknownEffect(R.ProviderDoctorInput);
const decodeApprovedInput = Schema.decodeUnknownEffect(R.ProviderDoctorApprovedInput);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const diagnosticRunner = yield* Effect.serviceOption(ProviderDiagnosticRunner);
  const approvals = yield* Effect.serviceOption(WorkflowApprovals);
  const active = new Map<string, Fiber.Fiber<R.ProviderDoctorResult, R.RuntimeOperationError>>();
  const guard = <A, E>(effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.mapError((e) =>
        isRuntimeError(e) ? e : failure("storage", "Provider diagnostic storage failed."),
      ),
    );
  const get = (runId: string) =>
    guard(
      Effect.gen(function* () {
        const rows = yield* sql<{
          json: string;
        }>`SELECT result_json AS json FROM provider_diagnostics WHERE run_id=${runId}`;
        return rows[0] && rows[0].json !== "null" ? yield* decodeResult(rows[0].json) : null;
      }),
    );
  const run = (raw: R.ProviderDoctorInput, authority: DiagnosticAuthority) =>
    guard(
      Effect.gen(function* () {
        const input = yield* decodeInput(raw).pipe(
          Effect.mapError(() => failure("invalid", "Invalid diagnostic request.")),
        );
        if (
          input.tier === "full" &&
          (!authority.trustedOperator ||
            authority.consentRunId !== input.runId ||
            authority.consentInstanceId !== input.instanceId ||
            !input.model ||
            authority.consentModel !== input.model)
        )
          return yield* failure(
            "forbidden",
            "Charged inference is disabled without explicit client consent bound to this run, instance and model.",
          );
        const fingerprint = JSON.stringify(input);
        const rows = yield* sql<{
          input: string;
          json: string;
        }>`SELECT input_json AS input,result_json AS json FROM provider_diagnostics WHERE run_id=${input.runId}`;
        if (rows[0]) {
          if (rows[0].input !== fingerprint)
            return yield* failure(
              "conflict",
              "Diagnostic run identity was reused for a different request.",
            );
          if (rows[0].json === "null")
            return yield* failure(
              "conflict",
              "This diagnostic identity was cleared and cannot be replayed. Choose a new run identity with fresh consent.",
            );
          return yield* decodeResult(rows[0].json);
        }
        const cached = yield* registry.getProviders;
        if (!cached.some((p) => p.instanceId === input.instanceId))
          return yield* failure("notFound", "Configured provider instance was not found.");
        const providers =
          input.tier === "catalog"
            ? yield* registry.refreshInstance(ProviderInstanceId.make(input.instanceId))
            : cached;
        const provider = providers.find((p) => p.instanceId === input.instanceId)!;
        const stages: R.ProviderDoctorResult["stages"][number][] = [
          {
            name: "configuration",
            status:
              provider.enabled && provider.availability !== "unavailable" ? "passed" : "failed",
            detail: "Configured instance availability.",
          },
          {
            name: "installation",
            status: provider.installed ? "passed" : "failed",
            detail:
              input.tier !== "catalog"
                ? "Cached installation state, executable was not invoked."
                : "Refreshed provider installation inventory.",
          },
          {
            name: "authentication",
            status:
              provider.auth.status === "authenticated"
                ? "passed"
                : provider.auth.status === "unknown"
                  ? "unavailable"
                  : "failed",
            detail:
              input.tier !== "catalog"
                ? "Cached authentication state, credentials omitted."
                : "Refreshed authentication status, credentials omitted.",
          },
          {
            name: "model-catalog",
            status:
              provider.models.length > 0 &&
              (!input.model || provider.models.some((m) => m.slug === input.model))
                ? "passed"
                : "unavailable",
            detail:
              input.tier !== "catalog"
                ? "Cached model inventory only."
                : "Explicit native inventory refresh may invoke a process or network.",
          },
          { name: "inference", status: "skipped", detail: "No model turn was executed." },
        ];
        if (input.tier === "full") {
          if (Option.isSome(diagnosticRunner)) {
            const reserved: R.ProviderDoctorResult = {
              instanceId: input.instanceId,
              tier: input.tier,
              runId: input.runId,
              checkedAt: DateTime.formatIso(yield* DateTime.now),
              potentialCost: "quota-or-billing",
              stages: [
                ...stages.slice(0, -1),
                {
                  name: "inference",
                  status: "unavailable",
                  detail:
                    "This run is reserved. In-flight or interrupted charged inference is never automatically replayed.",
                },
              ],
            };
            const claims =
              yield* sql`INSERT OR IGNORE INTO provider_diagnostics VALUES(${input.runId},${fingerprint},${JSON.stringify(reserved)}) RETURNING run_id`;
            if (!claims.length) {
              const existing = yield* sql<{
                input: string;
                json: string;
              }>`SELECT input_json AS input,result_json AS json FROM provider_diagnostics WHERE run_id=${input.runId}`;
              if (!existing[0] || existing[0].input !== fingerprint || existing[0].json === "null")
                return yield* failure(
                  "conflict",
                  "Diagnostic identity is already reserved by a different or cleared request.",
                );
              return yield* decodeResult(existing[0].json);
            }
            stages.pop();
            stages.push(
              ...(yield* diagnosticRunner.value.run(input).pipe(
                Effect.catch(() =>
                  Effect.succeed([
                    {
                      name: "inference",
                      status: "failed" as const,
                      detail:
                        "Disposable diagnostic preparation failed. Provider output is omitted.",
                    },
                  ]),
                ),
              )),
            );
          } else
            stages.push({
              name: "disposable-runtime",
              status: "unavailable",
              detail:
                "Disposable isolated diagnostic workspace and echo-tool driver verification are not configured. Consent alone does not establish this capability.",
            });
        }
        const result: R.ProviderDoctorResult = {
          instanceId: input.instanceId,
          tier: input.tier,
          runId: input.runId,
          checkedAt: DateTime.formatIso(yield* DateTime.now),
          potentialCost:
            input.tier === "offline"
              ? "none"
              : input.tier === "catalog"
                ? "network-or-process"
                : "quota-or-billing",
          stages,
        };
        const json = yield* encodeResult(result);
        if (input.tier === "full" && Option.isSome(diagnosticRunner))
          yield* sql`UPDATE provider_diagnostics SET result_json=${json} WHERE run_id=${input.runId} AND input_json=${fingerprint} AND result_json<>'null'`;
        else
          yield* sql`INSERT INTO provider_diagnostics VALUES(${input.runId},${fingerprint},${json})`;
        return result;
      }),
    );
  const runApproved = (
    raw: R.ProviderDoctorApprovedInput,
    context: { readonly humanSessionId: string },
  ) =>
    guard(
      Effect.scoped(
        Effect.gen(function* () {
          const request = yield* decodeApprovedInput(raw).pipe(
            Effect.mapError(() => failure("invalid", "Invalid approved diagnostic request.")),
          );
          const input = request.input;
          if (
            input.tier !== "full" ||
            !input.model ||
            !context.humanSessionId ||
            Option.isNone(approvals)
          )
            return yield* failure(
              "forbidden",
              "Full diagnostics require a fresh authenticated human approval for the exact reviewed request.",
            );
          const review = R.providerDoctorApprovalReview(input);
          const digest = approvalDigest(review.review);
          yield* sql.withTransaction(
            Effect.gen(function* () {
              const prior = yield* sql<{
                approval_id: string;
                human_session_id: string;
                review_digest: string;
                expires_at: number;
              }>`SELECT approval_id,human_session_id,review_digest,expires_at FROM provider_diagnostic_consents WHERE run_id=${input.runId}`;
              if (prior[0]) {
                if (
                  prior[0].approval_id !== request.approvalId ||
                  prior[0].human_session_id !== context.humanSessionId ||
                  prior[0].review_digest !== digest
                )
                  return yield* failure(
                    "forbidden",
                    "Diagnostic consent does not match this human session and reviewed request.",
                  );
                const cached =
                  yield* sql`SELECT 1 FROM provider_diagnostics WHERE run_id=${input.runId}`;
                if (!cached.length && prior[0].expires_at < Date.now())
                  return yield* failure(
                    "forbidden",
                    "Diagnostic consent expired before native dispatch. Choose a new run identity and approve it again.",
                  );
                return;
              }
              yield* approvals.value
                .consumeForSession(
                  request.approvalId,
                  review.operation,
                  review.review,
                  context.humanSessionId,
                )
                .pipe(
                  Effect.mapError(() =>
                    failure(
                      "forbidden",
                      "Fresh human diagnostic approval is required and can be consumed only once.",
                    ),
                  ),
                );
              yield* sql`INSERT INTO provider_diagnostic_consents VALUES(${input.runId},${request.approvalId},${context.humanSessionId},${digest},${Date.now() + 60_000})`;
            }),
          );
          const running = active.get(input.runId);
          if (running) {
            const cached = yield* get(input.runId);
            return cached ?? (yield* Fiber.join(running));
          }
          const fiber = yield* run(input, {
            trustedOperator: true,
            consentRunId: input.runId,
            consentInstanceId: input.instanceId,
            consentModel: input.model,
          }).pipe(Effect.forkScoped);
          active.set(input.runId, fiber);
          return yield* Fiber.join(fiber).pipe(
            Effect.ensuring(
              Effect.sync(() => {
                if (active.get(input.runId) === fiber) active.delete(input.runId);
              }),
            ),
          );
        }),
      ),
    );
  const cancel = (runId: string, context: { readonly humanSessionId: string }) =>
    guard(
      Effect.gen(function* () {
        const rows = yield* sql<{
          human_session_id: string;
        }>`SELECT human_session_id FROM provider_diagnostic_consents WHERE run_id=${runId}`;
        if (!context.humanSessionId || rows[0]?.human_session_id !== context.humanSessionId)
          return yield* failure(
            "forbidden",
            "Only the authenticated human owner can cancel this diagnostic.",
          );
        const fiber = active.get(runId);
        if (!fiber) return false;
        yield* Fiber.interrupt(fiber);
        return true;
      }),
    );
  const remove = (runId: string, a: DiagnosticAuthority) =>
    guard(
      Effect.gen(function* () {
        if (!a.trustedOperator)
          return yield* failure(
            "forbidden",
            "Deleting diagnostics requires a trusted client operator.",
          );
        // Retain only the consumed identity so clearing a result cannot accidentally replay a charged run.
        yield* sql`UPDATE provider_diagnostics SET result_json='null' WHERE run_id=${runId}`;
      }),
    );
  return ProviderDoctor.of({ run, runApproved, cancel, get, remove });
});
export const layer = Layer.effect(ProviderDoctor, make);
