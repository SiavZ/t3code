import { Context, Effect, Layer, Option, Schema } from "effect";
import { makeCurrentCheck } from "./IntegrationConfigurationGuard.ts";
import { ThreadId } from "@t3tools/contracts";
import * as Projection from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import { isIntegrationSecretRefFor } from "./IntegrationSecrets.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import * as Attempts from "./WorkflowAttempts.ts";
import * as Snapshots from "./SourceSnapshots.ts";
export class RemoteBuildError extends Schema.TaggedError<RemoteBuildError>()("RemoteBuildError", {
  reason: Schema.Literals([
    "unconfigured",
    "authentication-required",
    "not-entitled",
    "insufficient-credits",
    "invalid-response",
    "snapshot-missing",
    "invalid-input",
    "approval-required",
    "unknown-outcome",
    "already-attempted",
  ]),
}) {}
export class RemoteBuildConfiguration extends Context.Service<
  RemoteBuildConfiguration,
  {
    readonly enabled: boolean;
    readonly protocol: "jcode-compile-v1";
    readonly baseUrl: string;
    readonly apiKeySecretRef: string;
  }
>()("t3/integrations/RemoteBuildConfiguration") {}
const Account = Schema.Struct({
  status: Schema.String,
  capabilities: Schema.Struct({ remote_compile: Schema.Boolean }),
  entitlements: Schema.optional(Schema.Struct({ cloud_compute: Schema.optional(Schema.Boolean) })),
});
const Credits = Schema.Struct({
  compute: Schema.Struct({
    unit: Schema.Literal("microcredits"),
    available_microcredits: Schema.Number,
  }),
});
const Result = Schema.Struct({
  exit_code: Schema.Number,
  stdout: Schema.String,
  stderr: Schema.String,
  truncated: Schema.optional(Schema.Boolean),
  cleanup_confirmed: Schema.optional(Schema.Boolean),
});
export interface BuildSubmission {
  readonly snapshotId: string;
  readonly requestId: string;
  readonly command: string;
  readonly timeoutSeconds: number;
  readonly unknownCostAcknowledged: true;
  readonly approvalId: string;
}
export class RemoteBuildService extends Context.Service<
  RemoteBuildService,
  {
    readonly status: () => Effect.Effect<
      {
        readonly state: "unconfigured" | "authentication-required" | "eligible" | "not-entitled";
        readonly availableMicrocredits?: number;
        readonly upstreamCancel: false;
        readonly artifacts: false;
      },
      RemoteBuildError
    >;
    readonly prepareForThread: (threadId: string) => Effect.Effect<
      {
        readonly snapshotId: string;
        readonly digest: string;
        readonly paths: ReadonlyArray<string>;
        readonly excluded: ReadonlyArray<string>;
        readonly bytes: number;
      },
      RemoteBuildError
    >;
    readonly prepare: (root: string) => Effect.Effect<
      {
        readonly snapshotId: string;
        readonly digest: string;
        readonly paths: ReadonlyArray<string>;
        readonly excluded: ReadonlyArray<string>;
        readonly bytes: number;
      },
      RemoteBuildError
    >;
    readonly discard: (snapshotId: string) => Effect.Effect<void>;
    /** Cancellation interrupts waiting only. Backend may continue and charge usage. */
    readonly submit: (
      input: BuildSubmission,
    ) => Effect.Effect<typeof Result.Type, RemoteBuildError>;
  }
>()("t3/integrations/RemoteBuildService") {}
const make = Effect.gen(function* () {
  const config = yield* RemoteBuildConfiguration;
  const current = yield* makeCurrentCheck("build");
  const http = yield* Http.IntegrationHttp;
  const secrets = yield* Secrets.ServerSecretStore;
  const approvals = yield* Approvals.WorkflowApprovals;
  const attempts = yield* Attempts.WorkflowAttempts;
  const snapshots = yield* Snapshots.SourceSnapshots;
  const projection = yield* Effect.serviceOption(Projection.ProjectionSnapshotQuery);
  const prepared = new Map<string, Snapshots.SourceSnapshot>();
  const error = (reason: RemoteBuildError["reason"]) => new RemoteBuildError({ reason });
  const key = () =>
    secrets.get(config.apiKeySecretRef).pipe(
      Effect.mapError(() => error("authentication-required")),
      Effect.flatMap((value) =>
        Option.isSome(value)
          ? Effect.succeed(Buffer.from(value.value).toString("utf8"))
          : Effect.fail(error("authentication-required")),
      ),
    );
  const request = (suffix: string, body?: unknown) =>
    Effect.gen(function* () {
      if (
        !(yield* current()) ||
        !config.enabled ||
        !config.baseUrl ||
        config.protocol !== "jcode-compile-v1"
      )
        return yield* Effect.fail(error("unconfigured"));
      if (!isIntegrationSecretRefFor(config.apiKeySecretRef, "build"))
        return yield* Effect.fail(error("authentication-required"));
      const token = yield* key();
      return yield* http
        .request({
          url: `${config.baseUrl.replace(/\/$/, "")}/${suffix}`,
          method: body === undefined ? "GET" : "POST",
          headers: { authorization: `Bearer ${token}`, "content-type": "application/json" },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
          maxBytes: 1_048_576,
          timeoutMs: suffix === "compile" ? 720_000 : 60_000,
        })
        .pipe(
          Effect.mapError((failure) =>
            error(
              failure.status === 401
                ? "authentication-required"
                : failure.status === 402
                  ? "insufficient-credits"
                  : body === undefined
                    ? "invalid-response"
                    : "unknown-outcome",
            ),
          ),
        );
    });
  const entitlement = () =>
    Effect.gen(function* () {
      const account = yield* request("me").pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Account)),
        Effect.mapError((failure) =>
          failure instanceof RemoteBuildError ? failure : error("invalid-response"),
        ),
      );
      if (
        account.status.toLowerCase() !== "active" ||
        !account.capabilities.remote_compile ||
        account.entitlements?.cloud_compute === false
      )
        return yield* Effect.fail(error("not-entitled"));
      const credits = yield* request("compute/usage").pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Credits)),
        Effect.mapError((failure) =>
          failure instanceof RemoteBuildError ? failure : error("invalid-response"),
        ),
      );
      return credits.compute.available_microcredits;
    });
  const service = RemoteBuildService.of({
    prepareForThread: (threadId) =>
      Effect.gen(function* () {
        if (Option.isNone(projection)) return yield* Effect.fail(error("invalid-input"));
        const id = yield* Schema.decodeUnknownEffect(ThreadId)(threadId).pipe(
          Effect.mapError(() => error("invalid-input")),
        );
        const context = yield* projection.value
          .getThreadCheckpointContext(id)
          .pipe(Effect.mapError(() => error("invalid-input")));
        if (Option.isNone(context)) return yield* Effect.fail(error("invalid-input"));
        return yield* service.prepare(context.value.worktreePath ?? context.value.workspaceRoot);
      }),
    status: () =>
      Effect.gen(function* () {
        if (!(yield* current()) || !config.enabled)
          return {
            state: "unconfigured" as const,
            upstreamCancel: false as const,
            artifacts: false as const,
          };
        const result = yield* entitlement().pipe(Effect.result);
        if (result._tag === "Success")
          return {
            state: "eligible" as const,
            availableMicrocredits: result.success,
            upstreamCancel: false as const,
            artifacts: false as const,
          };
        const failure = result.failure;
        if (failure.reason === "authentication-required" || failure.reason === "not-entitled")
          return {
            state: failure.reason,
            upstreamCancel: false as const,
            artifacts: false as const,
          };
        return yield* Effect.fail(failure);
      }),
    prepare: (root) =>
      Effect.gen(function* () {
        yield* entitlement();
        const snapshot = yield* snapshots
          .prepare(root)
          .pipe(Effect.mapError(() => error("invalid-input")));
        if (prepared.size >= 8) prepared.delete(prepared.keys().next().value!);
        prepared.set(snapshot.snapshotId, snapshot);
        return {
          snapshotId: snapshot.snapshotId,
          digest: snapshot.digest,
          paths: snapshot.files.map((file) => file.path),
          excluded: snapshot.excluded,
          bytes: snapshot.bytes,
        };
      }),
    discard: (id) =>
      Effect.sync(() => {
        prepared.delete(id);
      }),
    submit: (input) =>
      Effect.gen(function* () {
        if (
          !input.command.trim() ||
          input.command.length > 8192 ||
          input.command.includes("\0") ||
          input.timeoutSeconds < 1 ||
          input.timeoutSeconds > 600 ||
          !Number.isInteger(input.timeoutSeconds) ||
          input.unknownCostAcknowledged !== true
        )
          return yield* Effect.fail(error("invalid-input"));
        const snapshot = prepared.get(input.snapshotId);
        if (!snapshot) return yield* Effect.fail(error("snapshot-missing"));
        const available = yield* entitlement();
        if (available <= 0) return yield* Effect.fail(error("insufficient-credits"));
        // This backend cannot enforce a caller credit ceiling. Explicit unknown-price review is required.
        const review = JSON.stringify({
          backend: config.baseUrl,
          protocol: config.protocol,
          digest: snapshot.digest,
          command: input.command,
          timeoutSeconds: input.timeoutSeconds,
          price: "backend-metered-no-client-ceiling",
          unknownCostAcknowledged: input.unknownCostAcknowledged,
        });
        yield* approvals
          .consume(input.approvalId, "build.submit", review)
          .pipe(Effect.mapError(() => error("approval-required")));
        yield* attempts
          .claim(input.requestId, "build.submit", review)
          .pipe(Effect.mapError(() => error("already-attempted")));
        const result = yield* request("compile", {
          request_id: input.requestId,
          command: input.command,
          timeout_seconds: input.timeoutSeconds,
          files: snapshot.files,
        }).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Result)),
          Effect.mapError(() => error("unknown-outcome")),
        );
        yield* attempts
          .settle(input.requestId, "completed")
          .pipe(Effect.mapError(() => error("unknown-outcome")));
        return result;
      }),
  });
  return service;
});
export const layer = Layer.effect(RemoteBuildService, make);
