import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Scope from "effect/Scope";
import * as R from "../../../../packages/contracts/src/runtimeOperations.ts";
import * as ProcessRunner from "../processRunner.ts";
export const SourceBuildProfile = R.SourceBuildProfile;
export type SourceBuildProfile = typeof SourceBuildProfile.Type;
export const SourceBuildReceipt = R.SourceBuildReceipt;
export type SourceBuildReceipt = typeof SourceBuildReceipt.Type;
export interface SourceBuildAuthority {
  readonly trustedOperator: boolean;
}
export interface OwnedSupervisor {
  readonly id: string;
  readonly profileId: string;
  readonly reloadCandidate: (
    artifactDirectory: string,
  ) => Effect.Effect<void, R.RuntimeOperationError>;
}
export class SelfDevelopmentService extends Context.Service<
  SelfDevelopmentService,
  {
    readonly configure: (
      profile: SourceBuildProfile,
      authority: SourceBuildAuthority,
    ) => Effect.Effect<void, R.RuntimeOperationError>;
    readonly registerSupervisor: (supervisor: OwnedSupervisor) => Effect.Effect<void>;
    readonly unregisterSupervisor: (id: string) => Effect.Effect<void>;
    readonly status: () => Effect.Effect<ReadonlyArray<SourceBuildReceipt>>;
    readonly wait: (
      operationId: string,
    ) => Effect.Effect<SourceBuildReceipt, R.RuntimeOperationError>;
    readonly build: (
      input: { profileId: string; operationId: string },
      authority: SourceBuildAuthority,
    ) => Effect.Effect<SourceBuildReceipt, R.RuntimeOperationError>;
    readonly cancel: (
      operationId: string,
      authority: SourceBuildAuthority,
    ) => Effect.Effect<SourceBuildReceipt, R.RuntimeOperationError>;
    readonly requestReload: (
      operationId: string,
      authority: SourceBuildAuthority,
    ) => Effect.Effect<void, R.RuntimeOperationError>;
  }
>()("t3/orchestration/SelfDevelopmentService") {}
const failure = (code: R.RuntimeOperationError["code"], detail: string) =>
  new R.RuntimeOperationError({ code, detail });
const decodeProfile = Schema.decodeUnknownEffect(SourceBuildProfile);
const isRuntimeError = Schema.is(R.RuntimeOperationError);
const make = Effect.gen(function* () {
  const scope = yield* Scope.Scope;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const runner = yield* ProcessRunner.ProcessRunner;
  const profiles = new Map<string, SourceBuildProfile>();
  const supervisors = new Map<string, OwnedSupervisor>();
  const operations = new Map<
    string,
    { receipt: SourceBuildReceipt; fiber?: Fiber.Fiber<void>; artifactDirectory: string }
  >();
  const trusted = (a: SourceBuildAuthority) =>
    a.trustedOperator
      ? Effect.void
      : Effect.fail(
          failure("forbidden", "Source build operations require trusted client configuration."),
        );
  const configure = (raw: SourceBuildProfile, a: SourceBuildAuthority) =>
    Effect.gen(function* () {
      yield* trusted(a);
      const profile = yield* decodeProfile(raw).pipe(
        Effect.mapError(() => failure("invalid", "Invalid source build profile.")),
      );
      const checkout = yield* fs
        .realPath(profile.checkout)
        .pipe(Effect.mapError(() => failure("invalid", "Source checkout is not readable.")));
      const requestedArtifact = path.resolve(profile.artifactDirectory);
      let ancestor = requestedArtifact;
      while (
        !(yield* fs
          .exists(ancestor)
          .pipe(
            Effect.mapError(() =>
              failure("storage", "Build artifact parent could not be inspected."),
            ),
          ))
      ) {
        const parent = path.dirname(ancestor);
        if (parent === ancestor) break;
        ancestor = parent;
      }
      const realAncestor = yield* fs
        .realPath(ancestor)
        .pipe(Effect.mapError(() => failure("invalid", "Build artifact parent is not readable.")));
      const artifact = path.join(realAncestor, path.relative(ancestor, requestedArtifact));
      const protectedPath = artifact.toLowerCase();
      if (
        !path.isAbsolute(profile.artifactDirectory) ||
        artifact === checkout ||
        artifact.startsWith(checkout + path.sep) ||
        protectedPath.endsWith(`${path.sep}.t3`) ||
        protectedPath.includes(`${path.sep}.t3${path.sep}`) ||
        protectedPath.includes(`${path.sep}.jcode${path.sep}userdata`)
      )
        return yield* failure(
          "invalid",
          "Build artifacts must be outside the checkout and live runtime data directories.",
        );
      if (
        !(yield* fs
          .exists(path.join(checkout, ".git"))
          .pipe(
            Effect.mapError(() =>
              failure("storage", "Source Git metadata could not be inspected."),
            ),
          ))
      )
        return yield* failure("invalid", "A configured source checkout must contain Git metadata.");
      profiles.set(profile.id, { ...profile, checkout, artifactDirectory: artifact });
    });
  const build = (input: { profileId: string; operationId: string }, a: SourceBuildAuthority) =>
    Effect.gen(function* () {
      yield* trusted(a);
      const previous = operations.get(input.operationId);
      if (previous) {
        if (previous.receipt.profileId !== input.profileId)
          return yield* failure("conflict", "Build identity already belongs to another profile.");
        return previous.receipt;
      }
      const profile = profiles.get(input.profileId);
      if (!profile) return yield* failure("unavailable", "Source build profile is not configured.");
      if ([...operations.values()].some((o) => o.receipt.status === "running"))
        return yield* failure("busy", "A source build is already running.");
      if (!/^[a-zA-Z0-9_-]{1,120}$/.test(input.operationId))
        return yield* failure("invalid", "Build identity must be a bounded path-safe identifier.");
      const artifactDirectory = path.join(profile.artifactDirectory, input.operationId);
      const operation: {
        receipt: SourceBuildReceipt;
        fiber?: Fiber.Fiber<void>;
        artifactDirectory: string;
      } = {
        receipt: {
          ...input,
          status: "running",
          detail: "Isolated source build started. Running installations are untouched.",
        },
        artifactDirectory,
      };
      operations.set(input.operationId, operation);
      yield* Effect.gen(function* () {
        yield* fs.makeDirectory(profile.artifactDirectory, { recursive: true });
        if ((yield* fs.realPath(profile.artifactDirectory)) !== profile.artifactDirectory)
          return yield* failure("invalid", "Build artifact parent changed since configuration.");
        if (yield* fs.exists(artifactDirectory))
          return yield* failure(
            "conflict",
            "Build candidate directory already exists. Select a new operation identity.",
          );
        yield* fs.makeDirectory(artifactDirectory);
      }).pipe(
        Effect.tapError(() => Effect.sync(() => operations.delete(input.operationId))),
        Effect.mapError((error) =>
          isRuntimeError(error)
            ? error
            : failure("storage", "Build artifact directory could not be created."),
        ),
      );
      if (operation.receipt.status === "cancelled") return operation.receipt;
      operation.fiber = yield* runner
        .run({
          command: profile.command,
          args: profile.args,
          cwd: profile.checkout,
          timeout: profile.timeoutMs,
          maxOutputBytes: 32768,
          outputMode: "truncate",
          env: {
            T3CODE_HOME: path.join(artifactDirectory, "sandbox-home"),
            T3_BUILD_OUTPUT: artifactDirectory,
          },
        })
        .pipe(
          Effect.match({
            onFailure: () => {
              operation.receipt = {
                ...operation.receipt,
                status: "failed",
                detail:
                  "Source build failed. Existing running server and last-good artifacts were not changed.",
              };
            },
            onSuccess: (result) => {
              operation.receipt = {
                ...operation.receipt,
                status: result.code === 0 && !result.timedOut ? "succeeded" : "failed",
                detail:
                  result.code === 0 && !result.timedOut
                    ? "Build succeeded. Reload requires an explicitly registered owning supervisor."
                    : "Build failed. Running installation was not changed.",
              };
            },
          }),
          Effect.forkScoped,
          Effect.provideService(Scope.Scope, scope),
        );
      if (operations.get(input.operationId)?.receipt.status === "cancelled")
        yield* Fiber.interrupt(operation.fiber);
      return operation.receipt;
    });
  const cancel = (id: string, a: SourceBuildAuthority) =>
    Effect.gen(function* () {
      yield* trusted(a);
      const operation = operations.get(id);
      if (!operation) return yield* failure("notFound", "Build was not found.");
      if (operation.receipt.status === "running") {
        operation.receipt = {
          ...operation.receipt,
          status: "cancelled",
          detail: "Owned build cancelled. No unrelated process was touched.",
        };
        if (operation.fiber) yield* Fiber.interrupt(operation.fiber);
      }
      return operation.receipt;
    });
  const requestReload = (id: string, a: SourceBuildAuthority) =>
    Effect.gen(function* () {
      yield* trusted(a);
      const operation = operations.get(id);
      if (!operation || operation.receipt.status !== "succeeded")
        return yield* failure("invalid", "Reload requires a successful build receipt.");
      const owners = [...supervisors.values()].filter(
        (s) => s.profileId === operation.receipt.profileId,
      );
      if (owners.length !== 1)
        return yield* failure(
          "unavailable",
          "Exactly one owning supervisor must be registered. This server cannot replace or terminate itself.",
        );
      yield* owners[0]!.reloadCandidate(operation.artifactDirectory);
    });
  const wait = (id: string) =>
    Effect.gen(function* () {
      const operation = operations.get(id);
      if (!operation) return yield* failure("notFound", "Build was not found.");
      if (operation.fiber) yield* Fiber.await(operation.fiber);
      return operation.receipt;
    });
  return SelfDevelopmentService.of({
    configure,
    registerSupervisor: (s) =>
      Effect.sync(() => {
        supervisors.set(s.id, s);
      }),
    unregisterSupervisor: (id) =>
      Effect.sync(() => {
        supervisors.delete(id);
      }),
    status: () => Effect.sync(() => [...operations.values()].map((o) => o.receipt)),
    wait,
    build,
    cancel,
    requestReload,
  });
});
export const layer = Layer.effect(SelfDevelopmentService, make);
