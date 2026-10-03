import { describe, expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Deferred from "effect/Deferred";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
const { mkdtemp, mkdir, rm } = NodeFSP;
const { tmpdir } = NodeOS;
const { join } = NodePath;
import * as ProcessRunner from "../processRunner.ts";
import * as Self from "./SelfDevelopmentService.ts";
const result = {
  stdout: "",
  stderr: "",
  code: ChildProcessSpawner.ExitCode(0),
  timedOut: false,
  stdoutTruncated: false,
  stderrTruncated: false,
  stdoutInvalidUtf8: false,
  stderrInvalidUtf8: false,
};
describe("safe source builds", () => {
  it.effect(
    "rejects live runtime roots, protected symlink targets and existing candidate directories",
    () =>
      Effect.gen(function* () {
        const root = yield* Effect.promise(() => mkdtemp(join(tmpdir(), "t3-source-isolation-")));
        yield* Effect.promise(() => mkdir(join(root, "source", ".git"), { recursive: true }));
        const protectedRoot = join(root, ".t3", "userdata");
        yield* Effect.promise(() => mkdir(protectedRoot, { recursive: true }));
        yield* Effect.promise(() => NodeFSP.symlink(protectedRoot, join(root, "linked-artifacts")));
        yield* Effect.promise(() => mkdir(join(root, "artifacts", "stale"), { recursive: true }));
        yield* Effect.promise(() =>
          NodeFSP.writeFile(join(root, "artifacts", "stale", "keep.txt"), "last-good"),
        );
        let starts = 0;
        const runner = Layer.succeed(ProcessRunner.ProcessRunner, {
          run: () =>
            Effect.sync(() => {
              starts++;
              return result;
            }),
        });
        const testLayer = Self.layer.pipe(Layer.provide(runner), Layer.provide(NodeServices.layer));
        try {
          yield* Effect.gen(function* () {
            const service = yield* Self.SelfDevelopmentService;
            const authority = { trustedOperator: true };
            const profile = {
              id: "build",
              checkout: join(root, "source"),
              artifactDirectory: join(root, "artifacts"),
              command: "owned",
              args: [],
              timeoutMs: 1000,
            };
            expect(
              (yield* service
                .configure({ ...profile, artifactDirectory: join(root, ".t3") }, authority)
                .pipe(Effect.result))._tag,
            ).toBe("Failure");
            expect(
              (yield* service
                .configure(
                  { ...profile, artifactDirectory: join(root, "linked-artifacts") },
                  authority,
                )
                .pipe(Effect.result))._tag,
            ).toBe("Failure");
            yield* service.configure(profile, authority);
            expect(
              (yield* service
                .build({ profileId: "build", operationId: "stale" }, authority)
                .pipe(Effect.result))._tag,
            ).toBe("Failure");
            expect(starts).toBe(0);
          }).pipe(Effect.provide(testLayer), Effect.scoped);
          expect(
            yield* Effect.promise(() =>
              NodeFSP.readFile(join(root, "artifacts", "stale", "keep.txt"), "utf8"),
            ),
          ).toBe("last-good");
        } finally {
          yield* Effect.promise(() => rm(root, { recursive: true, force: true }));
        }
      }),
  );
  it.effect(
    "builds outside source, waits on captured receipt and reloads only registered owner",
    () =>
      Effect.gen(function* () {
        const root = yield* Effect.promise(() => mkdtemp(join(tmpdir(), "t3-source-build-")));
        yield* Effect.promise(() => mkdir(join(root, "source", ".git"), { recursive: true }));
        const requests: ProcessRunner.ProcessRunInput[] = [];
        const runner = Layer.succeed(ProcessRunner.ProcessRunner, {
          run: (input) =>
            Effect.sync(() => {
              requests.push(input);
              return result;
            }),
        });
        const testLayer = Self.layer.pipe(Layer.provide(runner), Layer.provide(NodeServices.layer));
        try {
          yield* Effect.gen(function* () {
            const service = yield* Self.SelfDevelopmentService;
            const authority = { trustedOperator: true };
            const profile = {
              id: "build",
              checkout: join(root, "source"),
              artifactDirectory: join(root, "artifacts"),
              command: "trusted-build",
              args: [],
              timeoutMs: 1000,
            };
            expect(
              (yield* service.configure(profile, { trustedOperator: false }).pipe(Effect.result))
                ._tag,
            ).toBe("Failure");
            yield* service.configure(profile, authority);
            yield* service.build({ profileId: "build", operationId: "one" }, authority);
            expect((yield* service.wait("one")).status).toBe("succeeded");
            expect(requests).toHaveLength(1);
            expect(requests[0]?.cwd).toBe(profile.checkout);
            expect(requests[0]?.env?.T3_BUILD_OUTPUT).toBe(join(root, "artifacts", "one"));
            expect((yield* service.requestReload("one", authority).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
            let reloaded: string | undefined;
            yield* service.registerSupervisor({
              id: "captured-owner",
              profileId: "build",
              reloadCandidate: (path) =>
                Effect.sync(() => {
                  reloaded = path;
                }),
            });
            yield* service.requestReload("one", authority);
            expect(reloaded).toBe(join(root, "artifacts", "one"));
            yield* service.unregisterSupervisor("captured-owner");
            expect((yield* service.requestReload("one", authority).pipe(Effect.result))._tag).toBe(
              "Failure",
            );
          }).pipe(Effect.provide(testLayer), Effect.scoped);
        } finally {
          yield* Effect.promise(() => rm(root, { recursive: true, force: true }));
        }
      }),
  );
  it.effect("cancels only the captured build fiber and rejects concurrent builds", () =>
    Effect.gen(function* () {
      const root = yield* Effect.promise(() => mkdtemp(join(tmpdir(), "t3-source-cancel-")));
      yield* Effect.promise(() => mkdir(join(root, "source", ".git"), { recursive: true }));
      const started = Deferred.makeUnsafe<void>();
      let interrupted = 0;
      const runner = Layer.succeed(ProcessRunner.ProcessRunner, {
        run: () =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                interrupted++;
              }),
            ),
          ),
      });
      const testLayer = Self.layer.pipe(Layer.provide(runner), Layer.provide(NodeServices.layer));
      try {
        yield* Effect.gen(function* () {
          const service = yield* Self.SelfDevelopmentService;
          const authority = { trustedOperator: true };
          yield* service.configure(
            {
              id: "build",
              checkout: join(root, "source"),
              artifactDirectory: join(root, "artifacts"),
              command: "owned",
              args: [],
              timeoutMs: 1000,
            },
            authority,
          );
          yield* service.build({ profileId: "build", operationId: "one" }, authority);
          yield* Deferred.await(started);
          expect(
            (yield* service
              .build({ profileId: "build", operationId: "two" }, authority)
              .pipe(Effect.result))._tag,
          ).toBe("Failure");
          expect((yield* service.cancel("one", authority)).status).toBe("cancelled");
          expect(interrupted).toBe(1);
          expect((yield* service.wait("one")).status).toBe("cancelled");
        }).pipe(Effect.provide(testLayer), Effect.scoped);
      } finally {
        yield* Effect.promise(() => rm(root, { recursive: true, force: true }));
      }
    }),
  );
});
