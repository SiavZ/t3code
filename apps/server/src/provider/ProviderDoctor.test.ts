import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProviderInstanceId, ProviderDriverKind, type ServerProvider } from "@t3tools/contracts";
import * as Approvals from "../integrations/WorkflowApprovals.ts";
import * as Runner from "./ProviderDiagnosticRunner.ts";
import * as Registry from "./Services/ProviderRegistry.ts";
import * as Doctor from "./ProviderDoctor.ts";
import migrate from "../persistence/Migrations/067_ProviderDiagnostics.ts";
let refreshes = 0;
const provider: ServerProvider = {
  instanceId: ProviderInstanceId.make("codex"),
  driver: ProviderDriverKind.make("codex"),
  enabled: true,
  installed: true,
  version: "1",
  status: "ready",
  auth: { status: "authenticated", email: "private@example.org" },
  checkedAt: "2026-10-03T00:00:00.000Z",
  models: [],
  slashCommands: [],
  skills: [],
};
const registry = Layer.succeed(Registry.ProviderRegistry, {
  getProviders: Effect.succeed([provider]),
  refresh: () => Effect.succeed([provider]),
  refreshInstance: () =>
    Effect.sync(() => {
      refreshes++;
      return [provider];
    }),
  refreshWorkspaceSnapshot: () => Effect.succeed([provider]),
  getProviderMaintenanceCapabilitiesForInstance: () => Effect.die("unused"),
  setProviderMaintenanceActionState: () => Effect.succeed([provider]),
  streamChanges: Stream.empty,
});
const services = Doctor.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
  Layer.provide(registry),
);
it.layer(services)("ProviderDoctor", (it) => {
  it.effect("offline uses cache, catalog refreshes once, retries persist and redact", () =>
    Effect.gen(function* () {
      yield* migrate;
      const doctor = yield* Doctor.ProviderDoctor;
      refreshes = 0;
      const input = { instanceId: "codex", tier: "offline" as const, runId: "offline" };
      const offline = yield* doctor.run(input, { trustedOperator: false });
      assert.equal(refreshes, 0);
      assert.equal(offline.potentialCost, "none");
      assert.ok(!JSON.stringify(offline).includes("private@example.org"));
      const catalog = yield* doctor.run(
        { ...input, tier: "catalog", runId: "catalog" },
        { trustedOperator: false },
      );
      assert.equal(refreshes, 1);
      assert.deepEqual(
        yield* doctor.run(
          { ...input, tier: "catalog", runId: "catalog" },
          { trustedOperator: false },
        ),
        catalog,
      );
      assert.equal(refreshes, 1);
      assert.equal(
        (yield* doctor
          .run({ ...input, runId: "catalog" }, { trustedOperator: false })
          .pipe(Effect.flip)).code,
        "conflict",
      );
    }),
  );
  it.effect(
    "denies charged tier without exact trusted consent and reports external prerequisite honestly",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const doctor = yield* Doctor.ProviderDoctor;
        const input = {
          instanceId: "codex",
          tier: "full" as const,
          runId: "charged",
          model: "model",
        };
        assert.equal(
          (yield* doctor.run(input, { trustedOperator: false }).pipe(Effect.flip)).code,
          "forbidden",
        );
        const result = yield* doctor.run(input, {
          trustedOperator: true,
          consentRunId: "charged",
          consentInstanceId: "codex",
          consentModel: "model",
        });
        assert.equal(result.stages.at(-1)?.status, "unavailable");
        yield* doctor.remove(input.runId, { trustedOperator: true });
        assert.equal(yield* doctor.get(input.runId), null);
      }),
  );
});

const approvals = Layer.succeed(Approvals.WorkflowApprovals, {
  grant: () => Effect.succeed("approval"),
  consume: () => Effect.void,
  consumeForSession: () => Effect.void,
  revokeSession: () => Effect.void,
});
it.effect("lets only the approving human session cancel an in-flight approved run", () =>
  Effect.gen(function* () {
    const started = yield* Deferred.make<void>();
    const runner = Layer.succeed(Runner.ProviderDiagnosticRunner, {
      run: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
    });
    yield* Effect.gen(function* () {
      yield* migrate;
      const doctor = yield* Doctor.ProviderDoctor;
      const input = {
        instanceId: "codex",
        tier: "full" as const,
        runId: "cancel-me",
        model: "model",
      };
      const running = yield* doctor
        .runApproved({ input, approvalId: "approval" }, { humanSessionId: "human" })
        .pipe(Effect.exit, Effect.forkChild);
      yield* Deferred.await(started);
      const foreign = yield* doctor
        .cancel(input.runId, { humanSessionId: "other" })
        .pipe(Effect.flip);
      assert.equal(foreign.code, "forbidden");
      assert.equal(yield* doctor.cancel(input.runId, { humanSessionId: "human" }), true);
      assert.ok(Exit.isFailure(yield* Fiber.join(running)));
      assert.equal(yield* doctor.cancel(input.runId, { humanSessionId: "human" }), false);
    }).pipe(
      Effect.provide(
        Doctor.layer.pipe(
          Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
          Layer.provide(Layer.mergeAll(registry, runner, approvals)),
        ),
      ),
    );
  }),
);
