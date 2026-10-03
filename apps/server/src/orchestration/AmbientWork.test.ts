import { expect, it } from "@effect/vitest";
import { ProjectId, ThreadId, type BackgroundPolicySnapshot } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as DateTime from "effect/DateTime";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { TestClock } from "effect/testing";
import * as AmbientWork from "./AmbientWork.ts";
import * as ScheduledWork from "./ScheduledWork.ts";
import * as BackgroundPolicy from "../background/BackgroundPolicy.ts";
import { ProjectionSnapshotQuery } from "./Services/ProjectionSnapshotQuery.ts";
import { OrchestrationEngineService } from "./Services/OrchestrationEngine.ts";
import migrate from "../persistence/Migrations/059_ScheduledWork.ts";

const owner = ThreadId.make("ambient-owner");
const project = ProjectId.make("ambient-project");
const config = {
  callerThreadId: owner,
  enabled: true,
  grantId: "human-consent",
  prompt: "Inspect recent work without changing permissions.",
  idleDelayMs: 60_000,
  minimumCycleMs: 60_000,
  maxCyclesPerDay: 2,
  timezone: "UTC",
  allowedHourStart: 0,
  allowedHourEnd: 24,
  allowUnknownQuota: true,
};
function harness() {
  let snapshot: BackgroundPolicySnapshot = {
    hostPower: {
      source: "node-macos-native",
      idle: "true",
      idleSeconds: 3600,
      locked: "false",
      suspended: false,
      onBattery: "false",
      lowPowerMode: "false",
      thermalState: "nominal",
      stale: false,
      updatedAt: DateTime.makeUnsafe(0),
    },
    leases: [],
    activeForegroundLeaseCount: 0,
    activeScopeKeys: [],
    shouldRunOpportunisticWork: false,
    updatedAt: DateTime.makeUnsafe(0),
  };
  const activation = Layer.succeed(ScheduledWork.ScheduledWorkActivation, {
    authorize: () =>
      Effect.succeed({
        projectId: project,
        grantRevision: 1,
        ceiling: { runtimeMode: "approval-required", mcpCapabilities: ["workers"] },
      }),
    validate: () => Effect.void,
    dispatch: () => Effect.succeed({ sequence: 1, threadId: owner }),
    reconcile: () => Effect.succeed(null),
    cancel: () => Effect.succeed("cancelled"),
  });
  const engine = Layer.mock(OrchestrationEngineService)({
    subscribeDomainEvents: Effect.succeed(Stream.empty),
  });
  const query = Layer.succeed(ProjectionSnapshotQuery, {
    getWorkerState: () =>
      Effect.succeed(
        Option.some({ thread: { id: owner, projectId: project }, pendingMessageId: null }),
      ),
    getShellSnapshot: () => Effect.succeed({ threads: [] }),
  } as unknown as ProjectionSnapshotQuery["Service"]);
  const policy = Layer.succeed(BackgroundPolicy.BackgroundPolicy, {
    snapshot: Effect.sync(() => snapshot),
  } as BackgroundPolicy.BackgroundPolicy["Service"]);
  const base = Layer.mergeAll(
    NodeSqliteClient.layer({ filename: ":memory:" }),
    activation,
    engine,
    query,
    policy,
  );
  const scheduler = ScheduledWork.layer.pipe(Layer.provideMerge(base));
  return {
    layer: AmbientWork.layer.pipe(Layer.provideMerge(scheduler)),
    setForeground: (count: number) => {
      snapshot = { ...snapshot, activeForegroundLeaseCount: count };
    },
  };
}

it.effect(
  "is absent by default, waits for idle, persists cycles, never duplicates an active cycle and disables pending work",
  () => {
    const test = harness();
    return Effect.gen(function* () {
      yield* migrate;
      const ambient = yield* AmbientWork.AmbientWork;
      const scheduler = yield* ScheduledWork.ScheduledWork;
      expect(yield* ambient.get({ callerThreadId: owner })).toBe(null);
      yield* ambient.configure(config, { source: "client" });
      yield* ambient.drain;
      expect((yield* scheduler.list({ callerThreadId: owner })).length).toBe(0);
      yield* TestClock.adjust(60_000);
      yield* ambient.drain;
      const first = yield* scheduler.list({ callerThreadId: owner });
      expect(first.length).toBe(1);
      expect(first[0]?.target.type).toBe("ambient");
      yield* ambient.drain;
      expect((yield* scheduler.list({ callerThreadId: owner })).length).toBe(1);
      expect((yield* ambient.get({ callerThreadId: owner }))?.cycles).toBe(1);
      yield* ambient.stop({ callerThreadId: owner });
      expect((yield* scheduler.get({ callerThreadId: owner, id: first[0]!.id })).state).toBe(
        "cancelled",
      );
      yield* TestClock.adjust(86_400_000);
      yield* ambient.drain;
      expect((yield* scheduler.list({ callerThreadId: owner })).length).toBe(1);
    }).pipe(Effect.provide(test.layer));
  },
);

it.effect(
  "foreground interaction resets the idle boundary, unknown quota blocks without explicit opt-in",
  () => {
    const test = harness();
    return Effect.gen(function* () {
      yield* migrate;
      const ambient = yield* AmbientWork.AmbientWork;
      const scheduler = yield* ScheduledWork.ScheduledWork;
      yield* ambient.configure({ ...config, allowUnknownQuota: false }, { source: "client" });
      yield* TestClock.adjust(60_000);
      yield* ambient.drain;
      expect((yield* ambient.get({ callerThreadId: owner }))?.reason).toContain("quota");
      yield* ambient.configure(config, { source: "client" });
      test.setForeground(1);
      yield* ambient.drain;
      test.setForeground(0);
      yield* TestClock.adjust(59_999);
      yield* ambient.drain;
      expect((yield* scheduler.list({ callerThreadId: owner })).length).toBe(0);
      yield* TestClock.adjust(1);
      yield* ambient.drain;
      expect((yield* scheduler.list({ callerThreadId: owner })).length).toBe(1);
      const sql = yield* SqlClient.SqlClient;
      expect(
        (yield* sql<{ count: number }>`SELECT count(*) AS count FROM ambient_work`)[0]?.count,
      ).toBe(1);
    }).pipe(Effect.provide(test.layer));
  },
);
