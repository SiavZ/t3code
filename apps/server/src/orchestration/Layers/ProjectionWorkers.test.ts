import { assert, it } from "@effect/vitest";
import { CommandId, ThreadId, type ThreadWorkerMetadata } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { SqlitePersistenceMemory } from "../../persistence/Layers/Sqlite.ts";
import { RepositoryIdentityResolver } from "../../project/RepositoryIdentityResolver.ts";
import * as ThreadBackgroundLiveness from "../ThreadBackgroundLiveness.ts";
import * as ThreadPlanProgress from "../ThreadPlanProgress.ts";
import { ProjectionSnapshotQuery } from "../Services/ProjectionSnapshotQuery.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./ProjectionSnapshotQuery.ts";

const NOW = "2026-10-02T17:00:00.000Z";
const ROOT = ThreadId.make("root");
const metadata: ThreadWorkerMetadata = {
  ownerThreadId: ROOT,
  rootThreadId: ROOT,
  depth: 1,
  spawnCommandId: CommandId.make("spawn"),
  spawnFingerprint: "fingerprint",
  label: "worker",
  runtimeModeCeiling: "approval-required",
  mcpCapabilityCeiling: ["workers"],
  stopRequestedAt: null,
  lastStopSequence: null,
};
const makeLayer = () =>
  OrchestrationProjectionSnapshotQueryLive.pipe(
    Layer.provideMerge(ThreadBackgroundLiveness.layer),
    Layer.provide(ThreadPlanProgress.layer),
    Layer.provide(
      Layer.succeed(RepositoryIdentityResolver, { resolve: () => Effect.succeed(null) }),
    ),
    Layer.provideMerge(SqlitePersistenceMemory),
  );
const insertWorker = (
  id: string,
  createdAt = NOW,
  worker = metadata,
  model = '{"instanceId":"codex","model":"gpt-5.4"}',
) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO projection_threads
      (thread_id, project_id, title, model_selection_json, runtime_mode, interaction_mode, worker_json, created_at, updated_at)
      VALUES (${id}, 'project', ${id}, ${model}, 'approval-required', 'default', ${JSON.stringify(worker)}, ${createdAt}, ${createdAt})`;
  });

it.effect("admission never hydrates historical completed worker state", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const query = yield* ProjectionSnapshotQuery;
    for (let i = 0; i < 80; i += 1)
      yield* insertWorker(`historical-${i}`, NOW, metadata, "invalid-json");
    yield* insertWorker("pending");
    yield* sql`INSERT INTO projection_turns (thread_id, pending_message_id, state, requested_at, checkpoint_files_json)
      VALUES ('pending', 'pending-message', 'pending', ${NOW}, '[]')`;
    const states = yield* query.getWorkerAdmissionStates([]);
    assert.deepEqual(
      states.map(({ thread }) => thread.id),
      [ThreadId.make("pending")],
    );
  }).pipe(Effect.provide(makeLayer())),
);

it.effect(
  "admission includes inactive durable stop intents until native stop is acknowledged",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const query = yield* ProjectionSnapshotQuery;
      const stopping = { ...metadata, stopRequestedAt: NOW, lastStopSequence: 7 };
      for (const status of ["ready", "interrupted", "error", "stopped"]) {
        yield* insertWorker(status, NOW, stopping);
        yield* sql`INSERT INTO projection_thread_sessions (thread_id, status, provider_name, active_turn_id, last_error, updated_at)
        VALUES (${status}, ${status}, 'codex', NULL, NULL, ${NOW})`;
      }
      yield* insertWorker("no-session", NOW, stopping);
      assert.deepEqual(
        (yield* query.getWorkerAdmissionStates([])).map(({ thread }) => thread.id).sort(),
        ["error", "interrupted", "no-session", "ready"],
      );
    }).pipe(Effect.provide(makeLayer())),
);

it.effect(
  "bounded recent worker lists keep old native-live workers first and filter owned children",
  () =>
    Effect.gen(function* () {
      const query = yield* ProjectionSnapshotQuery;
      const background = yield* ThreadBackgroundLiveness.ThreadBackgroundLivenessService;
      for (let i = 0; i < 210; i += 1) yield* insertWorker(`history-${i}`);
      yield* insertWorker("old-native", "2026-01-01T00:00:00.000Z");
      yield* insertWorker("nested", NOW, {
        ...metadata,
        ownerThreadId: ThreadId.make("old-native"),
        depth: 2,
      });
      background.recordTaskLiveness({
        threadId: "old-native",
        taskId: "native-task",
        taskType: "agent",
        status: "running",
        kind: "started",
      });
      const states = yield* query.listWorkerStates({ rootThreadId: ROOT });
      assert.strictEqual(states.length, 201);
      assert.strictEqual(states[0]?.thread.id, "old-native");
      assert.deepEqual(
        (yield* query.listWorkerStates({
          rootThreadId: ROOT,
          ownerThreadId: ThreadId.make("old-native"),
        })).map(({ thread }) => thread.id),
        [ThreadId.make("nested")],
      );
    }).pipe(Effect.provide(makeLayer())),
);

it.effect("worker result snapshots skip lifetime plans and checkpoint files at SQL read time", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const query = yield* ProjectionSnapshotQuery;
    yield* insertWorker("worker");
    yield* sql`INSERT INTO projection_thread_proposed_plans (plan_id, thread_id, plan_markdown, created_at, updated_at)
      VALUES ('plan', 'worker', 'Lifetime plan', 'invalid-date', 'invalid-date')`;
    yield* sql`INSERT INTO projection_turns
      (thread_id, turn_id, state, requested_at, completed_at, checkpoint_turn_count, checkpoint_ref, checkpoint_status, checkpoint_files_json)
      VALUES ('worker', 'turn', 'completed', ${NOW}, ${NOW}, 1, 'refs/t3/checkpoint', 'ready', 'invalid-json')`;
    const snapshot = yield* query.getThreadDetailSnapshot(
      ThreadId.make("worker"),
      { turnLimit: 1 },
      { includeHistoryArtifacts: false },
    );
    assert.isTrue(Option.isSome(snapshot));
    if (Option.isSome(snapshot)) {
      assert.deepEqual(snapshot.value.thread.proposedPlans, []);
      assert.deepEqual(snapshot.value.thread.checkpoints, []);
    }
    const defaultRead = yield* query
      .getThreadDetailSnapshot(ThreadId.make("worker"), { turnLimit: 1 })
      .pipe(Effect.exit);
    assert.strictEqual(defaultRead._tag, "Failure");
  }).pipe(Effect.provide(makeLayer())),
);

it.effect(
  "archived worker details remain available but deleted worker retry metadata remains read-only",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const query = yield* ProjectionSnapshotQuery;
      yield* insertWorker("worker");
      yield* sql`UPDATE projection_threads SET archived_at = ${NOW} WHERE thread_id = 'worker'`;
      yield* sql`INSERT INTO projection_turns (thread_id, turn_id, state, requested_at, completed_at, checkpoint_files_json)
        VALUES ('worker', 'completed-turn', 'completed', ${NOW}, ${NOW}, '[]')`;
      yield* sql`UPDATE projection_threads SET latest_turn_id = 'completed-turn' WHERE thread_id = 'worker'`;
      assert.isTrue(Option.isSome(yield* query.getWorkerState(ThreadId.make("worker"))));
      assert.strictEqual(
        Option.getOrThrow(yield* query.getWorkerState(ThreadId.make("worker"))).thread.latestTurn
          ?.state,
        "completed",
      );
      assert.isTrue(
        Option.isSome(
          yield* query.getThreadDetailSnapshot(ThreadId.make("worker"), { turnLimit: 1 }),
        ),
      );
      yield* sql`UPDATE projection_threads SET worker_json = NULL WHERE thread_id = 'worker'`;
      assert.isTrue(
        Option.isNone(
          yield* query.getThreadDetailSnapshot(ThreadId.make("worker"), { turnLimit: 1 }),
        ),
      );
      yield* sql`UPDATE projection_threads SET worker_json = ${JSON.stringify(metadata)}, deleted_at = ${NOW} WHERE thread_id = 'worker'`;
      assert.isTrue(Option.isNone(yield* query.getWorkerState(ThreadId.make("worker"))));
      assert.deepEqual(
        Option.getOrNull(yield* query.getWorkerSpawnMetadata(ThreadId.make("worker"))),
        metadata,
      );
    }).pipe(Effect.provide(makeLayer())),
);

it.effect(
  "compact worker get bounds abandoned prompts and activity hydration even without durable turns",
  () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const query = yield* ProjectionSnapshotQuery;
      yield* insertWorker("worker");
      for (let i = 0; i < 210; i += 1) {
        const suffix = String(i).padStart(3, "0");
        yield* sql`INSERT INTO projection_thread_messages (message_id, thread_id, role, text, attachments_json, is_streaming, created_at, updated_at)
        VALUES (${`message-${suffix}`}, 'worker', 'user', 'Canceled prompt', '[]', 0, ${NOW}, ${NOW})`;
        yield* sql`INSERT INTO projection_thread_activities (activity_id, thread_id, tone, kind, summary, payload_json, created_at)
        VALUES (${`activity-${suffix}`}, 'worker', 'info', 'provider.progress', 'Progress', ${i < 100 ? "invalid-json" : "{}"}, ${NOW})`;
      }
      const snapshot = Option.getOrThrow(
        yield* query.getThreadDetailSnapshot(
          ThreadId.make("worker"),
          { turnLimit: 1 },
          { includeHistoryArtifacts: false, boundedConversation: true },
        ),
      );
      assert.strictEqual(snapshot.thread.messages.length, 1);
      assert.strictEqual(snapshot.thread.messages[0]?.id, "message-209");
      assert.strictEqual(snapshot.thread.activities.length, 100);
    }).pipe(Effect.provide(makeLayer())),
);
