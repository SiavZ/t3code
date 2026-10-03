import {
  CommandId,
  EnvironmentId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  TurnId,
  type WorkerSummary,
  type WorkerGetResult,
} from "@t3tools/contracts";
import { Atom, AtomRegistry } from "effect/unstable/reactivity";
import { describe, expect, it } from "vite-plus/test";
import type { EnvironmentThreadShell } from "../state/models.ts";
import {
  boundedOwnedWorkers,
  canStopOwnedWorker,
  OWNED_WORKER_RESULT_LIMIT,
  ownedWorkerResultPreview,
  ownedWorkersRefreshKey,
} from "./model.ts";

const env = EnvironmentId.make("env");
const otherEnv = EnvironmentId.make("other");
const ownerId = ThreadId.make("owner");
const modelSelection = { instanceId: ProviderInstanceId.make("codex"), model: "model" };
function shell(id = "worker", owner = ownerId): EnvironmentThreadShell {
  return {
    environmentId: env,
    id: ThreadId.make(id),
    projectId: ProjectId.make("project"),
    title: "Worker",
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    branch: null,
    worktreePath: null,
    pullRequests: [],
    latestTurn: null,
    createdAt: "2026-10-03T00:00:00Z",
    updatedAt: "2026-10-03T00:00:00Z",
    archivedAt: null,
    settledOverride: null,
    settledAt: null,
    session: null,
    latestUserMessageAt: null,
    hasPendingApprovals: false,
    hasPendingUserInput: false,
    hasActionableProposedPlan: false,
    worker: {
      ownerThreadId: owner,
      rootThreadId: ownerId,
      depth: 1,
      spawnCommandId: CommandId.make(id),
      spawnFingerprint: id,
      label: id,
      runtimeModeCeiling: "full-access",
      mcpCapabilityCeiling: [],
      stopRequestedAt: null,
      lastStopSequence: null,
    },
  };
}
function summary(id = "worker"): WorkerSummary {
  return {
    threadId: ThreadId.make(id),
    ownerThreadId: ownerId,
    rootThreadId: ownerId,
    label: id,
    depth: 1,
    modelSelection,
    runtimeMode: "full-access",
    status: "running",
    pendingMessageId: null,
    turnId: null,
    result: null,
    createdAt: "2026-10-03T00:00:00Z",
    updatedAt: "2026-10-03T00:00:00Z",
  };
}

describe("bounded owned workers", () => {
  it("honors server truncation and independently bounds an oversized list", () => {
    const values = Array.from({ length: 205 }, (_, index) => summary(String(index)));
    expect(boundedOwnedWorkers({ workers: values, truncated: false }).workers).toHaveLength(200);
    expect(boundedOwnedWorkers({ workers: values, truncated: false }).truncated).toBe(true);
    expect(boundedOwnedWorkers({ workers: [summary()], truncated: true }).truncated).toBe(true);
    expect(boundedOwnedWorkers({ workers: [], truncated: false })).toEqual({
      workers: [],
      truncated: false,
    });
    expect(values).toHaveLength(205);
  });
  it("refreshes on worker lifecycle events but not unrelated shells or token timestamps", () => {
    const worker = shell();
    const key = ownedWorkersRefreshKey([worker], env, ownerId);
    expect(
      ownedWorkersRefreshKey(
        [
          {
            ...worker,
            updatedAt: "2026-10-03T01:00:00Z",
            title: "Regenerated title",
            planProgress: { step: "New token", completedSteps: 1, totalSteps: 3 },
          },
        ],
        env,
        ownerId,
      ),
    ).toBe(key);
    expect(
      ownedWorkersRefreshKey(
        [worker, { ...shell("unrelated"), environmentId: otherEnv }],
        env,
        ownerId,
      ),
    ).toBe(key);
    for (const changed of [
      { ...worker, hasPendingApprovals: true },
      { ...worker, hasPendingUserInput: true },
      { ...worker, backgroundLiveness: "working" as const },
      {
        ...worker,
        latestTurn: {
          turnId: TurnId.make("turn"),
          state: "completed" as const,
          requestedAt: "2026-10-03T00:00:00Z",
          startedAt: null,
          completedAt: null,
          assistantMessageId: null,
        },
      },
      { ...worker, worker: { ...worker.worker!, stopRequestedAt: "2026-10-03T01:00:00Z" } },
    ]) {
      expect(ownedWorkersRefreshKey([changed], env, ownerId)).not.toBe(key);
    }
    expect(ownedWorkersRefreshKey([], env, ownerId)).not.toBe(key);
  });
  it("uses root descendants for a root caller and direct children for a worker caller", () => {
    const parent = shell("parent");
    const child = {
      ...shell("child", parent.id),
      worker: { ...shell("child", parent.id).worker!, depth: 2 },
    };
    const sibling = shell("sibling");
    expect(ownedWorkersRefreshKey([parent, child, sibling], env, parent.id)).toBe(
      ownedWorkersRefreshKey([parent, child], env, parent.id),
    );
    expect(ownedWorkersRefreshKey([parent, child, sibling], env, ownerId)).not.toBe(
      ownedWorkersRefreshKey([parent, sibling], env, ownerId),
    );
    expect(ownedWorkersRefreshKey([parent, child], env, ownerId)).toBe(
      ownedWorkersRefreshKey([child, parent], env, ownerId),
    );
  });
  it("does not emit an atom invalidation when only a worker token timestamp changes", () => {
    const registry = AtomRegistry.make();
    const source = Atom.make<readonly EnvironmentThreadShell[]>([shell()]);
    const signal = Atom.make((get) => ownedWorkersRefreshKey(get(source), env, ownerId));
    const observed: string[] = [];
    const unsubscribe = registry.subscribe(signal, (value) => observed.push(value));
    registry.get(signal);
    observed.length = 0;
    registry.set(source, [{ ...shell(), updatedAt: "2026-10-03T01:00:00Z" }]);
    expect(observed).toHaveLength(0);
    registry.set(source, [{ ...shell(), hasPendingUserInput: true }]);
    expect(observed).toHaveLength(1);
    unsubscribe();
    registry.dispose();
  });
  it("offers stop only while a worker can be interrupted", () => {
    for (const status of ["pending", "running", "waiting"] as const)
      expect(canStopOwnedWorker({ status })).toBe(true);
    for (const status of [
      "idle",
      "completed",
      "failed",
      "interrupted",
      "stopping",
      "stopped",
    ] as const)
      expect(canStopOwnedWorker({ status })).toBe(false);
  });
  it("previews only the completed result message and caps its text", () => {
    const worker = {
      ...summary(),
      status: "completed" as const,
      result: {
        assistantMessageId: MessageId.make("answer"),
        turnId: TurnId.make("turn"),
        completedAt: "2026-10-03T00:00:00Z",
      },
    };
    const result: WorkerGetResult = {
      worker,
      detail: {
        snapshotSequence: 1,
        thread: {
          ...shell(),
          deletedAt: null,
          proposedPlans: [],
          activities: [],
          checkpoints: [],
          messages: [
            {
              id: MessageId.make("unrelated"),
              text: "Not the result",
              role: "user",
              turnId: null,
              streaming: false,
              createdAt: "2026-10-03T00:00:00Z",
              updatedAt: "2026-10-03T00:00:00Z",
            },
            {
              id: MessageId.make("answer"),
              text: "x".repeat(3_000),
              role: "assistant",
              turnId: TurnId.make("turn"),
              streaming: false,
              createdAt: "2026-10-03T00:00:00Z",
              updatedAt: "2026-10-03T00:00:00Z",
            },
          ],
        },
      },
    };
    expect(ownedWorkerResultPreview(result)).toHaveLength(OWNED_WORKER_RESULT_LIMIT + 1);
    expect(ownedWorkerResultPreview({ ...result, worker: summary() })).toBeNull();
    expect(
      ownedWorkerResultPreview({
        ...result,
        detail: { ...result.detail, thread: { ...result.detail.thread, messages: [] } },
      }),
    ).toBeNull();
  });
});
