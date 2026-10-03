import type {
  EnvironmentId,
  OrchestrationThreadShell,
  WorkerGetResult,
  WorkerListResult,
  WorkerSummary,
} from "@t3tools/contracts";

export const OWNED_WORKERS_LIST_LIMIT = 200;
export const OWNED_WORKER_WAIT_MS = 30_000;
export const OWNED_WORKER_RESULT_LIMIT = 2_000;

/** A shell-derived invalidation key, excluding tokens, timestamps and unrelated threads. */
export function ownedWorkersRefreshKey(
  shells: readonly (OrchestrationThreadShell & { readonly environmentId: EnvironmentId })[],
  environmentId: EnvironmentId,
  ownerThreadId: string,
): string {
  const caller = shells.find(
    (thread) => thread.environmentId === environmentId && thread.id === ownerThreadId,
  );
  const rootThreadId = caller?.worker?.rootThreadId ?? ownerThreadId;
  const workers = shells.filter(
    (thread) =>
      thread.environmentId === environmentId &&
      thread.worker &&
      (caller?.worker
        ? thread.worker.ownerThreadId === ownerThreadId
        : thread.worker.rootThreadId === rootThreadId),
  );
  return JSON.stringify(
    workers
      .map((thread) => [
        thread.id,
        thread.worker?.stopRequestedAt,
        thread.session?.status,
        thread.session?.activeTurnId,
        thread.latestTurn?.turnId,
        thread.latestTurn?.state,
        thread.hasPendingApprovals,
        thread.hasPendingUserInput,
        thread.backgroundLiveness ?? null,
        thread.archivedAt,
      ])
      .sort(([a], [b]) => String(a).localeCompare(String(b))),
  );
}

export function boundedOwnedWorkers(result: WorkerListResult) {
  return {
    workers: result.workers.slice(0, OWNED_WORKERS_LIST_LIMIT),
    truncated: result.truncated || result.workers.length > OWNED_WORKERS_LIST_LIMIT,
  };
}

export function canStopOwnedWorker(worker: Pick<WorkerSummary, "status">): boolean {
  return worker.status === "pending" || worker.status === "running" || worker.status === "waiting";
}

/** Get supplies one bounded turn; retain only a small plain-text result preview in the UI. */
export function ownedWorkerResultPreview(result: WorkerGetResult): string | null {
  const id = result.worker.result?.assistantMessageId;
  const message = id ? result.detail.thread.messages.find((entry) => entry.id === id) : undefined;
  const text = message?.text?.trim();
  if (!text) return null;
  return text.length > OWNED_WORKER_RESULT_LIMIT
    ? `${text.slice(0, OWNED_WORKER_RESULT_LIMIT)}…`
    : text;
}
