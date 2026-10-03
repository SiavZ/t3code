import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { useNavigate } from "@tanstack/react-router";
import { scopeThreadRef } from "@t3tools/client-runtime/environment";
import { createOwnedWorkersEnvironmentAtoms } from "@t3tools/client-runtime/state/owned-workers";
import {
  boundedOwnedWorkers,
  canStopOwnedWorker,
  OWNED_WORKER_WAIT_MS,
  ownedWorkerResultPreview,
} from "@t3tools/client-runtime/workers/model";
import { CommandId, ThreadId, type EnvironmentId, type WorkerSummary } from "@t3tools/contracts";
import * as Option from "effect/Option";
import { AsyncResult } from "effect/unstable/reactivity";
import { useContext, useState } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { randomUUID } from "../../lib/utils";
import { environmentThreadShells } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { buildThreadRouteParams } from "../../threadRoutes";
import { Button } from "../ui/button";

const workers = createOwnedWorkersEnvironmentAtoms(
  connectionAtomRuntime,
  environmentThreadShells.threadShellsAtom,
);
type Props = {
  environmentId: EnvironmentId;
  ownerThreadId: string;
  projectId: string;
  connected: boolean;
};

function WorkerResult({ worker, ...props }: Props & { worker: WorkerSummary }) {
  const result = useAtomValue(
    workers.get({
      environmentId: props.environmentId,
      input: {
        callerThreadId: ThreadId.make(props.ownerThreadId),
        workerThreadId: worker.threadId,
        turnLimit: 1,
      },
    }),
  );
  const value = Option.getOrNull(AsyncResult.value(result));
  if (result._tag === "Failure")
    return (
      <p role="alert">Could not load the worker result. Open its thread for the full history.</p>
    );
  return (
    <p className="whitespace-pre-wrap break-words text-sm text-muted-foreground">
      {value
        ? (ownedWorkerResultPreview(value) ??
          "No completed result yet. Open the thread to see its work.")
        : "Loading result…"}
    </p>
  );
}

function WorkerRow({ worker, ...props }: Props & { worker: WorkerSummary }) {
  const registry = useContext(RegistryContext);
  const navigate = useNavigate();
  const stop = useAtomCommand(workers.stop, { reportFailure: false });
  const wait = useAtomCommand(workers.wait, { reportFailure: false });
  const [pending, setPending] = useState<"stop" | "wait" | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [showResult, setShowResult] = useState(false);
  const query = workers.list({
    environmentId: props.environmentId,
    input: { callerThreadId: ThreadId.make(props.ownerThreadId) },
  });
  const act = async (action: "stop" | "wait") => {
    if (!props.connected || pending) return;
    setPending(action);
    setNotice(null);
    try {
      if (action === "stop") {
        const outcome = await stop({
          environmentId: props.environmentId,
          input: {
            callerThreadId: ThreadId.make(props.ownerThreadId),
            workerThreadId: worker.threadId,
            commandId: CommandId.make(randomUUID()),
          },
        });
        setNotice(
          outcome._tag === "Failure"
            ? "Could not request stop."
            : "Stop requested. Status updates when the provider stops.",
        );
      } else {
        const outcome = await wait({
          environmentId: props.environmentId,
          input: {
            callerThreadId: ThreadId.make(props.ownerThreadId),
            workerThreadIds: [worker.threadId],
            mode: "all",
            timeoutMs: OWNED_WORKER_WAIT_MS,
          },
        });
        setNotice(
          outcome._tag === "Failure"
            ? "Could not wait for this worker."
            : outcome.value.timedOut
              ? "Still active after 30 seconds. You can wait again."
              : "Worker has settled.",
        );
      }
      registry.refresh(query);
    } finally {
      setPending(null);
    }
  };
  return (
    <li className="space-y-2 border-b py-3 last:border-0">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <div>
          <p className="text-sm font-medium">{worker.label}</p>
          <p className="text-xs text-muted-foreground">
            {worker.status} · {worker.modelSelection.model}
            {worker.depth > 1 ? " · Nested worker" : ""}
          </p>
        </div>
        <div className="flex flex-wrap gap-1">
          <Button
            size="sm"
            variant="outline"
            disabled={!props.connected}
            onClick={() =>
              void navigate({
                to: "/$environmentId/$threadId",
                params: buildThreadRouteParams(
                  scopeThreadRef(props.environmentId, worker.threadId),
                ),
              })
            }
          >
            Open thread
          </Button>
          <Button
            size="sm"
            variant="ghost"
            disabled={!props.connected}
            aria-expanded={showResult}
            onClick={() => setShowResult(!showResult)}
          >
            {showResult ? "Hide result" : "Result"}
          </Button>
          {(canStopOwnedWorker(worker) || worker.status === "stopping") && (
            <>
              <Button
                size="sm"
                variant="outline"
                disabled={!props.connected || pending !== null}
                onClick={() => void act("wait")}
              >
                {pending === "wait" ? "Waiting (up to 30s)…" : "Wait"}
              </Button>
              <Button
                size="sm"
                variant="destructive-outline"
                disabled={!props.connected || pending !== null || !canStopOwnedWorker(worker)}
                onClick={() => void act("stop")}
              >
                {pending === "stop" ? "Requesting stop…" : "Stop"}
              </Button>
            </>
          )}
        </div>
      </div>
      {notice && (
        <p role="status" className="text-xs text-muted-foreground">
          {notice}
        </p>
      )}
      {showResult && <WorkerResult {...props} worker={worker} />}
    </li>
  );
}

function OpenWorkers(props: Props) {
  const registry = useContext(RegistryContext);
  const query = workers.list({
    environmentId: props.environmentId,
    input: { callerThreadId: ThreadId.make(props.ownerThreadId) },
  });
  const result = useAtomValue(query);
  const value = Option.getOrNull(AsyncResult.value(result));
  const bounded = value ? boundedOwnedWorkers(value) : null;
  return (
    <div className="max-h-80 overflow-auto px-3 pb-3">
      <Button
        size="sm"
        variant="ghost"
        disabled={!props.connected || result.waiting}
        onClick={() => registry.refresh(query)}
      >
        Refresh workers
      </Button>
      {!props.connected && (
        <p role="status" className="text-sm text-muted-foreground">
          Reconnect to update workers or use their controls.
        </p>
      )}
      {result._tag === "Failure" && (
        <p role="alert">Could not load workers. Reconnect or refresh to retry.</p>
      )}
      {!value && result._tag !== "Failure" && (
        <p className="text-sm text-muted-foreground">Loading workers…</p>
      )}
      {bounded?.workers.length === 0 && (
        <p className="text-sm text-muted-foreground">No owned workers in this thread.</p>
      )}
      {bounded?.truncated && (
        <p role="status" className="text-sm text-muted-foreground">
          Showing the first 200 workers. Older workers remain available in their ordinary threads.
        </p>
      )}
      <ul>
        {bounded?.workers.map((worker) => (
          <WorkerRow key={worker.threadId} {...props} worker={worker} />
        ))}
      </ul>
    </div>
  );
}

/** Collapsed by default; list/get subscriptions exist only while their views are open. */
export function OwnedWorkersView(props: Props) {
  const [open, setOpen] = useState(false);
  return (
    <section aria-label="Owned workers" className="shrink-0 border-b">
      <Button variant="ghost" aria-expanded={open} onClick={() => setOpen(!open)}>
        Workers
      </Button>
      {open && <OpenWorkers {...props} />}
    </section>
  );
}
