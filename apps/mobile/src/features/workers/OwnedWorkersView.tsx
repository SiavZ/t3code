import { RegistryContext, useAtomValue } from "@effect/atom-react";
import { useNavigation } from "@react-navigation/native";
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
import { ScrollView, View } from "react-native";
import { connectionAtomRuntime } from "../../connection/runtime";
import { environmentThreadShells } from "../../state/threads";
import { useAtomCommand } from "../../state/use-atom-command";
import { uuidv4 } from "../../lib/uuid";
import { ControlPill } from "../../components/ControlPill";
import { AppText } from "../../components/AppText";

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
      <AppText accessibilityRole="alert">
        Could not load the worker result. Open its thread for the full history.
      </AppText>
    );
  return (
    <AppText className="text-sm text-foreground-muted">
      {value
        ? (ownedWorkerResultPreview(value) ??
          "No completed result yet. Open the thread to see its work.")
        : "Loading result…"}
    </AppText>
  );
}

function WorkerRow({ worker, ...props }: Props & { worker: WorkerSummary }) {
  const registry = useContext(RegistryContext);
  const navigation = useNavigation();
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
            commandId: CommandId.make(uuidv4()),
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
    <View className="gap-2 border-b border-border-subtle py-3">
      <AppText className="text-sm font-t3-medium text-foreground">{worker.label}</AppText>
      <AppText className="text-xs text-foreground-muted">
        {worker.status} · {worker.modelSelection.model}
        {worker.depth > 1 ? " · Nested worker" : ""}
      </AppText>
      <View className="flex-row flex-wrap gap-2">
        <ControlPill
          variant="pill"
          label="Open thread"
          disabled={!props.connected}
          onPress={() =>
            navigation.navigate("Thread", {
              environmentId: String(props.environmentId),
              threadId: String(worker.threadId),
            })
          }
        />
        <ControlPill
          variant="pill"
          label={showResult ? "Hide result" : "Result"}
          disabled={!props.connected}
          onPress={() => setShowResult(!showResult)}
        />
        {(canStopOwnedWorker(worker) || worker.status === "stopping") && (
          <>
            <ControlPill
              variant="pill"
              label={pending === "wait" ? "Waiting (up to 30s)…" : "Wait"}
              disabled={!props.connected || pending !== null}
              onPress={() => void act("wait")}
            />
            <ControlPill
              variant="danger"
              label={pending === "stop" ? "Requesting stop…" : "Stop"}
              disabled={!props.connected || pending !== null || !canStopOwnedWorker(worker)}
              onPress={() => void act("stop")}
            />
          </>
        )}
      </View>
      {notice && (
        <AppText accessibilityLiveRegion="polite" className="text-xs text-foreground-muted">
          {notice}
        </AppText>
      )}
      {showResult && <WorkerResult {...props} worker={worker} />}
    </View>
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
    <ScrollView style={{ maxHeight: 320 }} nestedScrollEnabled>
      <View className="gap-2 px-3 pb-3">
        <ControlPill
          variant="pill"
          label="Refresh workers"
          disabled={!props.connected || result.waiting}
          onPress={() => registry.refresh(query)}
        />
        {!props.connected && (
          <AppText className="text-sm text-foreground-muted">
            Reconnect to update workers or use their controls.
          </AppText>
        )}
        {result._tag === "Failure" && (
          <AppText accessibilityRole="alert">
            Could not load workers. Reconnect or refresh to retry.
          </AppText>
        )}
        {!value && result._tag !== "Failure" && (
          <AppText className="text-sm text-foreground-muted">Loading workers…</AppText>
        )}
        {bounded?.workers.length === 0 && (
          <AppText className="text-sm text-foreground-muted">
            No owned workers in this thread.
          </AppText>
        )}
        {bounded?.truncated && (
          <AppText className="text-sm text-foreground-muted">
            Showing the first 200 workers. Older workers remain available in their ordinary threads.
          </AppText>
        )}
        {bounded?.workers.map((worker) => (
          <WorkerRow key={worker.threadId} {...props} worker={worker} />
        ))}
      </View>
    </ScrollView>
  );
}

export function OwnedWorkersView(props: Props) {
  const [open, setOpen] = useState(false);
  return (
    <View>
      <ControlPill
        variant="pill"
        label={open ? "Hide workers" : "Workers"}
        accessibilityLabel={open ? "Collapse workers" : "Expand workers"}
        onPress={() => setOpen(!open)}
      />
      {open && <OpenWorkers {...props} />}
    </View>
  );
}
