import {
  McpCapabilityUnavailableError,
  WorkerSpawnInput,
  WorkerGetInput,
  WorkerSendInput,
  WorkerStopInput,
  WorkerWaitInput,
  WorkerOperationError,
  WorkerOperationResult,
  WorkerListResult,
  WorkerGetResult,
  WorkerWaitResult,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";

import * as OwnedWorkers from "../../../orchestration/OwnedWorkers.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const dependencies = [McpInvocationContext.McpInvocationContext, OwnedWorkers.OwnedWorkers];
const failure = Schema.Union([McpCapabilityUnavailableError, WorkerOperationError]);
const { callerThreadId: _spawnCaller, ...spawnFields } = WorkerSpawnInput.fields;
const { callerThreadId: _getCaller, ...getFields } = WorkerGetInput.fields;
const { callerThreadId: _sendCaller, ...sendFields } = WorkerSendInput.fields;
const { callerThreadId: _stopCaller, ...stopFields } = WorkerStopInput.fields;
const { callerThreadId: _waitCaller, ...waitFields } = WorkerWaitInput.fields;

export const WorkersToolkit = Toolkit.make(
  Tool.make("workers_spawn", {
    description:
      "Start a durable worker owned by this thread. Choose a provider instance and model. Reuse commandId for retries. At most 4 active workers per root, 16 per environment, and depth 2.",
    parameters: Schema.Struct(spawnFields),
    success: WorkerOperationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("workers_list", {
    description:
      "List owned workers and their current status. Use workers_get to retrieve bounded conversation detail and final results.",
    success: WorkerListResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("workers_get", {
    description:
      "Get one owned worker's status and bounded recent conversation, including its completed result. turnLimit is at most 20.",
    parameters: Schema.Struct(getFields),
    success: WorkerGetResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("workers_send", {
    description:
      "Send a follow-up to an idle owned worker. Busy workers reject follow-ups. An explicit follow-up can resume a stopped worker. Reuse commandId for retries.",
    parameters: Schema.Struct(sendFields),
    success: WorkerOperationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("workers_stop", {
    description:
      "Stop an owned descendant's current execution and cancel its queued startup. The durable worker remains available for a later explicit follow-up. Reuse commandId for retries.",
    parameters: Schema.Struct(stopFields),
    success: WorkerOperationResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Destructive, true)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
  Tool.make("workers_wait", {
    description:
      "Wait for any or all selected owned workers to settle, or for timeoutMs (at most 600000). Returns status summaries, not conversation detail.",
    parameters: Schema.Struct(waitFields),
    success: WorkerWaitResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Destructive, false)
    .annotate(Tool.Idempotent, true)
    .annotate(Tool.OpenWorld, false),
);
