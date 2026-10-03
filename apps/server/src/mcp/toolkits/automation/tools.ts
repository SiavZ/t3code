import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import {
  ScheduledWorkCreateInput,
  ScheduledWorkReadInput,
  ScheduledWorkRecord,
  ScheduledWorkError,
} from "../../../../../..//packages/contracts/src/scheduledWork.ts";
import {
  BackgroundJobStartInput,
  BackgroundJobReadInput,
  BackgroundJobOutputInput,
  BackgroundJobWaitInput,
  BackgroundJobRecord,
  BackgroundJobOutput,
  BackgroundJobError,
} from "../../../../../..//packages/contracts/src/backgroundJobs.ts";
import { UnattendedGrant } from "../../../../../..//packages/contracts/src/unattendedGrants.ts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import * as ScheduledWork from "../../../orchestration/ScheduledWork.ts";
import * as BackgroundJobs from "../../../background/BackgroundJobs.ts";
import * as UnattendedGrants from "../../../orchestration/UnattendedGrants.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const { callerThreadId: _scheduleCaller, ...scheduleFields } = ScheduledWorkCreateInput.fields;
const { callerThreadId: _scheduleReadCaller, ...scheduleReadFields } =
  ScheduledWorkReadInput.fields;
const { callerThreadId: _jobCaller, ...jobFields } = BackgroundJobStartInput.fields;
const { callerThreadId: _jobReadCaller, ...jobReadFields } = BackgroundJobReadInput.fields;
const { callerThreadId: _outputCaller, ...outputFields } = BackgroundJobOutputInput.fields;
const { callerThreadId: _waitCaller, ...waitFields } = BackgroundJobWaitInput.fields;
const scheduleDependencies = [
  McpInvocationContext.McpInvocationContext,
  ScheduledWork.ScheduledWork,
];
const jobDependencies = [McpInvocationContext.McpInvocationContext, BackgroundJobs.BackgroundJobs];
const scheduleFailure = Schema.Union([McpCapabilityUnavailableError, ScheduledWorkError]);
const jobFailure = Schema.Union([McpCapabilityUnavailableError, BackgroundJobError]);

export const AutomationToolkit = Toolkit.make(
  Tool.make("schedule_create", {
    description:
      "Persist a one-shot resume or owned spawn. Requires an existing explicit human unattended grant. Cannot mint consent. Specify exactly one dueAt or delayMs, reuse id on retries. Acceptance is not completion.",
    parameters: Schema.Struct(scheduleFields),
    success: ScheduledWorkRecord,
    failure: scheduleFailure,
    dependencies: scheduleDependencies,
  }),
  Tool.make("schedule_list", {
    description: "List this thread's durable scheduled activations and truthful execution states.",
    success: Schema.Array(ScheduledWorkRecord),
    failure: scheduleFailure,
    dependencies: scheduleDependencies,
  }),
  Tool.make("schedule_get", {
    description: "Inspect one owned schedule.",
    parameters: Schema.Struct(scheduleReadFields),
    success: ScheduledWorkRecord,
    failure: scheduleFailure,
    dependencies: scheduleDependencies,
  }),
  Tool.make("schedule_cancel", {
    description:
      "Cancel an owned schedule, targeting only its exact activation. Stopping remains pending until acknowledged.",
    parameters: Schema.Struct(scheduleReadFields),
    success: ScheduledWorkRecord,
    failure: scheduleFailure,
    dependencies: scheduleDependencies,
  }),
  Tool.make("unattended_grants_list", {
    description:
      "Inspect existing explicit human consent and effective ceilings. This tool cannot create or elevate a grant.",
    success: Schema.Array(UnattendedGrant),
    failure: scheduleFailure,
    dependencies: [McpInvocationContext.McpInvocationContext, UnattendedGrants.UnattendedGrants],
  }),
  Tool.make("background_job_start", {
    description:
      "Execute a noninteractive host process in this thread's resolved project workspace, with explicit host-job consent. Streams bounded stdout/stderr, parses T3_PROGRESS JSON, and kills only its captured process on timeout/cancel. Never returns native provider background status. Shell execution requires explicitly choosing a shell executable and its arguments.",
    parameters: Schema.Struct(jobFields),
    success: BackgroundJobRecord,
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_list", {
    description: "List scoped host jobs, including terminal jobs retained across host restart.",
    success: Schema.Array(BackgroundJobRecord),
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_get", {
    description: "Inspect this thread's captured-process job.",
    parameters: Schema.Struct(jobReadFields),
    success: BackgroundJobRecord,
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_output", {
    description:
      "Read bounded stdout/stderr using a byte cursor. Truncation is explicit, output is retained in host SQLite until cleanup.",
    parameters: Schema.Struct(outputFields),
    success: BackgroundJobOutput,
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_cancel", {
    description:
      "Cancel only this job's captured process. Cancellation does not use PID/name matching.",
    parameters: Schema.Struct(jobReadFields),
    success: BackgroundJobRecord,
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_wait", {
    description:
      "Event-driven bounded wait for a terminal process state, subscribe-before-snapshot.",
    parameters: Schema.Struct(waitFields),
    success: Schema.Struct({ timedOut: Schema.Boolean, job: BackgroundJobRecord }),
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_subscribe", {
    description:
      "Persist terminal notify/wake preferences. Wake uses granted idle scheduling, not an injection into live foreground work.",
    parameters: Schema.Struct({ ...jobReadFields, notify: Schema.Boolean, wake: Schema.Boolean }),
    success: BackgroundJobRecord,
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
  Tool.make("background_job_cleanup", {
    description:
      "Delete only an owned terminal job and its retained bounded output. Live jobs reject cleanup.",
    parameters: Schema.Struct(jobReadFields),
    success: Schema.Void,
    failure: jobFailure,
    dependencies: jobDependencies,
  }),
);
