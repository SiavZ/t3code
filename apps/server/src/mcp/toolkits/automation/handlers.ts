import * as Effect from "effect/Effect";
import * as ScheduledWork from "../../../orchestration/ScheduledWork.ts";
import * as BackgroundJobs from "../../../background/BackgroundJobs.ts";
import * as UnattendedGrants from "../../../orchestration/UnattendedGrants.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { AutomationToolkit } from "./tools.ts";

const make = Effect.gen(function* () {
  const scheduled = yield* ScheduledWork.ScheduledWork;
  const jobs = yield* BackgroundJobs.BackgroundJobs;
  const grants = yield* UnattendedGrants.UnattendedGrants;
  return AutomationToolkit.of({
    schedule_create: Effect.fn("AutomationToolkit.create")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("automation");
      return yield* scheduled.create(
        { ...input, callerThreadId: caller.threadId },
        { mcpCapabilityCeiling: [...caller.capabilities] },
      );
    }),
    schedule_list: Effect.fn("AutomationToolkit.list")(function* () {
      const caller = yield* McpInvocationContext.requireMcpCapability("automation");
      return yield* scheduled.list({ callerThreadId: caller.threadId });
    }),
    schedule_get: Effect.fn("AutomationToolkit.get")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("automation");
      return yield* scheduled.get({ ...input, callerThreadId: caller.threadId });
    }),
    schedule_cancel: Effect.fn("AutomationToolkit.cancel")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("automation");
      return yield* scheduled.cancel({ ...input, callerThreadId: caller.threadId });
    }),
    unattended_grants_list: Effect.fn("AutomationToolkit.grants")(function* () {
      const caller = yield* McpInvocationContext.requireMcpCapability("automation");
      return yield* grants.list({ callerThreadId: caller.threadId });
    }),
    background_job_start: Effect.fn("AutomationToolkit.jobStart")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.start({ ...input, callerThreadId: caller.threadId });
    }),
    background_job_list: Effect.fn("AutomationToolkit.jobList")(function* () {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.list({ callerThreadId: caller.threadId });
    }),
    background_job_get: Effect.fn("AutomationToolkit.jobGet")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.get({ ...input, callerThreadId: caller.threadId });
    }),
    background_job_output: Effect.fn("AutomationToolkit.jobOutput")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.output({ ...input, callerThreadId: caller.threadId });
    }),
    background_job_cancel: Effect.fn("AutomationToolkit.jobCancel")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.cancel({ ...input, callerThreadId: caller.threadId });
    }),
    background_job_wait: Effect.fn("AutomationToolkit.jobWait")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.wait({ ...input, callerThreadId: caller.threadId });
    }),
    background_job_subscribe: Effect.fn("AutomationToolkit.jobSubscribe")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.subscribe({ ...input, callerThreadId: caller.threadId });
    }),
    background_job_cleanup: Effect.fn("AutomationToolkit.jobCleanup")(function* (input) {
      const caller = yield* McpInvocationContext.requireMcpCapability("background-jobs");
      return yield* jobs.cleanup({ ...input, callerThreadId: caller.threadId });
    }),
  });
});
export const AutomationToolkitHandlersLive = AutomationToolkit.toLayer(make);
