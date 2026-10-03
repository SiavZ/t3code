import { expect, it } from "@effect/vitest";
import { EnvironmentId, ProjectId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as BackgroundJobs from "../../../background/BackgroundJobs.ts";
import * as ScheduledWork from "../../../orchestration/ScheduledWork.ts";
import * as UnattendedGrants from "../../../orchestration/UnattendedGrants.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import migrate from "../../../persistence/Migrations/060_BackgroundJobs.ts";
import { AutomationToolkit } from "./tools.ts";
import { AutomationToolkitHandlersLive } from "./handlers.ts";

const owner = ThreadId.make("mcp-job-owner");
const invocation = (
  capabilities: ReadonlyArray<McpInvocationContext.McpCapability>,
): McpInvocationContext.McpInvocationScope => ({
  environmentId: EnvironmentId.make("test"),
  threadId: owner,
  providerSessionId: "test-session",
  providerInstanceId: ProviderInstanceId.make("codex"),
  capabilities: new Set(capabilities),
  issuedAt: 1,
});
const base = Layer.mergeAll(
  NodeSqliteClient.layer({ filename: ":memory:" }),
  NodeServices.layer,
  Layer.succeed(BackgroundJobs.BackgroundJobAuthority, {
    authorize: () => Effect.succeed({ projectId: ProjectId.make("project"), cwd: process.cwd() }),
    notify: () => Effect.void,
  }),
  Layer.mock(ScheduledWork.ScheduledWork)({}),
  Layer.mock(UnattendedGrants.UnattendedGrants)({}),
);
const live = BackgroundJobs.layer.pipe(Layer.provideMerge(base));

it.live(
  "authenticated MCP executes a real scoped host process and caller JSON cannot spoof its owner",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        yield* migrate;
        const toolkit = yield* AutomationToolkit.pipe(
          Effect.provide(AutomationToolkitHandlersLive),
        );
        const maliciousInput = {
          id: "mcp-job",
          callerThreadId: "spoofed",
          command: process.execPath,
          args: ["-e", 'process.stdout.write("MCP real output")'],
          timeoutMs: 10_000,
          maxOutputBytes: 1024,
        };
        yield* toolkit
          .handle("background_job_start", maliciousInput)
          .pipe(Stream.unwrap, Stream.runDrain);
        const jobs = yield* BackgroundJobs.BackgroundJobs;
        const result = yield* jobs.wait({
          callerThreadId: owner,
          id: "mcp-job",
          timeoutMs: 10_000,
        });
        expect(result.job.ownerThreadId).toBe(owner);
        expect(result.job.state).toBe("completed");
        const output = yield* jobs.output({
          callerThreadId: owner,
          id: "mcp-job",
          cursor: 0,
          limitBytes: 1024,
        });
        expect(output.chunks.map((chunk) => chunk.text).join("")).toBe("MCP real output");
        const denied = yield* toolkit
          .handle("background_job_list", {})
          .pipe(
            Stream.unwrap,
            Stream.runDrain,
            Effect.provideService(
              McpInvocationContext.McpInvocationContext,
              invocation(["automation"]),
            ),
            Effect.result,
          );
        expect(denied._tag).toBe("Failure");
        expect(Object.keys(AutomationToolkit.tools)).not.toContain("unattended_grants_create");
      }).pipe(
        Effect.provide(live),
        Effect.provideService(
          McpInvocationContext.McpInvocationContext,
          invocation(["background-jobs"]),
        ),
      ),
    ),
);
