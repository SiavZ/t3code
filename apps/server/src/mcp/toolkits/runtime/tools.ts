import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import { RuntimeThreadMetadata, McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as R from "../../../../../../packages/contracts/src/runtimeOperations.ts";
import { ThreadRuntimeService } from "../../../orchestration/ThreadRuntimeService.ts";
import { ProviderDoctor } from "../../../provider/ProviderDoctor.ts";
import { ProjectionSnapshotQuery } from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { McpInvocationContext } from "../../McpInvocationContext.ts";
const { threadId: _thread, ...handoffFields } = R.RuntimeHandoffInput.fields;
const { sourceThreadId: _source, ...forkFields } = R.RuntimeForkInput.fields;
const failure = Schema.Union([R.RuntimeOperationError, McpCapabilityUnavailableError]);
const dependencies = [
  McpInvocationContext,
  ThreadRuntimeService,
  ProjectionSnapshotQuery,
  ProviderDoctor,
];
export const RuntimeToolkit = Toolkit.make(
  Tool.make("runtime_metadata", {
    description:
      "Read this thread's durable runtime epoch, bounded visible handoff and fork provenance. Native hidden state is not included.",
    parameters: Tool.EmptyParams,
    success: RuntimeThreadMetadata,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Idempotent, true),
  Tool.make("runtime_fork", {
    description:
      "Fork this thread's completed visible conversation through a selected message into independent IDs. No approvals, workers, MCP sessions or native cursors are copied.",
    parameters: Schema.Struct(forkFields),
    success: R.RuntimeOperationReceipt,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("runtime_handoff", {
    description:
      "Explicitly hand off this idle thread to a fresh native epoch with bounded quoted visible history. Busy native turns, approvals and workers reject the request. This does not preserve hidden native state.",
    parameters: Schema.Struct(handoffFields),
    success: R.RuntimeOperationReceipt,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("runtime_doctor_offline", {
    description:
      "Read cached/offline diagnostics for this thread's configured provider. Does not run processes, network requests or charged inference.",
    parameters: Schema.Struct({ runId: R.ProviderDoctorInput.fields.runId }),
    success: R.ProviderDoctorResult,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Idempotent, true),
);
