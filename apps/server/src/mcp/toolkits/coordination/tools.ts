import {
  CoordinationReadInput,
  CoordinationWriteInput,
  CoordinationPlan,
  CoordinationError,
  McpCapabilityUnavailableError,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import * as CoordinationPlans from "../../../orchestration/CoordinationPlans.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import {
  CoordinationMailbox,
  CoordinationMailboxReadInput,
  CoordinationMailboxWriteInput,
} from "@t3tools/contracts";

const { callerThreadId: _caller, ...readFields } = CoordinationReadInput.fields;
const writeParameters = Schema.Union(
  CoordinationWriteInput.members.map((member) => {
    const { callerThreadId: _caller, ...fields } = member.fields;
    return Schema.Struct(fields);
  }),
);
const dependencies = [
  McpInvocationContext.McpInvocationContext,
  CoordinationPlans.CoordinationPlans,
];
const failure = Schema.Union([CoordinationError, McpCapabilityUnavailableError]);
const { callerThreadId: _mailboxCaller, ...mailboxReadFields } =
  CoordinationMailboxReadInput.fields;
const mailboxWriteParameters = Schema.Union(
  CoordinationMailboxWriteInput.members.map((member) => {
    const { callerThreadId: _caller, ...fields } = member.fields;
    return Schema.Struct(fields);
  }),
);
export const CoordinationToolkit = Toolkit.make(
  Tool.make("coordination_mailbox_read", {
    description:
      "Read your durable inbox, joined channels and root shared context. Pending means stored, not injected into an active native turn. Reading does not acknowledge messages.",
    parameters: Schema.Struct(mailboxReadFields),
    success: CoordinationMailbox,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Idempotent, true),
  Tool.make("coordination_mailbox_write", {
    description:
      "Send root-local peer/channel messages, explicitly acknowledge your inbox, manage channels or compare-and-set shared context. Active native workers are never interrupted by mailbox sends.",
    parameters: Schema.Struct({ input: mailboxWriteParameters }),
    success: CoordinationMailbox,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("coordination_read", {
    description:
      "Read a bounded root-local coordination plan and attempt artifacts. Evidence is agent-reported.",
    parameters: Schema.Struct(readFields),
    success: CoordinationPlan,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, true)
    .annotate(Tool.Idempotent, true),
  Tool.make("coordination_write", {
    description:
      "Create/control a bounded plan or report your currently assigned artifact. Completion waits for native quiescence. Reuse commandId for identical retries.",
    parameters: Schema.Struct({ input: writeParameters }),
    success: CoordinationPlan,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
);
