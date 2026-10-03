import { McpCapabilityUnavailableError } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Tool from "effect/unstable/ai/Tool";
import * as Toolkit from "effect/unstable/ai/Toolkit";
import * as Q from "../../../../../../packages/contracts/src/qualityRecords.ts";
import * as Quality from "../../../orchestration/QualityRecords.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
const dependencies = [
  McpInvocationContext.McpInvocationContext,
  Quality.QualityRecords,
  ProjectionSnapshotQuery.ProjectionSnapshotQuery,
];
const failure = Schema.Union([Q.QualityRecordsError, McpCapabilityUnavailableError]);
const { threadId: _threadId, ...updateFields } = Q.QualityUpdateInput.fields;
export const QualityRecordsToolkit = Toolkit.make(
  Tool.make("quality_records_update", {
    description:
      "Record this thread's grouped todos and goal evidence. Confidence is agent-reported, never independent test verification. Reuse operationId for retries. Omitted goals retain their groups.",
    parameters: Schema.Struct(updateFields),
    success: Q.QualityRecord,
    failure,
    dependencies,
  })
    .annotate(Tool.Readonly, false)
    .annotate(Tool.Idempotent, true),
  Tool.make("quality_records_get", {
    description:
      "Read this thread's durable reported quality record and bounded confidence history.",
    success: Schema.NullOr(Q.QualityRecord),
    failure,
    dependencies,
  }).annotate(Tool.Readonly, true),
);
