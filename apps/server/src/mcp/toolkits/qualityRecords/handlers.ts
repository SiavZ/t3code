import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Q from "../../../../../../packages/contracts/src/qualityRecords.ts";
import * as Quality from "../../../orchestration/QualityRecords.ts";
import * as ProjectionSnapshotQuery from "../../../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { QualityRecordsToolkit } from "./tools.ts";
const make = Effect.gen(function* () {
  const quality = yield* Quality.QualityRecords;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const authority = Effect.fn("QualityRecordsToolkit.authority")(function* () {
    const scope = yield* McpInvocationContext.requireMcpCapability("quality-records");
    const thread = yield* snapshots.getThreadShellById(scope.threadId).pipe(
      Effect.mapError(
        () =>
          new Q.QualityRecordsError({
            code: "forbidden",
            detail: "Authenticated thread could not be resolved.",
          }),
      ),
    );
    if (Option.isNone(thread))
      return yield* new Q.QualityRecordsError({
        code: "forbidden",
        detail: "Authenticated thread no longer exists.",
      });
    return { threadId: scope.threadId, source: "agent-reported" as const };
  });
  return QualityRecordsToolkit.of({
    quality_records_update: Effect.fn("QualityRecordsToolkit.update")(function* (input) {
      const a = yield* authority();
      return yield* quality.update({ ...input, threadId: a.threadId }, a);
    }),
    quality_records_get: Effect.fn("QualityRecordsToolkit.get")(function* () {
      const a = yield* authority();
      return yield* quality.get({ threadId: a.threadId }, a);
    }),
  });
});
export const QualityRecordsToolkitHandlersLive = QualityRecordsToolkit.toLayer(make);
