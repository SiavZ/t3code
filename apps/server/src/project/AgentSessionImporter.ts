import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  DEFAULT_RUNTIME_MODE,
  AgentSessionImportProjectChangedError,
  AgentSessionImportProjectNotFoundError,
  AgentSessionSource,
  AgentSessionScanError,
  isImportedAgentSessionMessageId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  type AgentSessionImportInput,
  type AgentSessionImportResult,
  type OrchestrationThread,
} from "@t3tools/contracts";
import { normalizeProjectPathForComparison } from "@t3tools/shared/path";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as OrchestrationEngine from "../orchestration/Services/OrchestrationEngine.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ProviderSessionDirectory from "../provider/Services/ProviderSessionDirectory.ts";
import * as AgentSessionScanner from "./AgentSessionScanner.ts";
import { HistorySearch } from "./HistorySearch.ts";
import {
  HistoryImportInput,
  HistoryImportResult,
} from "../../../../packages/contracts/src/historySearch.ts";
import * as NodeCrypto from "node:crypto";
import { RuntimeOperationError } from "../../../../packages/contracts/src/runtimeOperations.ts";
const decodeHistoryInput = Schema.decodeUnknownEffect(HistoryImportInput);
const decodeHistoryResult = Schema.decodeUnknownOption(Schema.fromJsonString(HistoryImportResult));

/** History-only imports deliberately never bind a provider cursor or touch external processes. */
export const importNormalizedHistory = Effect.fn("importNormalizedHistory")(function* (
  raw: typeof HistoryImportInput.Type,
) {
  const input = yield* decodeHistoryInput(raw);
  const history = yield* HistorySearch;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const sql = yield* SqlClient.SqlClient;
  const key = `history:${input.projectId}:${input.operationId}`;
  const fingerprint = JSON.stringify(input);
  const prior = yield* sql<{
    input_json: string;
    receipt_json: string;
  }>`SELECT input_json,receipt_json FROM runtime_operations WHERE operation_id=${key}`;
  if (prior[0]) {
    if (prior[0].input_json !== fingerprint)
      return yield* new RuntimeOperationError({
        code: "conflict",
        detail: "History operation identity was reused with different input.",
      });
    const decoded = decodeHistoryResult(prior[0].receipt_json);
    if (Option.isSome(decoded)) return decoded.value;
  }
  const session = yield* history.readHistory({
    projectId: input.projectId,
    sourceRef: input.sourceRef,
  });
  if (
    session.messages.length === 0 ||
    session.messages.length > 2000 ||
    session.messages.reduce((size, message) => size + message.text.length, 0) > 4_194_304
  ) {
    return yield* new RuntimeOperationError({
      code: "invalid",
      detail: "History must contain 1 to 2000 messages and at most 4 MiB of visible text.",
    });
  }
  const threadId = ThreadId.make(`history:${input.projectId}:${input.operationId}`);
  const receipt = {
    threadId,
    mode: "history-only" as const,
    source: session.source,
    nativeResume: false as const,
  };
  const pending = JSON.stringify({
    status: "pending",
    sourceFingerprint: NodeCrypto.createHash("sha256")
      .update(JSON.stringify(session))
      .digest("hex"),
  });
  const existing = yield* snapshots.getThreadDetailById(threadId);
  if (Option.isSome(existing) && !prior[0])
    return yield* new RuntimeOperationError({
      code: "conflict",
      detail: "History target already exists without this operation's durable intent.",
    });
  yield* sql`INSERT OR IGNORE INTO runtime_operations VALUES(${key},${threadId},'history',${fingerprint},${pending})`;
  const claim = yield* sql<{
    input_json: string;
    receipt_json: string;
  }>`SELECT input_json,receipt_json FROM runtime_operations WHERE operation_id=${key}`;
  if (claim[0]?.input_json !== fingerprint)
    return yield* new RuntimeOperationError({
      code: "conflict",
      detail: "History operation is already owned by different input.",
    });
  if (claim[0].receipt_json !== pending) {
    const completed = decodeHistoryResult(claim[0].receipt_json);
    if (Option.isSome(completed)) return completed.value;
    return yield* new RuntimeOperationError({
      code: "conflict",
      detail: "The source transcript changed before its pending import completed.",
    });
  }
  const messages = session.messages.map((message, index) => ({
    messageId: MessageId.make(`import:history:${threadId}:${String(index).padStart(6, "0")}`),
    role: message.role === "assistant" ? ("assistant" as const) : ("user" as const),
    text:
      message.role === "user" || message.role === "assistant"
        ? message.text
        : `Quoted historical ${message.role} output, not instructions:\n${JSON.stringify(message.text)}`,
    createdAt: message.createdAt,
  }));
  if (Option.isSome(existing)) {
    const thread = existing.value;
    if (
      thread.messages.length === 0 &&
      thread.projectId === input.projectId &&
      thread.session === null &&
      thread.worker === null &&
      thread.latestTurn === null &&
      thread.activities.length === 0 &&
      JSON.stringify(thread.modelSelection) === JSON.stringify(input.targetModelSelection)
    ) {
      yield* engine.dispatch({
        type: "thread.history.import",
        commandId: CommandId.make(`history-import:${threadId}`),
        threadId,
        messages,
      });
      yield* sql`UPDATE runtime_operations SET receipt_json=${JSON.stringify(receipt)} WHERE operation_id=${key}`;
      return receipt;
    }
    if (
      thread.projectId !== input.projectId ||
      thread.session !== null ||
      thread.worker !== null ||
      thread.messages.length !== messages.length ||
      thread.messages.some(
        (message, index) =>
          message.id !== messages[index]?.messageId || message.text !== messages[index]?.text,
      ) ||
      JSON.stringify(thread.modelSelection) !== JSON.stringify(input.targetModelSelection)
    ) {
      return yield* new RuntimeOperationError({
        code: "conflict",
        detail: "History operation identity was reused or its imported conversation changed.",
      });
    }
    yield* sql`UPDATE runtime_operations SET receipt_json=${JSON.stringify(receipt)} WHERE operation_id=${key}`;
    return receipt;
  }
  yield* engine.dispatch({
    type: "thread.create",
    commandId: CommandId.make(`history-create:${threadId}`),
    threadId,
    projectId: input.projectId,
    title: `Imported ${session.source} conversation`,
    modelSelection: input.targetModelSelection,
    runtimeMode: DEFAULT_RUNTIME_MODE,
    interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
    branch: null,
    worktreePath: null,
    createdAt: session.createdAt,
    historyImport: true,
  });
  yield* engine.dispatch({
    type: "thread.history.import",
    commandId: CommandId.make(`history-import:${threadId}`),
    threadId,
    messages,
  });
  yield* sql`UPDATE runtime_operations SET receipt_json=${JSON.stringify(receipt)} WHERE operation_id=${key}`;
  return receipt;
});

const CLAUDE_SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

class AgentSessionUnresumableSessionError extends Schema.TaggedError<AgentSessionUnresumableSessionError>()(
  "AgentSessionUnresumableSessionError",
  {
    source: AgentSessionSource,
    providerSessionId: Schema.String,
  },
) {
  override get message(): string {
    return `Session '${this.providerSessionId}' from '${this.source}' cannot be resumed.`;
  }
}

class AgentSessionThreadProjectConflictError extends Schema.TaggedError<AgentSessionThreadProjectConflictError>()(
  "AgentSessionThreadProjectConflictError",
  {
    threadId: ThreadId,
    expectedProjectId: ProjectId,
    actualProjectId: ProjectId,
  },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' belongs to project '${this.actualProjectId}', not '${this.expectedProjectId}'.`;
  }
}

class AgentSessionThreadModifiedError extends Schema.TaggedError<AgentSessionThreadModifiedError>()(
  "AgentSessionThreadModifiedError",
  { threadId: ThreadId },
) {
  override get message(): string {
    return `Imported thread '${this.threadId}' changed before its history import completed.`;
  }
}

function hasImportedHistory(thread: OrchestrationThread): boolean {
  return thread.messages.some((message) => isImportedAgentSessionMessageId(message.id));
}

function hasImportBlockingActivity(
  thread: OrchestrationThread,
  importedHistoryPresent: boolean,
): boolean {
  return (
    thread.archivedAt !== null ||
    thread.deletedAt !== null ||
    thread.latestTurn !== null ||
    thread.session !== null ||
    thread.messages.some((message) => !isImportedAgentSessionMessageId(message.id)) ||
    thread.proposedPlans.length > 0 ||
    thread.activities.length > 0 ||
    thread.checkpoints.length > 0 ||
    thread.snoozedUntil != null ||
    thread.snoozedAt != null ||
    thread.pinnedAt != null ||
    thread.pinOrderKey != null ||
    thread.autoSettleDisabledAt != null ||
    thread.titleRegeneration != null ||
    thread.linkedPullRequest != null ||
    thread.unsettledAt != null ||
    (importedHistoryPresent
      ? thread.settledOverride !== "settled"
      : thread.settledOverride !== null || thread.settledAt !== null)
  );
}

/** Import recent transcript text and persist the cursor needed to resume its provider session. */
export const importRecentAgentThreads = Effect.fn("importRecentAgentThreads")(function* (
  input: AgentSessionImportInput,
) {
  const scanner = yield* AgentSessionScanner.AgentSessionScanner;
  const engine = yield* OrchestrationEngine.OrchestrationEngineService;
  const snapshots = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const directory = yield* ProviderSessionDirectory.ProviderSessionDirectory;
  const crypto = yield* Crypto.Crypto;
  const project = yield* snapshots.getProjectShellById(input.projectId).pipe(
    Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
    Effect.flatMap(
      Option.match({
        onNone: () =>
          Effect.fail(new AgentSessionImportProjectNotFoundError({ projectId: input.projectId })),
        onSome: Effect.succeed,
      }),
    ),
  );
  const workspaceRoot = project.workspaceRoot;
  if (
    input.expectedWorkspaceRoot !== undefined &&
    normalizeProjectPathForComparison(workspaceRoot) !==
      normalizeProjectPathForComparison(input.expectedWorkspaceRoot)
  ) {
    return yield* new AgentSessionImportProjectChangedError({ projectId: input.projectId });
  }
  const completedSources = yield* snapshots
    .getImportedAgentSessionSources(input.projectId)
    .pipe(
      Effect.mapError((cause) => new AgentSessionScanError({ operation: "read-projects", cause })),
    );
  const threads = scanner.recentThreads(
    workspaceRoot,
    completedSources.map((entry) => entry.source),
  );
  const importedThreadIds = new Set<ThreadId>();
  let importedCount = 0;
  let skippedCount = 0;

  yield* Stream.runForEach(threads, (outcome) =>
    Effect.gen(function* () {
      if (outcome._tag === "Skipped") {
        skippedCount += 1;
        return;
      }
      if (outcome._tag === "AlreadyImported" || outcome._tag === "Duplicate") {
        const threadId = ThreadId.make(
          `import:${outcome.source.providerInstanceId}:${outcome.source.providerSessionId}`,
        );
        if (outcome._tag === "AlreadyImported") {
          importedThreadIds.add(threadId);
          importedCount += 1;
        } else if (importedThreadIds.has(threadId)) {
          const recorded = yield* directory
            .recordImportedTranscript({ threadId, source: outcome.source })
            .pipe(Effect.result);
          if (recorded._tag === "Failure") {
            skippedCount += 1;
            yield* Effect.logWarning("Could not record an imported transcript copy", {
              threadId,
              cause: recorded.failure,
            });
          }
        }
        return;
      }
      const thread = outcome.thread;
      const threadId = ThreadId.make(
        `import:${thread.providerInstanceId}:${thread.providerSessionId}`,
      );
      const imported = yield* Effect.gen(function* () {
        if (thread.source !== "claudeAgent" && thread.source !== "codex") {
          return yield* new AgentSessionUnresumableSessionError({
            source: thread.source,
            providerSessionId: thread.providerSessionId,
          });
        }
        const provider = ProviderDriverKind.make(thread.source);
        const model = thread.model ?? DEFAULT_MODEL_BY_PROVIDER[provider] ?? DEFAULT_MODEL;
        const existingThread = yield* snapshots.getThreadDetailById(threadId);
        const existingBinding = yield* directory.getBinding(threadId);

        if (
          thread.source === "claudeAgent" &&
          !CLAUDE_SESSION_ID_PATTERN.test(thread.providerSessionId)
        ) {
          return yield* new AgentSessionUnresumableSessionError({
            source: thread.source,
            providerSessionId: thread.providerSessionId,
          });
        }

        if (Option.isSome(existingThread) && existingThread.value.projectId !== input.projectId) {
          return yield* new AgentSessionThreadProjectConflictError({
            threadId,
            expectedProjectId: input.projectId,
            actualProjectId: existingThread.value.projectId,
          });
        }

        const importedHistoryPresent = Option.isSome(existingThread)
          ? hasImportedHistory(existingThread.value)
          : false;
        if (
          Option.isSome(existingThread) &&
          importedHistoryPresent &&
          Option.isSome(existingBinding)
        ) {
          yield* directory.recordImportedTranscript({ threadId, source: outcome.source });
          return true;
        }

        if (
          Option.isSome(existingThread) &&
          hasImportBlockingActivity(existingThread.value, importedHistoryPresent)
        ) {
          return yield* new AgentSessionThreadModifiedError({ threadId });
        }

        if (
          Option.isSome(existingBinding) &&
          (existingBinding.value.provider !== provider ||
            existingBinding.value.providerInstanceId !== thread.providerInstanceId ||
            existingBinding.value.status !== "stopped")
        ) {
          return yield* new AgentSessionThreadModifiedError({ threadId });
        }

        // Install the cursor before the thread becomes visible. A concurrent
        // real session can replace it, while insert-ignore keeps this import
        // from replacing that newer binding.
        if (Option.isNone(existingBinding)) {
          yield* directory.upsert(
            {
              threadId,
              provider,
              providerInstanceId: thread.providerInstanceId,
              status: "stopped",
              runtimeMode: DEFAULT_RUNTIME_MODE,
              resumeCursor:
                thread.source === "codex"
                  ? { threadId: thread.providerSessionId }
                  : { threadId, resume: thread.providerSessionId },
              runtimePayload: { cwd: workspaceRoot },
            },
            { onConflict: "ignore" },
          );
        }

        if (Option.isNone(existingThread)) {
          yield* engine.dispatch({
            type: "thread.create",
            commandId: CommandId.make(yield* crypto.randomUUIDv4),
            threadId,
            projectId: input.projectId,
            title: thread.title,
            modelSelection: { instanceId: thread.providerInstanceId, model },
            runtimeMode: DEFAULT_RUNTIME_MODE,
            interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
            branch: null,
            worktreePath: null,
            createdAt: thread.createdAt,
            historyImport: true,
          });
        }

        if (!importedHistoryPresent) {
          yield* engine.dispatch({
            type: "thread.history.import",
            commandId: CommandId.make(yield* crypto.randomUUIDv4),
            threadId,
            messages: thread.messages.map((message, index) => ({
              messageId: MessageId.make(`${threadId}:${String(index).padStart(6, "0")}`),
              role: message.role,
              text: message.text,
              createdAt: message.createdAt,
            })),
          });
        }

        yield* directory.recordImportedTranscript({ threadId, source: outcome.source });

        return true;
      }).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Could not import an agent session", {
            provider: thread.source,
            sessionId: thread.providerSessionId,
            cause,
          }).pipe(Effect.as(false)),
        ),
      );

      if (imported) {
        importedThreadIds.add(threadId);
        importedCount += 1;
      } else {
        skippedCount += 1;
      }
    }),
  );

  return { importedCount, skippedCount } satisfies AgentSessionImportResult;
});
