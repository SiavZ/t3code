import { describe, expect } from "vite-plus/test";
import { it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { CommandId, ProjectId, ThreadId, MessageId, ProviderInstanceId } from "@t3tools/contracts";
import { ServerConfig } from "../config.ts";
import { SqlitePersistenceMemory } from "../persistence/Layers/Sqlite.ts";
import { OrchestrationCommandReceiptRepositoryLive } from "../persistence/Layers/OrchestrationCommandReceipts.ts";
import { OrchestrationEventStoreLive } from "../persistence/Layers/OrchestrationEventStore.ts";
import * as RepositoryIdentityResolver from "../project/RepositoryIdentityResolver.ts";
import { OrchestrationEngineLive } from "./Layers/OrchestrationEngine.ts";
import { OrchestrationProjectionPipelineLive } from "./Layers/ProjectionPipeline.ts";
import { OrchestrationProjectionSnapshotQueryLive } from "./Layers/ProjectionSnapshotQuery.ts";
import * as Background from "./ThreadBackgroundLiveness.ts";
import * as PlanProgress from "./ThreadPlanProgress.ts";
import * as Engine from "./Services/OrchestrationEngine.ts";
import * as Snapshots from "./Services/ProjectionSnapshotQuery.ts";
import * as Runtime from "./ThreadRuntimeService.ts";
import * as HistorySearch from "../project/HistorySearch.ts";
import * as Scanner from "../project/AgentSessionScanner.ts";
import { importNormalizedHistory } from "../project/AgentSessionImporter.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as Hooks from "../provider/RuntimeHooks.ts";
import * as Observers from "../provider/RuntimeHookObservers.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as Deferred from "effect/Deferred";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeCrypto from "node:crypto";
const base = Layer.mergeAll(
  OrchestrationEngineLive.pipe(
    Layer.provide(OrchestrationProjectionSnapshotQueryLive),
    Layer.provide(OrchestrationProjectionPipelineLive),
  ),
  OrchestrationProjectionSnapshotQueryLive,
).pipe(
  Layer.provideMerge(Background.layer),
  Layer.provide(PlanProgress.layer),
  Layer.provide(OrchestrationEventStoreLive),
  Layer.provideMerge(OrchestrationCommandReceiptRepositoryLive),
  Layer.provide(RepositoryIdentityResolver.layer),
  Layer.provideMerge(SqlitePersistenceMemory),
  Layer.provideMerge(ServerConfig.layerTest(process.cwd(), { prefix: "runtime-test-" })),
  Layer.provideMerge(NodeServices.layer),
);
const layer = Runtime.layer.pipe(Layer.provideMerge(base));
const historyLayer = HistorySearch.layer.pipe(
  Layer.provide(Scanner.layer),
  Layer.provide(ServerSettingsService.layerTest()),
  Layer.provideMerge(base),
);
const at = "2026-10-03T00:00:00.000Z";
const projectId = ProjectId.make("runtime-project");
const threadId = ThreadId.make("runtime-thread");
const instanceId = ProviderInstanceId.make("codex");
describe("ThreadRuntimeService real engine/SQLite", () => {
  it.effect(
    "observes committed session transitions once and sends metadata rather than conversation text",
    () => {
      const ended = Deferred.makeUnsafe<void>();
      const turnEnded = Deferred.makeUnsafe<void>();
      const calls: ProcessRunner.ProcessRunInput[] = [];
      const runner = Layer.succeed(ProcessRunner.ProcessRunner, {
        run: (input) =>
          Effect.gen(function* () {
            calls.push(input);
            if (input.command === "end") yield* Deferred.succeed(ended, undefined);
            if (input.command === "turn-end") yield* Deferred.succeed(turnEnded, undefined);
            return {
              stdout: "",
              stderr: "",
              code: ChildProcessSpawner.ExitCode(0),
              timedOut: false,
              stdoutTruncated: false,
              stderrTruncated: false,
              stdoutInvalidUtf8: false,
              stderrInvalidUtf8: false,
            };
          }),
      });
      const observerLayer = Observers.layer.pipe(
        Layer.provideMerge(Hooks.layer),
        Layer.provide(runner),
        Layer.provideMerge(base),
      );
      return Effect.gen(function* () {
        const engine = yield* Engine.OrchestrationEngineService;
        const hooks = yield* Hooks.RuntimeHooks;
        const observer = yield* Observers.RuntimeHookObservers;
        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("observer-project"),
          projectId,
          title: "Observer",
          workspaceRoot: process.cwd(),
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("observer-thread"),
          threadId,
          projectId,
          title: "Private conversation",
          modelSelection: { instanceId, model: "model" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: at,
        });
        const config = {
          projectId,
          enabled: true,
          args: [],
          timeoutMs: 1000,
          failurePolicy: "open" as const,
          coverage: "host-tools" as const,
        };
        yield* hooks.configure(
          { ...config, id: "start", command: "start", event: "session.start" },
          { projectId, trustedOperator: true },
        );
        yield* hooks.configure(
          { ...config, id: "end", command: "end", event: "session.end" },
          { projectId, trustedOperator: true },
        );
        yield* hooks.configure(
          { ...config, id: "turn-start", command: "turn-start", event: "turn.start" },
          { projectId, trustedOperator: true },
        );
        yield* hooks.configure(
          { ...config, id: "turn-end", command: "turn-end", event: "turn.end" },
          { projectId, trustedOperator: true },
        );
        yield* observer.start();
        yield* engine.dispatch({
          type: "thread.turn.start",
          commandId: CommandId.make("observer-turn"),
          threadId,
          message: {
            messageId: MessageId.make("observer-message"),
            role: "user",
            text: "Private user request",
            attachments: [],
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          createdAt: at,
        });
        const session = {
          threadId,
          status: "ready" as const,
          providerName: "codex" as const,
          providerInstanceId: instanceId,
          runtimeMode: "full-access" as const,
          activeTurnId: null,
          lastError: null,
          updatedAt: at,
        };
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("ready-one"),
          threadId,
          session,
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("ready-two"),
          threadId,
          session,
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("stopped"),
          threadId,
          session: { ...session, status: "stopped" },
          createdAt: at,
        });
        yield* Deferred.await(ended);
        yield* Deferred.await(turnEnded);
        expect(calls.map((call) => call.command)).toEqual([
          "turn-start",
          "start",
          "end",
          "turn-end",
        ]);
        expect(calls.every((call) => !call.stdin?.includes("Private conversation"))).toBe(true);
      }).pipe(Effect.provide(observerLayer));
    },
  );
  it.effect(
    "imports normalized history-only through the real T3 reader without native runtime bindings",
    () =>
      Effect.gen(function* () {
        const engine = yield* Engine.OrchestrationEngineService;
        const snapshots = yield* Snapshots.ProjectionSnapshotQuery;
        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("project"),
          projectId,
          title: "Runtime",
          workspaceRoot: process.cwd(),
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId,
          title: "Source",
          modelSelection: { instanceId, model: "model" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: at,
          historyImport: true,
        });
        yield* engine.dispatch({
          type: "thread.history.import",
          commandId: CommandId.make("history"),
          threadId,
          messages: [
            {
              messageId: MessageId.make("import:source"),
              role: "user",
              text: "Visible normalized source",
              createdAt: at,
            },
          ],
        });
        const input = {
          projectId,
          sourceRef: `t3:${threadId}`,
          targetModelSelection: { instanceId, model: "fresh" },
          operationId: "history-one",
          mode: "history-only" as const,
        };
        const receipt = yield* importNormalizedHistory(input);
        expect(receipt.nativeResume).toBe(false);
        expect(yield* importNormalizedHistory(input)).toEqual(receipt);
        const imported = Option.getOrThrow(yield* snapshots.getThreadDetailById(receipt.threadId));
        expect(imported.messages[0]?.text).toBe("Visible normalized source");
        expect(imported.messages[0]?.id).not.toBe("import:source");
        expect(imported.session).toBeNull();
        expect(imported.worker).toBeNull();
        expect(
          (yield* importNormalizedHistory({
            ...input,
            targetModelSelection: { instanceId, model: "other" },
          }).pipe(Effect.result))._tag,
        ).toBe("Failure");
        const recoveryInput = { ...input, operationId: "history-recover" };
        const target = ThreadId.make(`history:${projectId}:history-recover`);
        const reader = yield* HistorySearch.HistorySearch;
        const sql = yield* SqlClient.SqlClient;
        const normalized = yield* reader.readHistory({ projectId, sourceRef: input.sourceRef });
        const intent = JSON.stringify({
          status: "pending",
          sourceFingerprint: NodeCrypto.createHash("sha256")
            .update(JSON.stringify(normalized))
            .digest("hex"),
        });
        yield* sql`INSERT INTO runtime_operations VALUES(${target},${target},'history',${JSON.stringify(recoveryInput)},${intent})`;
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("partial-history-create"),
          threadId: target,
          projectId,
          title: "Pending import",
          modelSelection: input.targetModelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: at,
          historyImport: true,
        });
        const recovered = yield* importNormalizedHistory(recoveryInput);
        expect(recovered.threadId).toBe(target);
        expect(
          Option.getOrThrow(yield* snapshots.getThreadDetailById(target)).messages.map(
            (message) => message.text,
          ),
        ).toEqual(["Visible normalized source"]);
        expect(yield* importNormalizedHistory(recoveryInput)).toEqual(recovered);
      }).pipe(Effect.provide(historyLayer)),
  );
  it.effect(
    "forks independent visible history and provenance without native sessions or approvals",
    () =>
      Effect.gen(function* () {
        const engine = yield* Engine.OrchestrationEngineService;
        const snapshots = yield* Snapshots.ProjectionSnapshotQuery;
        const service = yield* Runtime.ThreadRuntimeService;
        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("project"),
          projectId,
          title: "Runtime",
          workspaceRoot: process.cwd(),
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId,
          title: "Source",
          modelSelection: { instanceId, model: "model" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: at,
          historyImport: true,
        });
        yield* engine.dispatch({
          type: "thread.history.import",
          commandId: CommandId.make("history"),
          threadId,
          messages: [
            {
              messageId: MessageId.make("origin-user"),
              role: "user",
              text: "Visible request",
              createdAt: at,
            },
            {
              messageId: MessageId.make("origin-assistant"),
              role: "assistant",
              text: "Visible response",
              createdAt: "2026-10-03T00:00:01.000Z",
            },
          ],
        });
        const source = Option.getOrThrow(yield* snapshots.getThreadDetailById(threadId));
        const input = {
          sourceThreadId: threadId,
          expectedUpdatedAt: source.updatedAt,
          operationId: "fork-one",
          throughMessageId: MessageId.make("origin-assistant"),
        };
        const result = yield* service.fork(input);
        const fork = Option.getOrThrow(yield* snapshots.getThreadDetailById(result.threadId));
        expect(fork.messages.map((m) => m.text)).toEqual(source.messages.map((m) => m.text));
        expect(fork.messages[0]?.id).not.toBe(source.messages[0]?.id);
        expect(fork.session).toBeNull();
        expect(fork.worker).toBeNull();
        expect(fork.activities).toEqual([]);
        expect(fork.forkProvenance?.sourceThreadId).toBe(threadId);
        yield* engine.dispatch({
          type: "thread.message.user.append",
          commandId: CommandId.make("append"),
          threadId: fork.id,
          message: {
            messageId: MessageId.make("fork-extra"),
            text: "Independent follow-up",
            attachments: [],
          },
          createdAt: at,
        });
        expect(
          Option.getOrThrow(yield* snapshots.getThreadDetailById(threadId)).messages,
        ).toHaveLength(2);
      }).pipe(Effect.provide(layer)),
  );
  it.effect(
    "requires acknowledged stopped state and consumes a seed only after matching epoch acknowledgement",
    () =>
      Effect.gen(function* () {
        const engine = yield* Engine.OrchestrationEngineService;
        const snapshots = yield* Snapshots.ProjectionSnapshotQuery;
        const service = yield* Runtime.ThreadRuntimeService;
        yield* engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("project"),
          projectId,
          title: "Runtime",
          workspaceRoot: process.cwd(),
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId,
          title: "Source",
          modelSelection: { instanceId, model: "model" },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: at,
          historyImport: true,
        });
        yield* engine.dispatch({
          type: "thread.history.import",
          commandId: CommandId.make("history"),
          threadId,
          messages: [
            {
              messageId: MessageId.make("import:seed-user"),
              role: "user",
              text: "Retained visible request",
              createdAt: at,
            },
          ],
        });
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("session-ready"),
          threadId,
          session: {
            threadId,
            status: "ready",
            providerName: "codex",
            providerInstanceId: instanceId,
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: at,
          },
          createdAt: at,
        });
        const source = Option.getOrThrow(yield* snapshots.getThreadDetailById(threadId));
        const input = {
          threadId,
          expectedUpdatedAt: source.updatedAt,
          operationId: "handoff-one",
          targetModelSelection: { instanceId, model: "fresh" },
        };
        const receipt = yield* service.handoff(input);
        expect((yield* service.handoff(input)).epochId).toBe(receipt.epochId);
        expect(
          (yield* service.prepareContext({
            threadId,
            turnKey: "turn",
            messageText: "Next request",
          })).epochId,
        ).toBeNull();
        const premature = yield* engine
          .dispatch({
            type: "thread.runtime.handoff.commit",
            commandId: CommandId.make("premature-commit"),
            threadId,
            expectedEpochId: receipt.epochId!,
            expectedNativeSessionId: null,
            acknowledgedStopped: true,
            createdAt: at,
          })
          .pipe(Effect.result);
        expect(premature._tag).toBe("Failure");
        yield* engine.dispatch({
          type: "thread.session.set",
          commandId: CommandId.make("session-stopped"),
          threadId,
          session: {
            threadId,
            status: "stopped",
            providerName: "codex",
            providerInstanceId: instanceId,
            runtimeMode: "full-access",
            activeTurnId: null,
            lastError: null,
            updatedAt: at,
          },
          createdAt: at,
        });
        yield* engine.dispatch({
          type: "thread.runtime.handoff.commit",
          commandId: CommandId.make("commit"),
          threadId,
          expectedEpochId: receipt.epochId!,
          expectedNativeSessionId: null,
          acknowledgedStopped: true,
          createdAt: at,
        });
        const first = yield* service.prepareContext({
          threadId,
          turnKey: "turn",
          messageText: "Next request",
        });
        expect(first.messageText).toContain("Retained visible request");
        expect(
          yield* service.prepareContext({
            threadId,
            turnKey: "turn",
            messageText: "Next request",
          }),
        ).toEqual(first);
        const stale = yield* service
          .acknowledgeSeed({ threadId, turnKey: "turn", epochId: "old" })
          .pipe(Effect.result);
        expect(stale._tag).toBe("Failure");
        yield* service.acknowledgeSeed({ threadId, turnKey: "turn", epochId: receipt.epochId! });
        yield* service.acknowledgeSeed({ threadId, turnKey: "turn", epochId: receipt.epochId! });
        expect(
          yield* service.prepareContext({ threadId, turnKey: "later", messageText: "Later" }),
        ).toEqual({ messageText: "Later", epochId: null });
        expect(
          Option.getOrThrow(yield* snapshots.getThreadDetailById(threadId)).messages[0]?.text,
        ).toBe("Retained visible request");
        const current = Option.getOrThrow(yield* snapshots.getThreadDetailById(threadId));
        const interrupted = {
          threadId,
          expectedUpdatedAt: current.updatedAt,
          operationId: "interrupted",
          targetModelSelection: { instanceId, model: "another" },
        };
        yield* service.handoff(interrupted);
        yield* service.recoverPending();
        yield* service.recoverPending();
        expect((yield* service.metadata(threadId)).runtimeHandoff?.status).toBe("failed");
        expect((yield* service.handoff(interrupted)).status).toBe("failed");
        const sql = yield* SqlClient.SqlClient;
        const orphanInput = {
          threadId,
          operationId: "receipt-before-dispatch",
          expectedUpdatedAt: interrupted.expectedUpdatedAt,
          targetModelSelection: interrupted.targetModelSelection,
        };
        const orphanReceipt = {
          ...receipt,
          operationId: orphanInput.operationId,
          status: "accepted",
        };
        yield* sql`INSERT INTO runtime_operations VALUES(${`${threadId}:${orphanInput.operationId}`},${threadId},'handoff',${JSON.stringify(orphanInput)},${JSON.stringify(orphanReceipt)})`;
        yield* service.recoverPending();
        expect((yield* service.handoff(orphanInput)).status).toBe("failed");
      }).pipe(Effect.provide(layer)),
  );
});
