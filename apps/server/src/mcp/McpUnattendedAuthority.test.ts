import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  MessageId,
  OrchestrationThreadShell,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { HttpServer } from "effect/unstable/http";
import * as NetAddress from "effect/unstable/net/NetAddress";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import { ServerEnvironment } from "../environment/ServerEnvironment.ts";
import { NativeUnattendedActivation } from "../orchestration/nativeUnattendedAuthority.ts";
import { ProjectionSnapshotQuery } from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import scheduledMigration from "../persistence/Migrations/059_ScheduledWork.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { makeThreadMcpCapabilities } from "./McpInvocationContext.ts";
import * as McpSessionRegistry from "./McpSessionRegistry.ts";

const threadId = ThreadId.make("grant-root");
const projectId = ProjectId.make("grant-project");
const authority = Schema.decodeUnknownSync(ThreadUnattendedAuthority)({
  grantId: "grant-one",
  grantRevision: 1,
  ownerThreadId: threadId,
  runtimeModeCeiling: "approval-required",
  mcpCapabilityCeiling: ["workers", "memory"],
});
const origin: NativeUnattendedActivation["Service"] = {
  messageId: MessageId.make("scheduled-message"),
  sequence: 10,
  authority,
};
const root = Schema.decodeUnknownSync(OrchestrationThreadShell)({
  id: threadId,
  projectId,
  title: "Grant root",
  modelSelection: { instanceId: "codex", model: "gpt-6.1" },
  runtimeMode: "full-access",
  branch: null,
  worktreePath: null,
  latestTurn: null,
  session: null,
  latestUserMessageAt: null,
  hasPendingApprovals: false,
  hasPendingUserInput: false,
  hasActionableProposedPlan: false,
  createdAt: "2026-10-03T00:00:00.000Z",
  updatedAt: "2026-10-03T00:00:00.000Z",
});
const contextLayer = Layer.mergeAll(
  Layer.mock(ServerSettingsService)({
    getSettings: Effect.succeed({ ...DEFAULT_SERVER_SETTINGS, agentToolCapabilities: ["memory"] }),
  }),
  Layer.mock(ProjectionSnapshotQuery)({
    getThreadShellById: (id) => Effect.succeed(id === threadId ? Option.some(root) : Option.none()),
  }),
);
const initialize = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* scheduledMigration;
  yield* sql`CREATE TABLE IF NOT EXISTS projection_threads (
    thread_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, runtime_mode TEXT NOT NULL, deleted_at TEXT
  )`;
  yield* sql`DELETE FROM projection_thread_activation_authorities`;
  yield* sql`DELETE FROM unattended_grants`;
  yield* sql`DELETE FROM projection_threads`;
  yield* sql`INSERT INTO projection_threads VALUES (${threadId}, ${projectId}, 'full-access', NULL)`;
  yield* sql`INSERT INTO unattended_grants
    (grant_id,owner_thread_id,project_id,revision,ceiling_json,created_at)
    VALUES (${authority.grantId},${threadId},${projectId},1,
      ${JSON.stringify({ runtimeMode: "approval-required", mcpCapabilities: ["memory", "workers"] })},
      '2026-10-03T00:00:00.000Z')`;
  yield* sql`INSERT INTO projection_thread_activation_authorities VALUES
    (${threadId},${origin.messageId},${origin.sequence},${JSON.stringify(authority)})`;
  return sql;
});
const makeRegistry = McpSessionRegistry.__testing.make({ now: () => 1_000 }).pipe(
  Effect.provideService(
    HttpServer.HttpServer,
    HttpServer.HttpServer.of({
      address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 43123),
      serve: (() => Effect.void) as HttpServer.HttpServer["Service"]["serve"],
    }),
  ),
  Effect.provideService(
    ServerEnvironment,
    ServerEnvironment.of({
      getEnvironmentId: Effect.succeed(EnvironmentId.make("grant-environment")),
      getDescriptor: Effect.die("unused"),
    }),
  ),
  Effect.provide(contextLayer),
  Effect.provide(NodeServices.layer),
);
const issue = (registry: McpSessionRegistry.McpSessionRegistry["Service"]) =>
  registry.issue({
    threadId,
    providerInstanceId: ProviderInstanceId.make("codex"),
    capabilities: new Set(["memory", "workers", "preview", "pull-requests"]),
  });

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("unattended MCP authority", (it) => {
  it.effect(
    "intersects supervised activation grants without confusing the root's foreground default",
    () =>
      Effect.gen(function* () {
        const sql = yield* initialize;
        const resolve = yield* makeThreadMcpCapabilities.pipe(Effect.provide(contextLayer));
        expect([...(yield* resolve(threadId))!].sort()).toEqual(["memory", "workers"]);
        yield* sql`UPDATE unattended_grants SET revoked = 1`;
        expect([...(yield* resolve(threadId))!]).toEqual([]);
        yield* sql`UPDATE unattended_grants SET revoked = 0, revision = 2`;
        expect([...(yield* resolve(threadId))!]).toEqual([]);
        yield* sql`UPDATE unattended_grants SET revision = 1, project_id = 'other-project'`;
        expect([...(yield* resolve(threadId))!]).toEqual([]);
      }),
  );
  it.effect(
    "keeps credential provenance after foreground replacement and revokes it with its original grant",
    () =>
      Effect.gen(function* () {
        const sql = yield* initialize;
        const registry = yield* makeRegistry;
        const issued = yield* issue(registry).pipe(
          Effect.provideService(NativeUnattendedActivation, origin),
        );
        const token = issued.config.authorizationHeader.slice(7);
        expect([...(yield* registry.resolve(token))!.capabilities].sort()).toEqual([
          "memory",
          "workers",
        ]);
        yield* sql`UPDATE projection_thread_activation_authorities
        SET message_id = 'foreground-message', event_sequence = 11, authority_json = NULL`;
        const retained = yield* registry.resolve(token);
        expect(retained?.unattendedAuthority).toEqual(authority);
        expect([...retained!.capabilities].sort()).toEqual(["memory", "workers"]);
        yield* sql`UPDATE unattended_grants SET revoked = 1`;
        expect(yield* registry.resolve(token)).toBeUndefined();
        yield* sql`UPDATE unattended_grants SET revoked = 0`;
        expect(yield* registry.resolve(token)).toBeUndefined();
      }),
  );
  it.effect(
    "binds an existing native credential on resume and refuses a different grant's reuse",
    () =>
      Effect.gen(function* () {
        const sql = yield* initialize;
        yield* sql`UPDATE projection_thread_activation_authorities SET authority_json = NULL`;
        const registry = yield* makeRegistry;
        const issued = yield* issue(registry);
        const token = issued.config.authorizationHeader.slice(7);
        yield* sql`UPDATE projection_thread_activation_authorities SET authority_json = ${JSON.stringify(authority)}`;
        yield* registry
          .restrictThreadCapabilities(threadId, new Set(["memory", "workers"]))
          .pipe(Effect.provideService(NativeUnattendedActivation, origin));
        expect((yield* registry.resolve(token))?.unattendedAuthority).toEqual(authority);
        const otherAuthority = { ...authority, grantId: "grant-two" };
        yield* sql`INSERT INTO unattended_grants
        (grant_id,owner_thread_id,project_id,revision,ceiling_json,created_at)
        SELECT 'grant-two',owner_thread_id,project_id,revision,ceiling_json,created_at FROM unattended_grants`;
        yield* sql`UPDATE projection_thread_activation_authorities
        SET authority_json = ${JSON.stringify(otherAuthority)}, message_id = 'second-message', event_sequence = 12`;
        yield* registry.restrictThreadCapabilities(threadId, new Set(["memory", "workers"])).pipe(
          Effect.provideService(NativeUnattendedActivation, {
            messageId: MessageId.make("second-message"),
            sequence: 12,
            authority: otherAuthority,
          }),
        );
        expect(yield* registry.resolve(token)).toBeUndefined();
      }),
  );
  it.effect("does not grant tools to a queued origin superseded by an ordinary activation", () =>
    Effect.gen(function* () {
      const sql = yield* initialize;
      yield* sql`UPDATE projection_thread_activation_authorities
        SET message_id = 'newer-message', event_sequence = 11, authority_json = NULL`;
      const registry = yield* makeRegistry;
      const issued = yield* issue(registry).pipe(
        Effect.provideService(NativeUnattendedActivation, origin),
      );
      expect([...issued.config.capabilities]).toEqual([]);
    }),
  );
});

it.effect("fails closed for a captured unattended origin without its grant database", () =>
  Effect.gen(function* () {
    const registry = yield* makeRegistry;
    const issued = yield* issue(registry).pipe(
      Effect.provideService(NativeUnattendedActivation, origin),
    );
    expect([...issued.config.capabilities]).toEqual([]);
    expect(yield* registry.resolve(issued.config.authorizationHeader.slice(7))).toBeUndefined();
  }),
);
