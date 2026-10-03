import {
  type EnvironmentId,
  McpCapabilityUnavailableError,
  PreviewAutomationUnavailableError,
  ThreadUnattendedAuthority,
  isWorkerRuntimeModeAllowed,
  type WorkerMcpCapability,
  type OrchestrationThreadShell,
  type ProviderInstanceId,
  type ThreadId,
} from "@t3tools/contracts";
import { resolveProjectSettings } from "@t3tools/shared/projectSettings";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";

import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as ServerSettings from "../serverSettings.ts";

export type McpCapability = WorkerMcpCapability;

/** Capture optional orchestration once, then re-read settings and ceilings on every use. */
export const makeThreadMcpCapabilities = Effect.gen(function* () {
  const snapshots = yield* Effect.serviceOption(ProjectionSnapshotQuery.ProjectionSnapshotQuery);
  const settingsService = yield* Effect.serviceOption(ServerSettings.ServerSettingsService);
  const sql = yield* Effect.serviceOption(SqlClient.SqlClient);
  return Effect.fn("McpInvocationContext.threadCapabilities")(
    function* (threadId: ThreadId) {
      if (Option.isNone(settingsService)) {
        return Option.isNone(snapshots) ? undefined : new Set<McpCapability>();
      }
      const settings = yield* settingsService.value.getSettings;
      const ancestors: Array<OrchestrationThreadShell> = [];
      if (Option.isSome(snapshots)) {
        let currentId = threadId;
        const visited = new Set<ThreadId>();
        while (true) {
          if (visited.has(currentId) || ancestors.length > 2) return new Set<McpCapability>();
          visited.add(currentId);
          const current = yield* snapshots.value.getThreadShellById(currentId);
          if (Option.isNone(current)) {
            if (ancestors.length > 0) return new Set<McpCapability>();
            break;
          }
          const thread = current.value;
          if (thread.worker?.stopRequestedAt != null) return new Set<McpCapability>();
          if (ancestors[0] && thread.projectId !== ancestors[0].projectId) {
            return new Set<McpCapability>();
          }
          ancestors.push(thread);
          if (!thread.worker) break;
          currentId = thread.worker.ownerThreadId;
        }
        const root = ancestors.at(-1);
        if (ancestors.some((thread) => thread.worker && thread.worker.rootThreadId !== root?.id)) {
          return new Set<McpCapability>();
        }
      }
      const overridden = Object.values(settings.projectSettingsOverrides);
      const access = ancestors[0]
        ? resolveProjectSettings(settings, ancestors[0].projectId).settings
        : {
            enableAgentBrowserAccess:
              settings.enableAgentBrowserAccess &&
              !overridden.some((entry) => entry.enableAgentBrowserAccess !== undefined),
            enableAgentDeviceAccess:
              settings.enableAgentDeviceAccess &&
              !overridden.some((entry) => entry.enableAgentDeviceAccess !== undefined),
            agentToolCapabilities: [],
          };
      let capabilities = new Set<McpCapability>(["pull-requests"]);
      if (ancestors.length > 0) capabilities.add("workers");
      if (access.enableAgentBrowserAccess) capabilities.add("preview");
      if (access.enableAgentDeviceAccess) capabilities.add("device");
      if (ancestors.length > 0) {
        for (const capability of access.agentToolCapabilities) capabilities.add(capability);
      }
      for (const ancestor of ancestors) {
        const worker = ancestor.worker;
        if (worker) {
          capabilities = new Set(
            [...capabilities].filter((capability) =>
              worker.mcpCapabilityCeiling.includes(capability),
            ),
          );
        }
        if (Option.isSome(sql)) {
          const rows = yield* sql.value<{
            authority_json: string;
            owner_thread_id: string | null;
            project_id: string | null;
            revision: number | null;
            revoked: number | null;
            ceiling_json: string | null;
          }>`SELECT a.authority_json, g.owner_thread_id, g.project_id, g.revision, g.revoked, g.ceiling_json
            FROM projection_thread_activation_authorities a
            LEFT JOIN unattended_grants g ON g.grant_id = json_extract(a.authority_json, '$.grantId')
            WHERE a.thread_id = ${ancestor.id} AND a.authority_json IS NOT NULL LIMIT 1`;
          const row = rows[0];
          if (row) {
            const authority = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(ThreadUnattendedAuthority),
            )(row.authority_json);
            if (
              row.revoked !== 0 ||
              row.revision !== authority.grantRevision ||
              row.owner_thread_id !== authority.ownerThreadId ||
              row.project_id !== ancestor.projectId ||
              authority.ownerThreadId !== ancestors.at(-1)?.id ||
              !isWorkerRuntimeModeAllowed(authority.runtimeModeCeiling, ancestor.runtimeMode) ||
              !row.ceiling_json
            )
              return new Set<McpCapability>();
            const grantCeiling = yield* Schema.decodeUnknownEffect(
              Schema.fromJsonString(
                Schema.Struct({
                  runtimeMode: Schema.Literals(["approval-required", "full-access"]),
                  mcpCapabilities: Schema.Array(Schema.String),
                }),
              ),
            )(row.ceiling_json);
            if (
              !isWorkerRuntimeModeAllowed(authority.runtimeModeCeiling, grantCeiling.runtimeMode)
            ) {
              return new Set<McpCapability>();
            }
            capabilities = new Set(
              [...capabilities].filter(
                (capability) =>
                  authority.mcpCapabilityCeiling.includes(capability) &&
                  grantCeiling.mcpCapabilities.includes(capability),
              ),
            );
          }
        }
      }
      return capabilities;
    },
    Effect.catch(() => Effect.succeed(new Set<McpCapability>())),
  );
});

export interface McpInvocationScope {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
  readonly providerSessionId: string;
  readonly providerInstanceId: ProviderInstanceId;
  readonly capabilities: ReadonlySet<McpCapability>;
  /** Credential-bound provenance, never decoded from tool arguments. */
  readonly unattendedAuthority?: ThreadUnattendedAuthority;
  readonly issuedAt: number;
}

export class McpInvocationContext extends Context.Service<
  McpInvocationContext,
  McpInvocationScope
>()("t3/mcp/McpInvocationContext") {}

/** The error a missing capability surfaces as; preview keeps its own so the broker can route it. */
export type McpCapabilityError<C extends McpCapability> = C extends "preview"
  ? PreviewAutomationUnavailableError
  : McpCapabilityUnavailableError;

const missingCapability = (
  invocation: McpInvocationScope,
  capability: McpCapability,
): PreviewAutomationUnavailableError | McpCapabilityUnavailableError => {
  const fields = {
    environmentId: invocation.environmentId,
    threadId: invocation.threadId,
    providerSessionId: invocation.providerSessionId,
    providerInstanceId: invocation.providerInstanceId,
  };
  return capability === "preview"
    ? new PreviewAutomationUnavailableError({ capability, ...fields })
    : new McpCapabilityUnavailableError({ capability, ...fields });
};

export const requireMcpCapability = <const C extends McpCapability>(
  capability: C,
): Effect.Effect<McpInvocationScope, McpCapabilityError<C>, McpInvocationContext> =>
  McpInvocationContext.pipe(
    Effect.filterOrFail(
      (invocation) => invocation.capabilities.has(capability),
      // The conditional type narrows what the literal argument decided at runtime.
      (invocation) => missingCapability(invocation, capability) as McpCapabilityError<C>,
    ),
    Effect.withSpan("mcp.requireCapability"),
  );
