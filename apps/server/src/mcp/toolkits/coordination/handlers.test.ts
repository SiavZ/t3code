import { expect, it } from "@effect/vitest";
import { CommandId, EnvironmentId, ProviderInstanceId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import { CoordinationWriteInput, CoordinationError } from "@t3tools/contracts";
import * as Plans from "../../../orchestration/CoordinationPlans.ts";
import * as Invocation from "../../McpInvocationContext.ts";
import { CoordinationToolkitHandlersLive } from "./handlers.ts";
import { CoordinationToolkit } from "./tools.ts";

const decodeWrite = Schema.decodeUnknownEffect(CoordinationWriteInput);

it.effect("forwards authenticated graph authority separately from untrusted tool parameters", () =>
  Effect.gen(function* () {
    const owner = ThreadId.make("grant-owner");
    const scope: Invocation.McpInvocationScope = {
      environmentId: EnvironmentId.make("environment"),
      threadId: owner,
      providerSessionId: "session",
      providerInstanceId: ProviderInstanceId.make("codex"),
      capabilities: new Set(["workers"]),
      issuedAt: 1,
      unattendedAuthority: {
        grantId: "grant",
        grantRevision: 1,
        ownerThreadId: owner,
        runtimeModeCeiling: "approval-required",
        mcpCapabilityCeiling: ["workers"],
      },
    };
    const service = Layer.mock(Plans.CoordinationPlans)({
      write: (input, authority) => {
        expect(input.callerThreadId).toBe(owner);
        expect(authority).toEqual(scope.unattendedAuthority);
        return Effect.fail(
          new CoordinationError({
            code: "forbidden",
            detail: "Domain grant validation rejected the request",
          }),
        );
      },
    });
    const toolkit = yield* CoordinationToolkit.pipe(
      Effect.provide(CoordinationToolkitHandlersLive.pipe(Layer.provide(service))),
    );
    for (const operation of ["create", "run", "retry", "repair"] as const) {
      const input = {
        operation,
        commandId: CommandId.make(`denied-${operation}`),
        rootThreadId: owner,
        planId: "plan",
        expectedRevision: 0,
        ...(operation === "create"
          ? {
              policy: { mode: "light", maxConcurrent: 1, retainWorkers: true },
              nodes: [
                {
                  id: "work",
                  kind: "work",
                  prompt: "test",
                  dependsOn: [],
                  gateScope: [],
                  attemptLimit: 1,
                  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "model" },
                },
              ],
            }
          : {}),
        ...(operation === "retry" ? { nodeId: "work" } : {}),
        ...(operation === "repair"
          ? {
              nodeId: "gate",
              successorGateId: "gate-next",
              repairs: [
                {
                  id: "repair",
                  kind: "work",
                  prompt: "repair",
                  dependsOn: [],
                  gateScope: [],
                  attemptLimit: 1,
                  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "model" },
                },
              ],
            }
          : {}),
      };
      const decoded = yield* decodeWrite({ ...input, callerThreadId: owner });
      const { callerThreadId: _caller, ...parameters } = decoded;
      const error = yield* toolkit
        .handle("coordination_write", { input: parameters })
        .pipe(
          Stream.unwrap,
          Stream.runDrain,
          Effect.provide(service),
          Effect.provideService(Invocation.McpInvocationContext, scope),
          Effect.flip,
        );
      expect(error).toMatchObject({ _tag: "CoordinationError", code: "forbidden" });
    }
  }),
);
