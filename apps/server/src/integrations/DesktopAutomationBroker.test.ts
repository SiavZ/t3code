import { expect, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Stream } from "effect";
import * as Broker from "./DesktopAutomationBroker.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import { testPersistence } from "./integrationTestSupport.ts";

it.effect(
  "routes real host messages only with exact fresh consent and rejects cross-thread leases",
  () => {
    const persistence = testPersistence();
    return Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* Broker.DesktopAutomationBroker;
        const approvals = yield* Approvals.WorkflowApprovals;
        const hostDescription = {
          hostId: "test-mac",
          displayName: "Test Mac",
          environmentId: "test-env",
          generation: "g1",
          operations: ["observe"] as const,
        };
        const stream = yield* broker.connectForClient(hostDescription, "host-owner", "test-env");
        expect(
          (yield* Effect.flip(broker.connectForClient(hostDescription, "intruder", "test-env")))
            .reason,
        ).toBe("scope-denied");
        expect(
          (yield* Effect.flip(broker.disconnectForClient("test-mac", "g1", "intruder"))).reason,
        ).toBe("scope-denied");
        const params = {
          hostId: "test-mac",
          environmentId: "test-env",
          threadId: "thread",
          app: "Fixture",
        };
        expect(
          (yield* broker.lease({ ...params, approvalId: "agent-true" }).pipe(Effect.result))._tag,
        ).toBe("Failure");
        const approvalId = yield* approvals.grant({
          humanSessionId: "test-human",
          operation: "desktop.lease",
          review: JSON.stringify({ ...params, generation: "g1" }),
        });
        const lease = yield* broker.lease({ ...params, approvalId });
        const host = yield* Stream.runForEach(stream, (request) => {
          expect(request.action).toEqual({ kind: "observe", app: "Fixture" });
          return broker.respondForClient(
            {
              hostId: "test-mac",
              generation: "g1",
              requestId: request.requestId,
              result: { nodes: ["fixture-node"] },
            },
            "host-owner",
          );
        }).pipe(Effect.forkScoped);
        const result = yield* broker.invoke(
          { environmentId: "test-env", threadId: "thread" },
          lease.leaseId,
          { kind: "observe", app: "Fixture" },
        );
        expect(result).toEqual({ nodes: ["fixture-node"] });
        expect(
          (yield* broker
            .invoke({ environmentId: "test-env", threadId: "foreign" }, lease.leaseId, {
              kind: "observe",
              app: "Fixture",
            })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        yield* broker.disconnect("test-mac", "g1");
        expect(
          (yield* broker
            .invoke({ environmentId: "test-env", threadId: "thread" }, lease.leaseId, {
              kind: "observe",
              app: "Fixture",
            })
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        yield* Fiber.interrupt(host);
      }),
    ).pipe(Effect.provide(Broker.layer.pipe(Layer.provideMerge(persistence))));
  },
);
