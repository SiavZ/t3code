import { Context, Deferred, Effect, Layer, Queue, Schema, Stream } from "effect";
import type * as Cause from "effect/Cause";
import * as NodeCrypto from "node:crypto";
const { randomUUID } = NodeCrypto;
import {
  type DesktopAction,
  type DesktopHost,
  type DesktopLease,
} from "../../../../packages/contracts/src/desktopAutomation.ts";
import * as Approvals from "./WorkflowApprovals.ts";

export class DesktopAutomationError extends Schema.TaggedError<DesktopAutomationError>()(
  "DesktopAutomationError",
  {
    reason: Schema.Literals([
      "host-unavailable",
      "consent-required",
      "stale-lease",
      "scope-denied",
      "unsupported-action",
      "disconnected",
      "execution",
      "timeout",
    ]),
  },
) {}
export interface DesktopAutomationRequest {
  readonly requestId: string;
  readonly leaseId: string;
  readonly action: DesktopAction;
}
export class DesktopAutomationBroker extends Context.Service<
  DesktopAutomationBroker,
  {
    /** Authenticated desktop host registration only, never agent supplied. */
    readonly connect: (host: DesktopHost) => Effect.Effect<Stream.Stream<DesktopAutomationRequest>>;
    readonly connectForClient: (
      host: DesktopHost,
      humanSessionId: string,
      environmentId: string,
    ) => Effect.Effect<Stream.Stream<DesktopAutomationRequest>, DesktopAutomationError>;
    readonly disconnectForClient: (
      hostId: string,
      generation: string,
      humanSessionId: string,
    ) => Effect.Effect<void, DesktopAutomationError>;
    readonly authorizeForClient: (
      input: { readonly hostId: string; readonly generation: string; readonly requestId: string },
      humanSessionId: string,
    ) => Effect.Effect<boolean>;
    readonly respondForClient: (
      input: {
        readonly hostId: string;
        readonly generation: string;
        readonly requestId: string;
        readonly result: unknown;
        readonly failed?: boolean | undefined;
      },
      humanSessionId: string,
    ) => Effect.Effect<void, DesktopAutomationError>;
    readonly disconnect: (hostId: string, generation: string) => Effect.Effect<void>;
    readonly hosts: (environmentId: string) => Effect.Effect<ReadonlyArray<DesktopHost>>;
    readonly lease: (input: {
      readonly hostId: string;
      readonly environmentId: string;
      readonly threadId: string;
      readonly app: string;
      readonly approvalId: string;
    }) => Effect.Effect<DesktopLease, DesktopAutomationError>;
    readonly revoke: (leaseId: string) => Effect.Effect<void>;
    readonly invoke: (
      scope: { readonly environmentId: string; readonly threadId: string },
      leaseId: string,
      action: DesktopAction,
    ) => Effect.Effect<unknown, DesktopAutomationError>;
    readonly respond: (
      hostId: string,
      generation: string,
      requestId: string,
      result: unknown,
      failed?: boolean,
    ) => Effect.Effect<void, DesktopAutomationError>;
  }
>()("t3/integrations/DesktopAutomationBroker") {}

const make = Effect.gen(function* () {
  const approvals = yield* Approvals.WorkflowApprovals;
  const hosts = new Map<
    string,
    { host: DesktopHost; queue: Queue.Queue<DesktopAutomationRequest, Cause.Done> }
  >();
  const owners = new Map<string, string>();
  const leases = new Map<string, DesktopLease>();
  const pending = new Map<
    string,
    {
      hostId: string;
      generation: string;
      leaseId: string;
      deferred: Deferred.Deferred<unknown, DesktopAutomationError>;
    }
  >();
  const fail = (reason: DesktopAutomationError["reason"]) => new DesktopAutomationError({ reason });
  const revoke = (leaseId: string) =>
    Effect.gen(function* () {
      leases.delete(leaseId);
      for (const [id, entry] of pending)
        if (entry.leaseId === leaseId) {
          pending.delete(id);
          yield* Deferred.fail(entry.deferred, fail("disconnected"));
        }
    });
  const disconnect = (hostId: string, generation: string) =>
    Effect.gen(function* () {
      const entry = hosts.get(hostId);
      if (!entry || entry.host.generation !== generation) return;
      hosts.delete(hostId);
      owners.delete(hostId);
      yield* Queue.shutdown(entry.queue);
      for (const [id, lease] of leases) if (lease.hostId === hostId) yield* revoke(id);
    });
  const service = DesktopAutomationBroker.of({
    connect: (host) =>
      Effect.gen(function* () {
        const previous = hosts.get(host.hostId);
        if (previous) yield* disconnect(host.hostId, previous.host.generation);
        const queue = yield* Queue.unbounded<DesktopAutomationRequest, Cause.Done>();
        hosts.set(host.hostId, { host, queue });
        return Stream.fromQueue(queue).pipe(
          Stream.ensuring(disconnect(host.hostId, host.generation)),
        );
      }),
    connectForClient: (host, session, environmentId) =>
      Effect.gen(function* () {
        if (
          !session ||
          host.environmentId !== environmentId ||
          (owners.has(host.hostId) && owners.get(host.hostId) !== session)
        )
          return yield* Effect.fail(fail("scope-denied"));
        const stream = yield* service.connect(host);
        owners.set(host.hostId, session);
        return stream;
      }),
    disconnectForClient: (hostId, generation, session) =>
      owners.get(hostId) !== session
        ? Effect.fail(fail("scope-denied"))
        : disconnect(hostId, generation),
    authorizeForClient: (input, session) =>
      Effect.sync(() => {
        const request = pending.get(input.requestId);
        const lease = request ? leases.get(request.leaseId) : undefined;
        return (
          owners.get(input.hostId) === session &&
          request?.hostId === input.hostId &&
          request.generation === input.generation &&
          lease !== undefined &&
          lease.expiresAt > Date.now() &&
          hosts.get(input.hostId)?.host.generation === input.generation
        );
      }),
    respondForClient: (input, session) =>
      owners.get(input.hostId) !== session
        ? Effect.fail(fail("scope-denied"))
        : service.respond(
            input.hostId,
            input.generation,
            input.requestId,
            input.result,
            input.failed,
          ),
    disconnect,
    hosts: (environmentId) =>
      Effect.sync(() =>
        [...hosts.values()]
          .filter((entry) => entry.host.environmentId === environmentId)
          .map((entry) => entry.host),
      ),
    lease: (input) =>
      Effect.gen(function* () {
        const entry = hosts.get(input.hostId);
        if (!entry) return yield* Effect.fail(fail("host-unavailable"));
        if (entry.host.environmentId !== input.environmentId)
          return yield* Effect.fail(fail("scope-denied"));
        const { approvalId, ...review } = input;
        yield* approvals
          .consume(
            approvalId,
            "desktop.lease",
            JSON.stringify({ ...review, generation: entry.host.generation }),
          )
          .pipe(Effect.mapError(() => fail("consent-required")));
        const lease = {
          leaseId: randomUUID(),
          hostId: input.hostId,
          generation: entry.host.generation,
          environmentId: input.environmentId,
          threadId: input.threadId,
          app: input.app,
          expiresAt: Date.now() + 300_000,
        };
        leases.set(lease.leaseId, lease);
        return lease;
      }),
    revoke,
    invoke: (scope, leaseId, action) =>
      Effect.gen(function* () {
        const lease = leases.get(leaseId);
        if (!lease || lease.expiresAt < Date.now()) return yield* Effect.fail(fail("stale-lease"));
        if (
          lease.environmentId !== scope.environmentId ||
          lease.threadId !== scope.threadId ||
          action.app !== lease.app
        )
          return yield* Effect.fail(fail("scope-denied"));
        const entry = hosts.get(lease.hostId);
        if (!entry || entry.host.generation !== lease.generation)
          return yield* Effect.fail(fail("host-unavailable"));
        if (!entry.host.operations.includes(action.kind))
          return yield* Effect.fail(fail("unsupported-action"));
        if (pending.size >= 32) return yield* Effect.fail(fail("execution"));
        const requestId = randomUUID();
        const deferred = yield* Deferred.make<unknown, DesktopAutomationError>();
        pending.set(requestId, {
          hostId: lease.hostId,
          generation: lease.generation,
          leaseId,
          deferred,
        });
        yield* Queue.offer(entry.queue, { requestId, leaseId, action });
        return yield* Deferred.await(deferred).pipe(
          Effect.timeout("30 seconds"),
          Effect.mapError((failure) =>
            failure instanceof DesktopAutomationError ? failure : fail("timeout"),
          ),
          Effect.ensuring(
            Effect.sync(() => {
              pending.delete(requestId);
            }),
          ),
        );
      }),
    respond: (hostId, generation, requestId, result, failed) =>
      Effect.gen(function* () {
        const entry = pending.get(requestId);
        if (!entry || entry.hostId !== hostId || entry.generation !== generation)
          return yield* Effect.fail(fail("scope-denied"));
        pending.delete(requestId);
        if (failed) yield* Deferred.fail(entry.deferred, fail("execution"));
        else yield* Deferred.succeed(entry.deferred, result);
      }),
  });
  return service;
});
export const layer = Layer.effect(DesktopAutomationBroker, make);
