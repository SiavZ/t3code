import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import {
  CoordinationError,
  CoordinationReadInput,
  CoordinationWriteInput,
  type CoordinationPlan,
  type ThreadUnattendedAuthority,
} from "@t3tools/contracts";
import { validateGraph } from "./coordinationGraph.ts";
import {
  CoordinationMailboxReadInput,
  CoordinationMailboxWriteInput,
  type CoordinationMailbox,
} from "@t3tools/contracts";

const decodeMailboxRead = Schema.decodeUnknownEffect(CoordinationMailboxReadInput);
const decodeMailboxWrite = Schema.decodeUnknownEffect(CoordinationMailboxWriteInput);
const decodeRead = Schema.decodeUnknownEffect(CoordinationReadInput);
const decodeWrite = Schema.decodeUnknownEffect(CoordinationWriteInput);

/** The engine implementation must serialize authorization, replay fingerprints,
 * plan projection events and ordinary worker dispatch/stop events atomically.
 * A split SQL-write then workers.spawn implementation is not a valid store. */
export class CoordinationPlanStore extends Context.Service<
  CoordinationPlanStore,
  {
    readonly mailboxRead: (
      input: CoordinationMailboxReadInput,
    ) => Effect.Effect<CoordinationMailbox, CoordinationError>;
    readonly mailboxWrite: (
      input: CoordinationMailboxWriteInput,
    ) => Effect.Effect<CoordinationMailbox, CoordinationError>;
    readonly read: (
      input: CoordinationReadInput,
    ) => Effect.Effect<CoordinationPlan, CoordinationError>;
    readonly execute: (
      input: CoordinationWriteInput,
      trustedAuthority?: ThreadUnattendedAuthority,
    ) => Effect.Effect<CoordinationPlan, CoordinationError>;
  }
>()("t3/orchestration/CoordinationPlanStore") {}

export class CoordinationPlans extends Context.Service<
  CoordinationPlans,
  {
    readonly mailboxRead: (
      input: CoordinationMailboxReadInput,
    ) => Effect.Effect<CoordinationMailbox, CoordinationError>;
    readonly mailboxWrite: (
      input: CoordinationMailboxWriteInput,
    ) => Effect.Effect<CoordinationMailbox, CoordinationError>;
    readonly read: (
      input: CoordinationReadInput,
    ) => Effect.Effect<CoordinationPlan, CoordinationError>;
    readonly write: (
      input: CoordinationWriteInput,
      trustedAuthority?: ThreadUnattendedAuthority,
    ) => Effect.Effect<CoordinationPlan, CoordinationError>;
  }
>()("t3/orchestration/CoordinationPlans") {}

const invalid = () =>
  new CoordinationError({ code: "invalid", detail: "Invalid bounded coordination input." });
const make = Effect.gen(function* () {
  const store = yield* CoordinationPlanStore;
  return CoordinationPlans.of({
    mailboxRead: Effect.fn("CoordinationPlans.mailboxRead")(function* (input) {
      const decoded = yield* decodeMailboxRead(input).pipe(Effect.mapError(invalid));
      return yield* store.mailboxRead(decoded);
    }),
    mailboxWrite: Effect.fn("CoordinationPlans.mailboxWrite")(function* (input) {
      const decoded = yield* decodeMailboxWrite(input).pipe(Effect.mapError(invalid));
      return yield* store.mailboxWrite(decoded);
    }),
    read: Effect.fn("CoordinationPlans.read")(function* (input) {
      const decoded = yield* decodeRead(input).pipe(Effect.mapError(invalid));
      return yield* store.read(decoded);
    }),
    write: Effect.fn("CoordinationPlans.write")(function* (input, trustedAuthority) {
      const decoded = yield* decodeWrite(input).pipe(Effect.mapError(invalid));
      if (decoded.operation !== "complete" && decoded.callerThreadId !== decoded.rootThreadId) {
        return yield* new CoordinationError({
          code: "forbidden",
          detail: "Only the root may mutate plan control state.",
        });
      }
      if (decoded.operation === "create") {
        yield* Effect.try({
          try: () => validateGraph(decoded.nodes, decoded.policy),
          catch: (cause) => (cause instanceof CoordinationError ? cause : invalid()),
        });
      }
      if (decoded.operation === "complete" && decoded.callerThreadId !== decoded.workerThreadId) {
        return yield* new CoordinationError({
          code: "forbidden",
          detail: "Artifact caller must be the assigned worker.",
        });
      }
      if (JSON.stringify(decoded).length > 262_144) return yield* invalid();
      return yield* store.execute(decoded, trustedAuthority);
    }),
  });
});
export const layer = Layer.effect(CoordinationPlans, make);
