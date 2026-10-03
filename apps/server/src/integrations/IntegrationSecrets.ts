import { Context, Effect, Layer, Option, Schema } from "effect";
import * as NodeCrypto from "node:crypto";
const { randomUUID } = NodeCrypto;
import * as Store from "../auth/ServerSecretStore.ts";
export const IntegrationSecretPurpose = Schema.Literals([
  "gmail-client",
  "gmail-token",
  "images",
  "build",
  "browser",
]);
export const isIntegrationSecretRef = (ref: string) =>
  /^integration-(gmail-client|gmail-token|images|build|browser)-[a-z0-9-]{1,100}$/.test(ref);
export const isIntegrationSecretRefFor = (
  ref: string,
  purpose: typeof IntegrationSecretPurpose.Type,
) => isIntegrationSecretRef(ref) && ref.startsWith(`integration-${purpose}-`);
export class IntegrationSecretError extends Schema.TaggedError<IntegrationSecretError>()(
  "IntegrationSecretError",
  { reason: Schema.Literals(["invalid-reference", "storage"]) },
) {}
/** Human environment-admin transport only. No raw secret reads are exposed. */
export class IntegrationSecrets extends Context.Service<
  IntegrationSecrets,
  {
    readonly store: (
      purpose: typeof IntegrationSecretPurpose.Type,
      value: string,
    ) => Effect.Effect<string, IntegrationSecretError>;
    readonly remove: (reference: string) => Effect.Effect<void, IntegrationSecretError>;
    readonly configured: (reference: string) => Effect.Effect<boolean, IntegrationSecretError>;
  }
>()("t3/integrations/IntegrationSecrets") {}
export const layer = Layer.effect(
  IntegrationSecrets,
  Effect.gen(function* () {
    const store = yield* Store.ServerSecretStore;
    const error = () => new IntegrationSecretError({ reason: "storage" });
    const valid = (reference: string) =>
      isIntegrationSecretRef(reference)
        ? Effect.void
        : Effect.fail(new IntegrationSecretError({ reason: "invalid-reference" }));
    return IntegrationSecrets.of({
      store: (purpose, value) =>
        Effect.gen(function* () {
          if (!Schema.is(IntegrationSecretPurpose)(purpose) || !value || value.length > 32_000)
            return yield* Effect.fail(new IntegrationSecretError({ reason: "invalid-reference" }));
          const ref = `integration-${purpose}-${randomUUID()}`;
          yield* store.create(ref, Buffer.from(value)).pipe(Effect.mapError(error));
          return ref;
        }),
      remove: (ref) =>
        valid(ref).pipe(Effect.andThen(store.remove(ref).pipe(Effect.mapError(error)))),
      configured: (ref) =>
        valid(ref).pipe(
          Effect.andThen(store.get(ref).pipe(Effect.map(Option.isSome), Effect.mapError(error))),
        ),
    });
  }),
);
