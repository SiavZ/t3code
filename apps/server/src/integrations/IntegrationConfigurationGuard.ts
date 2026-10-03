import { Context, Effect, Layer, Option } from "effect";
import type { OptionalIntegrations } from "../../../../packages/contracts/src/integrationWorkflows.ts";
import * as Settings from "../serverSettings.ts";
/** A changed configuration is fail-closed until its production layer is rebuilt. */
export class IntegrationConfigurationGuard extends Context.Service<
  IntegrationConfigurationGuard,
  {
    readonly current: (domain: keyof OptionalIntegrations) => Effect.Effect<boolean>;
  }
>()("t3/integrations/IntegrationConfigurationGuard") {}
export const configuredLayer = (initial: OptionalIntegrations) =>
  Layer.effect(
    IntegrationConfigurationGuard,
    Effect.gen(function* () {
      const settings = yield* Effect.serviceOption(Settings.ServerSettingsService);
      return IntegrationConfigurationGuard.of({
        current: (domain) =>
          Option.isNone(settings)
            ? Effect.succeed(true)
            : settings.value.getSettings.pipe(
                Effect.map(
                  (live) =>
                    JSON.stringify(live.optionalIntegrations?.[domain]) ===
                    JSON.stringify(initial[domain]),
                ),
                Effect.catch(() => Effect.succeed(false)),
              ),
      });
    }),
  );
export const makeCurrentCheck = (domain: keyof OptionalIntegrations) =>
  Effect.gen(function* () {
    const guard = yield* Effect.serviceOption(IntegrationConfigurationGuard);
    return () => (Option.isNone(guard) ? Effect.succeed(true) : guard.value.current(domain));
  });
