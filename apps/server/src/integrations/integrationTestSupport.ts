import { Effect, Layer, Option } from "effect";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import * as Attempts from "./WorkflowAttempts.ts";
import migration from "../persistence/Migrations/064_IntegrationWorkflows.ts";

export const testDatabase = () =>
  Layer.effectDiscard(migration).pipe(
    Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
  );
export const testPersistence = () => {
  const database = testDatabase();
  return Layer.mergeAll(
    database,
    Approvals.layer.pipe(Layer.provide(database)),
    Attempts.layer.pipe(Layer.provide(database)),
  );
};
export const testSecrets = (initial: Readonly<Record<string, string>> = {}) => {
  const values = new Map(Object.entries(initial).map(([key, value]) => [key, Buffer.from(value)]));
  const layer = Layer.succeed(
    Secrets.ServerSecretStore,
    Secrets.ServerSecretStore.of({
      get: (key) => Effect.sync(() => Option.fromUndefinedOr(values.get(key))),
      set: (key, value) =>
        Effect.sync(() => {
          values.set(key, Buffer.from(value));
        }),
      create: (key, value) =>
        Effect.sync(() => {
          values.set(key, Buffer.from(value));
        }),
      getOrCreateRandom: (key, size) =>
        Effect.sync(() => {
          const value = Buffer.alloc(size);
          values.set(key, value);
          return value;
        }),
      remove: (key) =>
        Effect.sync(() => {
          values.delete(key);
        }),
    }),
  );
  return { values, layer };
};
