import * as Effect from "effect/Effect";
import migrateEvents from "../persistence/Migrations/001_OrchestrationEvents.ts";
import migrateDocuments from "../persistence/Migrations/061_AgentDocuments.ts";

/** Minimal schema for AgentDocuments fixtures. Mounts record the current event-log position. */
export const migrateAgentDocuments = Effect.andThen(migrateEvents, migrateDocuments);
