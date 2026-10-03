import { WS_METHODS } from "@t3tools/contracts";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import type { EnvironmentThreadShell } from "./models.ts";
import { Atom } from "effect/unstable/reactivity";
import { createEnvironmentRpcCommand, createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";
import { ownedWorkersRefreshKey } from "../workers/model.ts";

/** Uses ordinary thread shells only as invalidation signals, never a second worker store. */
export function createOwnedWorkersEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
  shells: Atom.Atom<readonly EnvironmentThreadShell[]>,
) {
  const refreshKey = Atom.family((key: string) => {
    const [environmentId, ownerThreadId] = JSON.parse(key) as [
      EnvironmentThreadShell["environmentId"],
      string,
    ];
    return Atom.make((get) => ownedWorkersRefreshKey(get(shells), environmentId, ownerThreadId));
  });
  return {
    list: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:owned-workers:list",
      tag: WS_METHODS.workersList,
      staleTimeMs: 0,
      refreshTrigger: (target) =>
        refreshKey(JSON.stringify([target.environmentId, target.input.callerThreadId])),
    }),
    get: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:owned-workers:get",
      tag: WS_METHODS.workersGet,
      staleTimeMs: 0,
      refreshTrigger: (target) =>
        refreshKey(JSON.stringify([target.environmentId, target.input.callerThreadId])),
    }),
    stop: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:owned-workers:stop",
      tag: WS_METHODS.workersStop,
    }),
    wait: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:owned-workers:wait",
      tag: WS_METHODS.workersWait,
    }),
  };
}
