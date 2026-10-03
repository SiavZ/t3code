import { WS_METHODS } from "@t3tools/contracts";
import type { EnvironmentRegistry } from "../connection/registry.ts";
import { Atom } from "effect/unstable/reactivity";
import { createEnvironmentRpcCommand, createEnvironmentRpcQueryAtomFamily } from "./runtime.ts";

/** Documents remain environment-local and reuse the authenticated connection transport. */
export function createAgentDocumentsEnvironmentAtoms<R, E>(
  runtime: Atom.AtomRuntime<EnvironmentRegistry | R, E>,
) {
  return {
    prepareAsset: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:agent-documents:prepare-asset",
      tag: WS_METHODS.agentDocumentsPrepareAsset,
    }),
    read: createEnvironmentRpcQueryAtomFamily(runtime, {
      label: "environment-data:agent-documents:read",
      tag: WS_METHODS.agentDocumentsRead,
      staleTimeMs: 0,
    }),
    write: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:agent-documents:write",
      tag: WS_METHODS.agentDocumentsWrite,
    }),
    action: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:agent-documents:action",
      tag: WS_METHODS.agentDocumentsAction,
    }),
    wait: createEnvironmentRpcCommand(runtime, {
      label: "environment-data:agent-documents:wait",
      tag: WS_METHODS.agentDocumentsWait,
    }),
  };
}
