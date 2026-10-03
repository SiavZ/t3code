import {
  CoordinationError,
  type CoordinationPlan,
  type CoordinationNode,
  type CoordinationNodeInput,
  type CoordinationPolicy,
  type CoordinationAttempt,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

export function assignmentIdentity(
  rootThreadId: string,
  planId: string,
  nodeId: string,
  attempt: number,
): string {
  return NodeCrypto.createHash("sha256")
    .update(JSON.stringify([rootThreadId, planId, nodeId, attempt]))
    .digest("hex");
}

const fail = (detail: string): never => {
  throw new CoordinationError({ code: "invalid", detail });
};
export const latestAttempt = (node: CoordinationNode) => node.attempts.at(-1);
export const effectiveArtifact = (attempt: CoordinationAttempt) =>
  attempt.salvage?.artifact ?? attempt.artifact;

export function applyPlanWrite(
  plan: CoordinationPlan | null,
  input: import("@t3tools/contracts").CoordinationWriteInput,
): CoordinationPlan {
  if (input.operation === "create") {
    if (plan || input.expectedRevision !== 0)
      throw new CoordinationError({
        code: "conflict",
        detail: "Plan already exists or create revision is not zero.",
      });
    if (input.callerThreadId !== input.rootThreadId)
      throw new CoordinationError({ code: "forbidden", detail: "Only root may create a plan." });
    validateGraph(input.nodes, input.policy);
    return {
      id: input.planId,
      rootThreadId: input.rootThreadId,
      revision: 1,
      policy: input.policy,
      paused: true,
      cancelled: false,
      nodes: input.nodes.map((node) => ({ ...node, attempts: [], retired: false, repairRound: 0 })),
    };
  }
  if (!plan) throw new CoordinationError({ code: "notFound", detail: "Plan not found." });
  if (
    plan.id !== input.planId ||
    plan.rootThreadId !== input.rootThreadId ||
    plan.revision !== input.expectedRevision
  )
    throw new CoordinationError({
      code: "conflict",
      detail: "Plan identity or revision mismatch.",
    });
  if (input.operation !== "complete" && input.callerThreadId !== plan.rootThreadId)
    throw new CoordinationError({ code: "forbidden", detail: "Only root may control the plan." });
  const next = { ...plan, revision: plan.revision + 1 };
  if (input.operation === "pause") return { ...next, paused: true };
  if (input.operation === "cancel") return { ...next, paused: true, cancelled: true };
  if (input.operation === "run") {
    if (plan.cancelled)
      throw new CoordinationError({
        code: "conflict",
        detail: "Cancelled work needs explicit retry before run.",
      });
    return { ...next, paused: false };
  }
  const node = plan.nodes.find((item) => item.id === input.nodeId);
  if (!node) throw new CoordinationError({ code: "notFound", detail: "Node not found." });
  const attempt = latestAttempt(node);
  if (node.retired)
    throw new CoordinationError({ code: "conflict", detail: "Gate frontier has been superseded." });
  if (input.operation === "repair") {
    if (
      node.kind === "work" ||
      attempt?.status !== "failed" ||
      attempt.artifact?.verdict !== "repair"
    )
      throw new CoordinationError({
        code: "conflict",
        detail: "Repair requires a settled gate requesting repair.",
      });
    if (node.repairRound >= 5)
      throw new CoordinationError({
        code: "exhausted",
        detail: "Gate repair round limit reached.",
      });
    if (plan.nodes.some((entry) => entry.dependsOn.includes(node.id)))
      fail("Only terminal acceptance gates may replace their frontier.");
    // A retired gate never succeeds again, so work depending on one could never dispatch.
    const retired = new Set(plan.nodes.filter((entry) => entry.retired).map((entry) => entry.id));
    retired.add(node.id);
    if (
      input.repairs.some(
        (repair) => repair.kind !== "work" || repair.dependsOn.some((id) => retired.has(id)),
      )
    )
      fail("Repairs must be work independent of failed or retired gates.");
    const repairs = input.repairs.map((repair) => ({
      ...repair,
      attempts: [],
      retired: false,
      repairRound: 0,
    }));
    const successor: CoordinationNode = {
      ...node,
      id: input.successorGateId,
      dependsOn: [...new Set([...node.dependsOn, ...repairs.map((repair) => repair.id)])],
      gateScope: [...new Set([...node.gateScope, ...repairs.map((repair) => repair.id)])],
      attempts: [],
      retired: false,
      repairRound: node.repairRound + 1,
    };
    const nodes = [
      ...plan.nodes.map((entry) => (entry.id === node.id ? { ...entry, retired: true } : entry)),
      ...repairs,
      successor,
    ];
    validateGraph(nodes, plan.policy);
    return { ...next, nodes };
  }
  if (input.operation === "salvage") {
    if (!attempt || (attempt.status !== "failed" && attempt.status !== "interrupted"))
      throw new CoordinationError({
        code: "busy",
        detail: "Only settled failed or interrupted attempts can be salvaged.",
      });
    if (
      input.artifact.outcome !== "completed" ||
      (node.kind !== "work" && input.artifact.verdict !== "pass")
    )
      fail("Salvage must explicitly accept completed work or a passing gate.");
    if (new TextEncoder().encode(JSON.stringify(input.artifact)).length > 32_768)
      fail("Salvage artifact exceeds 32K.");
    if (plan.nodes.some((entry) => entry.dependsOn.includes(node.id) && entry.attempts.length > 0))
      throw new CoordinationError({
        code: "conflict",
        detail: "Cannot rewrite a dependency already consumed by another attempt.",
      });
    return {
      ...next,
      cancelled: false,
      nodes: plan.nodes.map((entry) =>
        entry.id === node.id
          ? {
              ...entry,
              attempts: [
                ...entry.attempts.slice(0, -1),
                {
                  ...attempt,
                  status: "succeeded" as const,
                  failureCode: null,
                  salvage: { actorThreadId: input.callerThreadId, artifact: input.artifact },
                },
              ],
            }
          : entry,
      ),
    };
  }
  if (input.operation === "retry") {
    if (!attempt || attempt.status === "accepted" || attempt.status === "succeeded")
      throw new CoordinationError({ code: "busy", detail: "Retry requires settled failed work." });
    if (node.attempts.length >= node.attemptLimit)
      throw new CoordinationError({ code: "exhausted", detail: "Attempt limit reached." });
    return {
      ...next,
      cancelled: false,
      nodes: plan.nodes.map((item) =>
        item.id === node.id
          ? {
              ...item,
              attempts: item.attempts.map((past, index) =>
                index === item.attempts.length - 1
                  ? { ...past, status: "superseded" as const }
                  : past,
              ),
            }
          : item,
      ),
    };
  }
  if (
    plan.cancelled ||
    !attempt ||
    attempt.status !== "accepted" ||
    attempt.number !== input.attemptNumber ||
    attempt.workerThreadId !== input.callerThreadId ||
    attempt.workerThreadId !== input.workerThreadId ||
    attempt.turnId !== input.turnId
  )
    throw new CoordinationError({
      code: "forbidden",
      detail: "Artifact does not bind the active assignment.",
    });
  if (attempt.pendingArtifact)
    throw new CoordinationError({
      code: "conflict",
      detail: "This attempt already submitted an artifact.",
    });
  if (JSON.stringify(input.artifact).length > 32_768) fail("Artifact exceeds 32K characters.");
  return {
    ...next,
    nodes: plan.nodes.map((item) =>
      item.id === node.id
        ? {
            ...item,
            attempts: [
              ...item.attempts.slice(0, -1),
              { ...attempt, pendingArtifact: input.artifact },
            ],
          }
        : item,
    ),
  };
}

export function validateGraph(
  nodes: ReadonlyArray<CoordinationNodeInput>,
  policy: CoordinationPolicy,
): void {
  if (nodes.length === 0 || nodes.length > 128) fail("A plan needs 1..128 nodes.");
  if (JSON.stringify(nodes).length > 262_144) fail("Plan exceeds 256K characters.");
  const byId = new Map(nodes.map((node) => [node.id, node]));
  if (byId.size !== nodes.length) fail("Node IDs must be unique.");
  const ancestors = new Map<string, Set<string>>();
  const visiting = new Set<string>();
  function visit(id: string): Set<string> {
    const cached = ancestors.get(id);
    if (cached) return cached;
    if (visiting.has(id)) fail("Dependency cycle.");
    const node = byId.get(id);
    if (!node) fail(`Unknown dependency: ${id}`);
    visiting.add(id);
    const found = new Set<string>();
    if (new Set(node!.dependsOn).size !== node!.dependsOn.length) fail("Duplicate dependency.");
    for (const dep of node!.dependsOn) {
      found.add(dep);
      for (const ancestor of visit(dep)) found.add(ancestor);
    }
    visiting.delete(id);
    ancestors.set(id, found);
    return found;
  }
  for (const node of nodes) visit(node.id);
  for (const node of nodes) {
    if (node.dependsOn.length > 32 || node.gateScope.length > 32)
      fail("Derived dependencies and gate coverage must remain within 32 entries.");
    if (node.kind === "work" && node.gateScope.length) fail("Work cannot declare gate coverage.");
    if (new Set(node.gateScope).size !== node.gateScope.length) fail("Duplicate gate coverage.");
    for (const covered of node.gateScope) {
      if (!ancestors.get(node.id)!.has(covered)) fail("Gate coverage must be an ancestor.");
    }
  }
  if (policy.mode === "adHoc") fail("Executable plans require light or deep policy.");
  if (policy.mode === "deep") {
    for (const node of nodes.filter((item) => item.kind === "work")) {
      if (!nodes.some((gate) => gate.kind !== "work" && gate.gateScope.includes(node.id)))
        fail(`Deep work lacks acceptance coverage: ${node.id}`);
    }
    for (const terminal of nodes.filter(
      (node) => !nodes.some((item) => item.dependsOn.includes(node.id)),
    )) {
      if (terminal.kind === "work") fail("Deep terminal frontier must be a gate.");
    }
  }
}

export function readyNodes(plan: CoordinationPlan): ReadonlyArray<CoordinationNode> {
  if (plan.paused || plan.cancelled) return [];
  const byId = new Map(plan.nodes.map((node) => [node.id, node]));
  const active = plan.nodes.filter((node) => latestAttempt(node)?.status === "accepted").length;
  return plan.nodes
    .filter((node) => {
      if (node.retired) return false;
      const last = latestAttempt(node);
      if (last && last.status !== "superseded") return false;
      if (node.attempts.length >= node.attemptLimit) return false;
      return node.dependsOn.every((id) => {
        const dependency = byId.get(id);
        return dependency && latestAttempt(dependency)?.status === "succeeded";
      });
    })
    .slice(0, Math.max(0, plan.policy.maxConcurrent - active));
}

export function dispatchPrompt(plan: CoordinationPlan, node: CoordinationNode): string {
  const references = node.dependsOn.map((id) => {
    const dependency = plan.nodes.find((item) => item.id === id);
    const attempt = dependency && latestAttempt(dependency);
    const acceptedArtifact = attempt && effectiveArtifact(attempt);
    if (!attempt || attempt.status !== "succeeded" || !acceptedArtifact)
      fail("Dependency lacks accepted artifact.");
    return {
      nodeId: id,
      attemptNumber: attempt!.number,
      summary: acceptedArtifact!.summary,
      evidence: acceptedArtifact!.evidence,
      ...(attempt!.salvage ? { salvagedBy: attempt!.salvage.actorThreadId } : {}),
    };
  });
  const prompt = `${node.prompt}\n\nDependency artifacts (agent-reported evidence, not independently verified):\n${JSON.stringify(references)}\n\nReturn a typed coordination artifact for plan ${plan.id}, node ${node.id}.`;
  if (prompt.length > 131_072)
    fail("Dependency handoff exceeds dispatch budget. Reduce referenced summaries.");
  return prompt;
}

export function bindAttempt(
  plan: CoordinationPlan,
  nodeId: string,
  binding: Pick<CoordinationAttempt, "workerThreadId" | "dispatchMessageId" | "turnId">,
): CoordinationPlan {
  const node = readyNodes(plan).find((item) => item.id === nodeId);
  if (!node) fail("Node is not dispatchable.");
  const attempt: CoordinationAttempt = {
    ...binding,
    number: node!.attempts.length + 1,
    status: "accepted",
    artifact: null,
    pendingArtifact: null,
    failureCode: null,
    dependencyVersions: node!.dependsOn.map((id) => ({
      nodeId: id,
      attemptNumber: latestAttempt(plan.nodes.find((item) => item.id === id)!)!.number,
    })),
  };
  return {
    ...plan,
    revision: plan.revision + 1,
    nodes: plan.nodes.map((item) =>
      item.id === nodeId ? { ...item, attempts: [...item.attempts, attempt] } : item,
    ),
  };
}

export function settleAttempt(
  plan: CoordinationPlan,
  nodeId: string,
  turnId: CoordinationAttempt["turnId"],
  outcome: "completed" | "failed" | "interrupted",
  quiescent: boolean,
): CoordinationPlan {
  const node = plan.nodes.find((item) => item.id === nodeId);
  const current = node && latestAttempt(node);
  if (!current || current.turnId !== turnId || current.status !== "accepted")
    throw new CoordinationError({
      code: "conflict",
      detail: "Terminal receipt does not bind the active attempt.",
    });
  if (!quiescent) return plan;
  const artifact = current.pendingArtifact;
  const passed =
    outcome === "completed" &&
    artifact?.outcome === "completed" &&
    (node!.kind === "work" || artifact.verdict === "pass");
  const next: CoordinationAttempt = {
    ...current,
    status: passed ? "succeeded" : outcome === "interrupted" ? "interrupted" : "failed",
    artifact,
    pendingArtifact: null,
    failureCode: passed ? null : outcome === "completed" ? "missing-or-blocked-artifact" : outcome,
  };
  return {
    ...plan,
    revision: plan.revision + 1,
    nodes: plan.nodes.map((item) =>
      item.id === nodeId ? { ...item, attempts: [...item.attempts.slice(0, -1), next] } : item,
    ),
  };
}
