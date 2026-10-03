import {
  CommandId,
  ProviderInstanceId,
  ThreadId,
  MessageId,
  TurnId,
  type CoordinationArtifact,
  type CoordinationNodeInput,
} from "@t3tools/contracts";
import { describe, expect, it } from "@effect/vitest";
import {
  applyPlanWrite,
  bindAttempt,
  dispatchPrompt,
  readyNodes,
  settleAttempt,
  validateGraph,
} from "./coordinationGraph.ts";

const root = ThreadId.make("root");
const target = { callerThreadId: root, rootThreadId: root, planId: "graph" };
const commandId = CommandId.make("test");
const policy = { mode: "deep" as const, maxConcurrent: 1, retainWorkers: true };
const work = (id: string): CoordinationNodeInput => ({
  id,
  kind: "work",
  dependsOn: [],
  gateScope: [],
  attemptLimit: 2,
  prompt: id,
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test" },
});
const artifact: CoordinationArtifact = {
  version: 1,
  summary: "Reported result",
  findings: [],
  evidence: [],
  validation: { status: "notRun", detail: "Not verified" },
  unchecked: [],
  confidence: "medium",
  outcome: "completed",
};
const binding = {
  workerThreadId: ThreadId.make("worker"),
  dispatchMessageId: MessageId.make("message"),
  turnId: TurnId.make("turn"),
};
const create = (nodes: CoordinationNodeInput[], mode: "light" | "deep" = "deep") => {
  let plan = applyPlanWrite(null, {
    ...target,
    commandId,
    expectedRevision: 0,
    operation: "create",
    policy: { ...policy, mode },
    nodes,
  });
  return applyPlanWrite(plan, {
    ...target,
    commandId,
    expectedRevision: plan.revision,
    operation: "run",
  });
};
const accept = (plan: ReturnType<typeof create>, nodeId: string, report: CoordinationArtifact) => {
  plan = bindAttempt(plan, nodeId, binding);
  plan = applyPlanWrite(plan, {
    ...target,
    callerThreadId: binding.workerThreadId,
    commandId,
    expectedRevision: plan.revision,
    operation: "complete",
    nodeId,
    attemptNumber: 1,
    ...binding,
    artifact: report,
  });
  return settleAttempt(plan, nodeId, binding.turnId, "completed", true);
};

describe("coordination pure frontier", () => {
  it("rejects cycles, missing dependencies, ungated work and oversized derived coverage", () => {
    expect(() =>
      validateGraph([{ ...work("a"), dependsOn: ["a"] }], { ...policy, mode: "light" }),
    ).toThrow();
    expect(() =>
      validateGraph([{ ...work("a"), dependsOn: ["missing"] }], { ...policy, mode: "light" }),
    ).toThrow();
    expect(() => validateGraph([work("a")], policy)).toThrow();
    const nodes = Array.from({ length: 33 }, (_, index) => work(`w${index}`));
    expect(() =>
      validateGraph(
        [
          ...nodes,
          {
            ...work("gate"),
            kind: "verify",
            dependsOn: nodes.map((node) => node.id),
            gateScope: nodes.map((node) => node.id),
          },
        ],
        policy,
      ),
    ).toThrow();
  });
  it("replaces a failed gate with an immutable repair frontier and gates repaired work", () => {
    let plan = create([
      work("task"),
      { ...work("verify"), kind: "verify", dependsOn: ["task"], gateScope: ["task"] },
    ]);
    plan = accept(plan, "task", artifact);
    plan = accept(plan, "verify", { ...artifact, verdict: "repair" });
    expect(plan.nodes[1]!.attempts[0]!.status).toBe("failed");
    const failed = plan.nodes[1]!.attempts[0]!;
    plan = applyPlanWrite(plan, {
      ...target,
      commandId,
      expectedRevision: plan.revision,
      operation: "repair",
      nodeId: "verify",
      successorGateId: "verify-2",
      repairs: [{ ...work("repair"), dependsOn: ["task"] }],
    });
    expect(plan.nodes[1]!.retired).toBe(true);
    expect(plan.nodes[1]!.attempts[0]).toEqual(failed);
    expect(readyNodes(plan).map((node) => node.id)).toEqual(["repair"]);
    plan = accept(plan, "repair", artifact);
    expect(readyNodes(plan).map((node) => node.id)).toEqual(["verify-2"]);
    expect(dispatchPrompt(plan, plan.nodes.at(-1)!)).toContain("repair");
    plan = accept(plan, "verify-2", { ...artifact, verdict: "pass" });
    expect(plan.nodes.at(-1)!.attempts[0]!.status).toBe("succeeded");
    expect(readyNodes(plan)).toEqual([]);
  });
  it("rejects repair work that depends on an earlier retired gate", () => {
    let plan = create([
      work("task"),
      { ...work("verify"), kind: "verify", dependsOn: ["task"], gateScope: ["task"] },
    ]);
    plan = accept(plan, "task", artifact);
    plan = accept(plan, "verify", { ...artifact, verdict: "repair" });
    const repair = (gate: string, successorGateId: string, id: string, dependsOn: string[]) =>
      applyPlanWrite(plan, {
        ...target,
        commandId,
        expectedRevision: plan.revision,
        operation: "repair",
        nodeId: gate,
        successorGateId,
        repairs: [{ ...work(id), dependsOn }],
      });
    plan = repair("verify", "verify-2", "fix-1", ["task"]);
    plan = accept(plan, "fix-1", artifact);
    plan = accept(plan, "verify-2", { ...artifact, verdict: "repair" });
    const before = plan;
    expect(() => repair("verify-2", "verify-3", "fix-2", ["verify"])).toThrow(
      expect.objectContaining({ detail: expect.stringMatching(/retired gates/) }),
    );
    expect(plan).toBe(before);
    plan = repair("verify-2", "verify-3", "fix-2", ["fix-1"]);
    expect(readyNodes(plan).map((node) => node.id)).toEqual(["fix-2"]);
  });
  it("salvages only settled work and preserves the original artifact with root provenance", () => {
    let plan = create([work("task"), { ...work("next"), dependsOn: ["task"] }], "light");
    plan = accept(plan, "task", { ...artifact, outcome: "blocked" });
    const original = plan.nodes[0]!.attempts[0]!.artifact;
    plan = applyPlanWrite(plan, {
      ...target,
      commandId,
      expectedRevision: plan.revision,
      operation: "salvage",
      nodeId: "task",
      artifact,
    });
    expect(plan.nodes[0]!.attempts[0]!.artifact).toEqual(original);
    expect(plan.nodes[0]!.attempts[0]!.salvage?.actorThreadId).toBe(root);
    expect(dispatchPrompt(plan, plan.nodes[1]!)).toContain("salvagedBy");
    expect(readyNodes(plan).map((node) => node.id)).toEqual(["next"]);
    expect(() =>
      applyPlanWrite(plan, {
        ...target,
        commandId,
        expectedRevision: plan.revision,
        operation: "retry",
        nodeId: "task",
      }),
    ).toThrow();
  });
  it("bounds retries and does not accept reports or retry before native quiescence", () => {
    let plan = create([work("task")], "light");
    plan = bindAttempt(plan, "task", binding);
    expect(() =>
      applyPlanWrite(plan, {
        ...target,
        commandId,
        expectedRevision: plan.revision,
        operation: "retry",
        nodeId: "task",
      }),
    ).toThrow();
    expect(settleAttempt(plan, "task", binding.turnId, "failed", false)).toEqual(plan);
    plan = settleAttempt(plan, "task", binding.turnId, "failed", true);
    plan = applyPlanWrite(plan, {
      ...target,
      commandId,
      expectedRevision: plan.revision,
      operation: "retry",
      nodeId: "task",
    });
    plan = bindAttempt(plan, "task", binding);
    plan = settleAttempt(plan, "task", binding.turnId, "failed", true);
    expect(() =>
      applyPlanWrite(plan, {
        ...target,
        commandId,
        expectedRevision: plan.revision,
        operation: "retry",
        nodeId: "task",
      }),
    ).toThrow();
  });
});
