import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { ProjectId, ThreadId } from "@t3tools/contracts";
import * as ProcessRunner from "../processRunner.ts";
import * as ChildProcessSpawner from "effect/unstable/process/ChildProcessSpawner";
import * as Hooks from "./RuntimeHooks.ts";
import migrate from "../persistence/Migrations/063_RuntimeOperations.ts";
let calls = 0;
const runner = Layer.succeed(ProcessRunner.ProcessRunner, {
  run: (input) =>
    Effect.sync(() => {
      calls++;
      return {
        stdout: "secret output",
        stderr: "",
        code: ChildProcessSpawner.ExitCode(input.command === "deny" ? 1 : 0),
        timedOut: input.command === "timeout",
        stdoutTruncated: false,
        stderrTruncated: false,
        stdoutInvalidUtf8: false,
        stderrInvalidUtf8: false,
      };
    }),
});
const services = Hooks.layer.pipe(
  Layer.provideMerge(NodeSqliteClient.layer({ filename: ":memory:" })),
  Layer.provide(runner),
);
const projectId = ProjectId.make("hooks-project");
const authority = { projectId, trustedOperator: true };
const context = {
  projectId,
  threadId: ThreadId.make("thread"),
  event: "host-tool.before" as const,
  receiptId: "receipt",
};
const hook = {
  id: "gate",
  projectId,
  enabled: true,
  event: "host-tool.before" as const,
  command: "deny",
  args: [],
  timeoutMs: 100,
  failurePolicy: "closed" as const,
  coverage: "host-tools" as const,
};
it.layer(services)("RuntimeHooks", (it) => {
  it.effect("denies side effects and keeps denial on retry without reexecuting", () =>
    Effect.gen(function* () {
      yield* migrate;
      const hooks = yield* Hooks.RuntimeHooks;
      calls = 0;
      yield* hooks.configure(hook, authority);
      let sideEffects = 0;
      const tool = hooks
        .beforeTool(context)
        .pipe(Effect.tap(() => Effect.sync(() => sideEffects++)));
      assert.equal((yield* tool.pipe(Effect.flip)).code, "denied");
      assert.equal((yield* tool.pipe(Effect.flip)).code, "denied");
      assert.equal(calls, 1);
      assert.equal(sideEffects, 0);
    }),
  );
  it.effect(
    "requires trusted configuration, bounds observer errors, prevents recursive execution and removes",
    () =>
      Effect.gen(function* () {
        yield* migrate;
        const hooks = yield* Hooks.RuntimeHooks;
        assert.equal(
          (yield* hooks.configure(hook, { ...authority, trustedOperator: false }).pipe(Effect.flip))
            .code,
          "forbidden",
        );
        yield* hooks.configure(
          { ...hook, event: "turn.end", command: "timeout", failurePolicy: "open" },
          authority,
        );
        const results = yield* hooks.observe({
          ...context,
          event: "turn.end",
          receiptId: "observer",
        });
        assert.equal(results[0]?.outcome, "failed");
        assert.ok(!JSON.stringify(results).includes("secret output"));
        const before = calls;
        assert.deepEqual(yield* hooks.observe({ ...context, recursive: true }), []);
        assert.equal(calls, before);
        yield* hooks.remove(hook.id, authority);
        assert.deepEqual(yield* hooks.list(authority), []);
      }),
  );
});
