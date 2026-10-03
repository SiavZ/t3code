import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import type { ProjectId } from "@t3tools/contracts";
import * as H from "../../../../packages/contracts/src/runtimeHooks.ts";
import * as ProcessRunner from "../processRunner.ts";
export interface HookAuthority {
  readonly projectId: ProjectId;
  readonly trustedOperator: boolean;
}
export class RuntimeHooks extends Context.Service<
  RuntimeHooks,
  {
    readonly configure: (
      hook: H.RuntimeHook,
      authority: HookAuthority,
    ) => Effect.Effect<void, H.RuntimeHooksError>;
    readonly remove: (
      id: string,
      authority: HookAuthority,
    ) => Effect.Effect<void, H.RuntimeHooksError>;
    readonly list: (
      authority: HookAuthority,
    ) => Effect.Effect<ReadonlyArray<H.RuntimeHook>, H.RuntimeHooksError>;
    readonly beforeTool: (
      context: H.RuntimeHookContext,
    ) => Effect.Effect<ReadonlyArray<H.RuntimeHookResult>, H.RuntimeHooksError>;
    readonly afterTool: (
      context: H.RuntimeHookContext,
    ) => Effect.Effect<ReadonlyArray<H.RuntimeHookResult>, H.RuntimeHooksError>;
    readonly observe: (
      context: H.RuntimeHookContext,
    ) => Effect.Effect<ReadonlyArray<H.RuntimeHookResult>, H.RuntimeHooksError>;
  }
>()("t3/provider/RuntimeHooks") {}
const error = (code: H.RuntimeHooksError["code"], detail: string) =>
  new H.RuntimeHooksError({ code, detail });
const isHookError = Schema.is(H.RuntimeHooksError);
const encodeHook = Schema.encodeEffect(Schema.fromJsonString(H.RuntimeHook));
const encodeHookResult = Schema.encodeEffect(Schema.fromJsonString(H.RuntimeHookResult));
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const runner = yield* ProcessRunner.ProcessRunner;
  const guard = <A, E>(effect: Effect.Effect<A, E>) =>
    effect.pipe(
      Effect.mapError((e) =>
        isHookError(e) ? e : error("storage", "Runtime hook storage failed."),
      ),
    );
  const decode = <A>(schema: Schema.Codec<A, unknown, never, never>, input: unknown) =>
    Schema.decodeUnknownEffect(schema)(input).pipe(
      Effect.mapError(() => error("invalid", "Invalid runtime hook configuration.")),
    );
  const list = (a: HookAuthority) =>
    guard(
      Effect.gen(function* () {
        const rows = yield* sql<{
          json: string;
        }>`SELECT config_json AS json FROM runtime_hooks WHERE project_id=${a.projectId} ORDER BY hook_id`;
        return yield* Effect.forEach(rows, (row) =>
          decode(Schema.fromJsonString(H.RuntimeHook), row.json),
        );
      }),
    );
  const configure = (raw: H.RuntimeHook, a: HookAuthority) =>
    guard(
      Effect.gen(function* () {
        if (!a.trustedOperator || raw.projectId !== a.projectId)
          return yield* error(
            "forbidden",
            "Hook configuration requires a trusted client operator.",
          );
        const hook = yield* decode(H.RuntimeHook, raw);
        if (hook.event === "host-tool.before" && hook.coverage !== "host-tools")
          return yield* error("invalid", "Native observation cannot gate all native tools.");
        const json = yield* encodeHook(hook);
        yield* sql`INSERT INTO runtime_hooks VALUES(${a.projectId},${hook.id},${json}) ON CONFLICT(project_id,hook_id) DO UPDATE SET config_json=excluded.config_json`;
        yield* sql`DELETE FROM runtime_hook_receipts WHERE project_id=${a.projectId} AND hook_id=${hook.id}`;
      }),
    );
  const remove = (id: string, a: HookAuthority) =>
    guard(
      Effect.gen(function* () {
        if (!a.trustedOperator)
          return yield* error("forbidden", "Hook removal requires a trusted client operator.");
        yield* sql`DELETE FROM runtime_hooks WHERE project_id=${a.projectId} AND hook_id=${id}`;
        yield* sql`DELETE FROM runtime_hook_receipts WHERE project_id=${a.projectId} AND hook_id=${id}`;
      }),
    );
  const run = (raw: H.RuntimeHookContext) =>
    guard(
      Effect.gen(function* () {
        const context = yield* decode(H.RuntimeHookContext, raw);
        if (context.recursive) return [];
        const hooks = (yield* list({
          projectId: context.projectId,
          trustedOperator: false,
        })).filter((h) => h.enabled && h.event === context.event);
        const results: H.RuntimeHookResult[] = [];
        for (const hook of hooks) {
          const rows = yield* sql<{
            json: string;
          }>`SELECT result_json AS json FROM runtime_hook_receipts WHERE project_id=${context.projectId} AND hook_id=${hook.id} AND receipt_id=${context.receiptId}`;
          if (rows[0]) {
            results.push(yield* decode(Schema.fromJsonString(H.RuntimeHookResult), rows[0].json));
            continue;
          }
          // Only bounded metadata reaches stdin. Raw arguments, secrets and transcript bodies are omitted.
          const execution = yield* runner
            .run({
              command: hook.command,
              args: hook.args,
              stdin: JSON.stringify(context),
              timeout: hook.timeoutMs,
              maxOutputBytes: 4096,
              outputMode: "truncate",
              timeoutBehavior: "timedOutResult",
            })
            .pipe(Effect.result);
          const success =
            execution._tag === "Success" &&
            execution.success.code === 0 &&
            !execution.success.timedOut;
          const result: H.RuntimeHookResult = {
            hookId: hook.id,
            coverage: hook.coverage,
            outcome: success
              ? "allowed"
              : context.event === "host-tool.before" && hook.failurePolicy === "closed"
                ? "denied"
                : "failed",
            detail: success ? "Hook completed." : "Hook failed or timed out. Output is withheld.",
          };
          const json = yield* encodeHookResult(result);
          yield* sql`INSERT OR IGNORE INTO runtime_hook_receipts VALUES(${context.projectId},${hook.id},${context.receiptId},${json})`;
          results.push(result);
        }
        if (results.some((result) => result.outcome === "denied"))
          return yield* error("denied", "Host tool denied by a configured fail-closed hook.");
        return results;
      }),
    );
  return RuntimeHooks.of({
    configure,
    remove,
    list,
    beforeTool: (context) => run({ ...context, event: "host-tool.before" }),
    afterTool: (context) => run({ ...context, event: "host-tool.after" }),
    observe: run,
  });
});
export const layer = Layer.effect(RuntimeHooks, make);
