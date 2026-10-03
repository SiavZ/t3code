import { ProjectId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as M from "../../../../packages/contracts/src/memory.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import { MemoryService, type MemoryAuthority } from "./Memory.ts";

/** Authenticated human session authority. Agents and wire payloads cannot construct this grant. */
export interface GlobalMemoryAuthority {
  readonly humanSessionId: string;
  readonly admin: boolean;
}
export class GlobalMemory extends Context.Service<
  GlobalMemory,
  {
    readonly read: (
      input: M.GlobalMemoryReadInput,
      authority: GlobalMemoryAuthority,
    ) => Effect.Effect<M.MemoryResult, M.MemoryError>;
    readonly write: (
      input: M.GlobalMemoryWriteInput,
      authority: GlobalMemoryAuthority,
    ) => Effect.Effect<M.MemoryMutationResult, M.MemoryError>;
  }
>()("t3/memory/GlobalMemory") {}
const decodeRead = Schema.decodeUnknownEffect(M.GlobalMemoryReadInput);
const decodeWrite = Schema.decodeUnknownEffect(M.GlobalMemoryWriteInput);
const invalid = () =>
  new M.MemoryError({ code: "invalid", detail: "Invalid global memory request." });
const make = Effect.gen(function* () {
  const memory = yield* MemoryService;
  const settings = yield* ServerSettingsService;
  const authorize = Effect.fn("GlobalMemory.authorize")(function* (a: GlobalMemoryAuthority) {
    if (!a.admin || !a.humanSessionId.trim() || a.humanSessionId.length > 256)
      return yield* new M.MemoryError({
        code: "forbidden",
        detail: "Global memory requires an authenticated admin human session.",
      });
    const current = yield* settings.getSettings.pipe(
      Effect.mapError(
        () =>
          new M.MemoryError({
            code: "storage",
            detail: "Global memory settings could not be read.",
          }),
      ),
    );
    if (!current.enableGlobalMemory)
      return yield* new M.MemoryError({
        code: "forbidden",
        detail: "Global memory is disabled for this environment.",
      });
    return {
      // No project context exists for this namespace. globalOnly prevents this anchor granting project access.
      projectId: ProjectId.make("environment-global-memory"),
      allowGlobal: true,
      globalOnly: true,
      actorId: a.humanSessionId,
    } satisfies MemoryAuthority;
  });
  return GlobalMemory.of({
    read: (raw, a) =>
      Effect.gen(function* () {
        const authority = yield* authorize(a);
        const request = yield* decodeRead(raw).pipe(Effect.mapError(invalid));
        switch (request.operation) {
          case "recall":
            return yield* memory.recall({ ...request.input, scope: "global" }, authority);
          case "search":
            return yield* memory.search({ ...request.input, scope: "global" }, authority);
          case "related":
            return yield* memory.related(request.input, authority);
        }
      }),
    write: (raw, a) =>
      Effect.gen(function* () {
        const authority = yield* authorize(a);
        const request = yield* decodeWrite(raw).pipe(Effect.mapError(invalid));
        switch (request.operation) {
          case "remember":
            return yield* memory.remember({ ...request.input, scope: "global" }, authority);
          case "forget":
            return yield* memory.forget(request.input, authority);
          case "tag":
            return yield* memory.tag(request.input, authority);
          case "link":
            return yield* memory.link(request.input, authority);
        }
      }),
  });
});
export const layer = Layer.effect(GlobalMemory, make);
