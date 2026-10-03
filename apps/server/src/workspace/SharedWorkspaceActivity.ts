import * as NodeCrypto from "node:crypto";
import * as NodeFSP from "node:fs/promises";
import * as NodePath from "node:path";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as CodexSchema from "effect-codex-app-server/schema";
import {
  CommandId,
  type CoordinationError,
  type ProviderRuntimeEvent,
  type ThreadId,
} from "@t3tools/contracts";
import * as CoordinationPlans from "../orchestration/CoordinationPlans.ts";
import * as ProjectionSnapshotQuery from "../orchestration/Services/ProjectionSnapshotQuery.ts";
import * as WorkspacePaths from "./WorkspacePaths.ts";

export class SharedWorkspaceActivityError extends Schema.TaggedError<SharedWorkspaceActivityError>()(
  "SharedWorkspaceActivityError",
  {
    reason: Schema.Literals(["invalid-path", "limit-exceeded"]),
  },
) {}

export interface SharedWorkspaceActivityResult {
  readonly supported: boolean;
  readonly touchedFiles: ReadonlyArray<string>;
  readonly notifiedThreadIds: ReadonlyArray<ThreadId>;
  readonly warnings: ReadonlyArray<string>;
}
export class SharedWorkspaceActivity extends Context.Service<
  SharedWorkspaceActivity,
  {
    readonly record: (
      event: ProviderRuntimeEvent,
    ) => Effect.Effect<
      SharedWorkspaceActivityResult,
      SharedWorkspaceActivityError | CoordinationError
    >;
  }
>()("t3/workspace/SharedWorkspaceActivity") {}

const decodeCompletion = Schema.decodeUnknownOption(CodexSchema.V2ItemCompletedNotification);
const within = (root: string, path: string) => {
  const relative = NodePath.relative(root, path);
  return (
    relative !== "" &&
    relative !== ".." &&
    !relative.startsWith(`..${NodePath.sep}`) &&
    !NodePath.isAbsolute(relative)
  );
};
// Deleted/new paths are resolved through the nearest existing ancestor, never by title or shell text.
const canonicalFile = async (root: string, path: string) => {
  if (!path || path.length > 4096 || path.includes("\0")) throw new Error("Invalid path");
  const absolute = NodePath.resolve(root, path);
  if (!within(root, absolute)) throw new Error("Outside checkout");
  let ancestor = absolute;
  const suffix: string[] = [];
  for (let depth = 0; depth < 128; depth++) {
    try {
      const canonical = NodePath.join(await NodeFSP.realpath(ancestor), ...suffix);
      if (!within(root, canonical)) throw new Error("Outside canonical checkout");
      return NodePath.relative(root, canonical).split(NodePath.sep).join("/");
    } catch (cause) {
      if (!(cause instanceof Error && "code" in cause && cause.code === "ENOENT")) throw cause;
      suffix.unshift(NodePath.basename(ancestor));
      const parent = NodePath.dirname(ancestor);
      if (parent === ancestor) throw cause;
      ancestor = parent;
    }
  }
  throw new Error("Path depth exceeded");
};

const make = Effect.gen(function* () {
  const mailbox = yield* CoordinationPlans.CoordinationPlans;
  const query = yield* ProjectionSnapshotQuery.ProjectionSnapshotQuery;
  const workspacePaths = yield* WorkspacePaths.WorkspacePaths;
  const lock = yield* Semaphore.make(1);
  const roots = new Map<
    string,
    { files: Map<string, Set<ThreadId>>; events: Set<string>; delivered: Set<string> }
  >();
  const record = Effect.fn("SharedWorkspaceActivity.record")(function* (
    event: ProviderRuntimeEvent,
  ) {
    const empty = { touchedFiles: [], notifiedThreadIds: [], warnings: [] };
    if (event.provider !== "codex")
      return {
        ...empty,
        supported: false,
        warnings: [`${event.provider}: no verified normalized checkout-edit evidence reader`],
      };
    if (
      event.type !== "item.completed" ||
      event.payload.itemType !== "file_change" ||
      event.payload.status !== "completed"
    )
      return { ...empty, supported: true };
    const decoded = decodeCompletion(event.payload.data);
    if (
      decoded._tag === "None" ||
      decoded.value.item.type !== "fileChange" ||
      decoded.value.item.status !== "completed"
    )
      return {
        ...empty,
        supported: true,
        warnings: ["Invalid or unsuccessful native fileChange completion ignored"],
      };
    const changes = decoded.value.item.changes;
    if (changes.length > 128)
      return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
    const thread = yield* query
      .getThreadShellById(event.threadId)
      .pipe(Effect.mapError(() => new SharedWorkspaceActivityError({ reason: "invalid-path" })));
    if (thread._tag === "None")
      return { ...empty, supported: true, warnings: ["Thread no longer exists"] };
    const project = yield* query
      .getProjectShellById(thread.value.projectId)
      .pipe(Effect.mapError(() => new SharedWorkspaceActivityError({ reason: "invalid-path" })));
    if (project._tag === "None")
      return { ...empty, supported: true, warnings: ["Project no longer exists"] };
    const rootThreadId = thread.value.worker?.rootThreadId ?? event.threadId;
    const workspaceRoot = yield* workspacePaths
      .normalizeWorkspaceRoot(thread.value.worktreePath ?? project.value.workspaceRoot)
      .pipe(Effect.mapError(() => new SharedWorkspaceActivityError({ reason: "invalid-path" })));
    const root = yield* Effect.tryPromise({
      try: () => NodeFSP.realpath(workspaceRoot),
      catch: () => new SharedWorkspaceActivityError({ reason: "invalid-path" }),
    });
    const paths = changes.flatMap((change) =>
      change.kind.type === "update" && change.kind.move_path
        ? [change.path, change.kind.move_path]
        : [change.path],
    );
    const touchedFiles = [
      ...new Set(
        yield* Effect.forEach(paths, (path) =>
          Effect.tryPromise({
            try: () => canonicalFile(root, path),
            catch: () => new SharedWorkspaceActivityError({ reason: "invalid-path" }),
          }),
        ),
      ),
    ].sort();
    const key = `${rootThreadId}\0${root}`;
    let state = roots.get(key);
    if (!state) {
      if (roots.size >= 64)
        return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
      state = { files: new Map(), events: new Set(), delivered: new Set() };
      roots.set(key, state);
    }
    if (state.events.has(event.eventId)) return { ...empty, supported: true };
    if (
      state.events.size >= 4096 ||
      touchedFiles.filter((path) => !state.files.has(path)).length + state.files.size > 2048
    )
      return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
    const recipients = new Map<ThreadId, string[]>();
    for (const path of touchedFiles) {
      const owners = state.files.get(path);
      if (owners && owners.size >= 64 && !owners.has(event.threadId))
        return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
      for (const owner of owners ?? [])
        if (owner !== event.threadId)
          recipients.set(owner, [...(recipients.get(owner) ?? []), path]);
    }
    if (recipients.size > 64)
      return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
    // Footprints reflect verified edits even if a mailbox quota prevents an advisory.
    for (const path of touchedFiles) {
      const owners = state.files.get(path) ?? new Set<ThreadId>();
      owners.add(event.threadId);
      state.files.set(path, owners);
    }
    // Root authority and recipient lineage are checked by the durable mailbox service.
    for (const [recipient, files] of recipients) {
      const deliveryKey = `${event.eventId}\0${recipient}`;
      if (state.delivered.has(deliveryKey)) continue;
      if (state.delivered.size >= 8192)
        return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
      const text = `Shared checkout warning: thread ${event.threadId} also touched ${files.slice(0, 16).join(", ").slice(0, 6000)}${files.length > 16 ? " (additional files omitted)" : ""}. Native completed file-change evidence only. This is not a conflict diagnosis or automatic resolution.`;
      let delivered = false;
      for (let attempt = 0; attempt < 3; attempt++) {
        const current = yield* mailbox.mailboxRead({
          callerThreadId: rootThreadId,
          rootThreadId: rootThreadId,
        });
        const digest = NodeCrypto.createHash("sha256")
          .update(`${key}\0${event.eventId}\0${recipient}\0${current.revision}`)
          .digest("hex");
        const result = yield* mailbox
          .mailboxWrite({
            operation: "message",
            callerThreadId: rootThreadId,
            rootThreadId: rootThreadId,
            commandId: CommandId.make(`shared-file-${digest}`),
            expectedRevision: current.revision,
            recipientThreadIds: [recipient],
            channelId: null,
            text,
          })
          .pipe(Effect.result);
        if (result._tag === "Success") {
          state.delivered.add(deliveryKey);
          delivered = true;
          break;
        }
        if (result.failure.code !== "conflict" || attempt === 2) return yield* result.failure;
      }
      if (!delivered) return yield* new SharedWorkspaceActivityError({ reason: "limit-exceeded" });
    }
    state.events.add(event.eventId);
    return {
      supported: true,
      touchedFiles,
      notifiedThreadIds: [...recipients.keys()],
      warnings: [],
    };
  });
  return SharedWorkspaceActivity.of({ record: (event) => lock.withPermits(1)(record(event)) });
});
export const layer = Layer.effect(SharedWorkspaceActivity, make);
