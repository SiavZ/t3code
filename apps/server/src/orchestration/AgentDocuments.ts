import * as Deferred from "effect/Deferred";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import { patchDocument, validateDocumentBudget } from "./agentDocumentValidation.ts";
import {
  AgentDocument,
  AgentDocumentError,
  AgentDocumentReadInput,
  AgentDocumentWriteInput,
  AgentDocumentActionInput,
  AgentDocumentWaitInput,
  type AgentDocumentAcceptedAction,
} from "../../../../packages/contracts/src/agentDocuments.ts";

export class AgentDocuments extends Context.Service<
  AgentDocuments,
  {
    readonly recover: Effect.Effect<void, AgentDocumentError>;
    readonly releaseOwner: (input: {
      readonly ownerThreadId: string;
      readonly projectId: string;
      readonly operationId: string;
      readonly reason: "turnEnded" | "deleted";
      readonly throughSequence?: number;
    }) => Effect.Effect<void, AgentDocumentError>;
    readonly read: (
      input: AgentDocumentReadInput,
    ) => Effect.Effect<readonly AgentDocument[], AgentDocumentError>;
    readonly action: (input: AgentDocumentActionInput) => Effect.Effect<number, AgentDocumentError>;
    readonly wait: (
      input: AgentDocumentWaitInput,
    ) => Effect.Effect<readonly AgentDocumentAcceptedAction[], AgentDocumentError>;
    readonly write: (
      input: AgentDocumentWriteInput,
    ) => Effect.Effect<AgentDocument, AgentDocumentError>;
  }
>()("t3/orchestration/AgentDocuments") {}
const fail = (code: AgentDocumentError["code"], detail: string) =>
  new AgentDocumentError({ code, detail });
const decodeDocument = Schema.decodeUnknownSync(AgentDocument);
const decodeReadInput = Schema.decodeUnknownEffect(AgentDocumentReadInput);
const decodeWriteInput = Schema.decodeUnknownEffect(AgentDocumentWriteInput);
const decodeActionInput = Schema.decodeUnknownEffect(AgentDocumentActionInput);
const decodeStoredAction = Schema.decodeUnknownSync(AgentDocumentActionInput);
const decodeWaitInput = Schema.decodeUnknownEffect(AgentDocumentWaitInput);
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const notifications = new Map<string, Set<Deferred.Deferred<void>>>();
  const decode = (snapshot: string) => decodeDocument(JSON.parse(snapshot));
  const read = Effect.fn("AgentDocuments.read")(function* (raw: AgentDocumentReadInput) {
    const input = yield* decodeReadInput(raw).pipe(
      Effect.mapError(() => fail("invalid", "Invalid read input.")),
    );
    if (input.operation === "get") {
      const rows = yield* sql<{
        snapshot: string;
      }>`SELECT snapshot FROM agent_documents WHERE id = ${input.documentId} AND project_id = ${input.projectId} AND owner_thread_id = ${input.ownerThreadId}`;
      if (!rows[0]) return yield* fail("notFound", "Document not found in scope.");
      return [decode(rows[0].snapshot)];
    }
    const rows = yield* sql<{
      snapshot: string;
    }>`SELECT snapshot FROM agent_documents WHERE project_id = ${input.projectId} AND owner_thread_id = ${input.ownerThreadId} ORDER BY id LIMIT 100`;
    const docs = rows.map((row) => decode(row.snapshot));
    return docs.filter((doc) => input.includeClosed || !doc.closed);
  });
  const write = Effect.fn("AgentDocuments.write")(function* (raw: AgentDocumentWriteInput) {
    yield* Effect.try({
      try: () => validateDocumentBudget(raw),
      catch: () => fail("quota", "Document exceeds structure budget."),
    });
    const input = yield* decodeWriteInput(raw).pipe(
      Effect.mapError(() => fail("invalid", "Invalid mutation input.")),
    );
    if (JSON.stringify(input).length > 262_144)
      return yield* fail("quota", "Document exceeds byte budget.");
    const notification = notifications.get(input.documentId);
    const result = yield* sql.withTransaction(
      Effect.gen(function* () {
        const rows = yield* sql<{
          snapshot: string;
        }>`SELECT snapshot FROM agent_documents WHERE id = ${input.documentId} AND project_id = ${input.projectId} AND owner_thread_id = ${input.ownerThreadId}`;
        const existing = rows[0] ? decode(rows[0].snapshot) : undefined;
        const retries = yield* sql<{
          fingerprint: string;
          result: string;
        }>`SELECT fingerprint, result FROM agent_document_operations WHERE document_id = ${input.documentId} AND operation_id = ${input.operationId}`;
        const fingerprint = JSON.stringify(input);
        if (retries[0]) {
          if (!existing || retries[0].fingerprint !== fingerprint)
            return yield* fail("conflict", "Operation id reused with different input or scope.");
          return decode(retries[0].result);
        }
        if (input.operation === "mount" || input.operation === "replace") {
          if (input.body.kind === "pdf") {
            const assets =
              yield* sql`SELECT asset_id FROM agent_document_assets WHERE asset_id = ${input.body.assetId} AND project_id = ${input.projectId} AND owner_thread_id = ${input.ownerThreadId}`;
            if (!assets.length)
              return yield* fail("notFound", "PDF asset not found in document scope.");
          }
        }
        let next: AgentDocument;
        if (input.operation === "mount") {
          if (existing || input.expectedRevision !== 0)
            return yield* fail("conflict", "Mount requires a new document and revision zero.");
          next = {
            id: input.documentId,
            projectId: input.projectId,
            ownerThreadId: input.ownerThreadId,
            revision: 1,
            title: input.title,
            body: input.body,
            placement: input.placement,
            lifetime: input.lifetime,
            closed: false,
          };
          yield* sql`INSERT INTO agent_documents (id, project_id, owner_thread_id, revision, snapshot, mounted_after_sequence) VALUES (${next.id}, ${next.projectId}, ${next.ownerThreadId}, ${next.revision}, ${JSON.stringify(next)}, (SELECT coalesce(max(sequence), 0) FROM orchestration_events))`;
        } else {
          if (!existing) return yield* fail("notFound", "Document not found in scope.");
          if (existing.revision !== input.expectedRevision)
            return yield* fail("conflict", "Document revision changed.");
          next = { ...existing, revision: existing.revision + 1 };
          if (input.operation === "acknowledge")
            yield* sql`UPDATE agent_document_actions SET acknowledged = 1 WHERE document_id = ${next.id} AND sequence <= ${input.throughSequence}`;
          if (input.operation === "patch")
            next = yield* Effect.try({
              try: () => patchDocument(next, input.patches),
              catch: () => fail("invalid", "Patch failed validation."),
            });
          if (input.operation === "replace") next = { ...next, body: input.body };
          if (input.operation === "move") next = { ...next, placement: input.placement };
          if (input.operation === "close" || input.operation === "reopen")
            next = { ...next, closed: input.operation === "close" };
          yield* sql`UPDATE agent_documents SET revision = ${next.revision}, snapshot = ${JSON.stringify(next)} WHERE id = ${next.id} AND revision = ${input.expectedRevision}`;
        }
        yield* Effect.try({
          try: () => validateDocumentBudget(next),
          catch: () => fail("quota", "Resulting document exceeds budget."),
        });
        const usage = yield* sql<{
          count: number;
          bytes: number;
        }>`SELECT count(*) AS count, coalesce(sum(length(CAST(snapshot AS BLOB))), 0) AS bytes FROM agent_documents WHERE owner_thread_id = ${input.ownerThreadId} AND project_id = ${input.projectId} AND id <> ${next.id}`;
        if (
          (usage[0]?.count ?? 0) >= 64 ||
          (usage[0]?.bytes ?? 0) + new TextEncoder().encode(JSON.stringify(next)).byteLength >
            1_048_576
        )
          return yield* fail("quota", "Thread document storage exceeds budget.");
        const receipts = yield* sql<{
          bytes: number;
        }>`SELECT coalesce(sum(length(CAST(fingerprint AS BLOB)) + length(CAST(result AS BLOB))), 0) AS bytes FROM agent_document_operations WHERE document_id = ${next.id}`;
        if (
          (receipts[0]?.bytes ?? 0) + fingerprint.length + JSON.stringify(next).length >
          4_194_304
        )
          return yield* fail("quota", "Document operation history exceeds budget.");
        yield* sql`INSERT INTO agent_document_operations (document_id, operation_id, fingerprint, result) VALUES (${next.id}, ${input.operationId}, ${fingerprint}, ${JSON.stringify(next)})`;
        return next;
      }),
    );
    if (input.operation === "close" && notification)
      for (const waiter of notification) yield* Deferred.succeed(waiter, undefined);
    return result;
  });
  const action = Effect.fn("AgentDocuments.action")(function* (raw: AgentDocumentActionInput) {
    yield* Effect.try({
      try: () => validateDocumentBudget(raw),
      catch: () => fail("quota", "Action exceeds budget."),
    });
    if (new TextEncoder().encode(JSON.stringify(raw)).byteLength > 65_536)
      return yield* fail("quota", "Action snapshot exceeds 64 KiB.");
    const input = yield* decodeActionInput(raw).pipe(
      Effect.mapError(() => fail("invalid", "Invalid action.")),
    );
    const sequence = yield* sql.withTransaction(
      Effect.gen(function* () {
        const docs = yield* read({ ...input, operation: "get" });
        const doc = docs[0]!;
        const fingerprint = JSON.stringify(input);
        const retry = yield* sql<{
          sequence: number;
          fingerprint: string;
        }>`SELECT sequence, fingerprint FROM agent_document_actions WHERE document_id = ${doc.id} AND action_id = ${input.actionId}`;
        if (retry[0]) {
          if (retry[0].fingerprint !== fingerprint)
            return yield* fail("conflict", "Action id reused.");
          return retry[0].sequence;
        }
        if (doc.closed) return yield* fail("closed", "Document is closed.");
        if (doc.revision !== input.expectedRevision)
          return yield* fail("conflict", "Action revision is stale.");
        if (doc.body.kind !== "applet")
          return yield* fail("invalid", "Only applets accept actions.");
        const actions: string[] = [];
        const visit = (node: typeof doc.body.view): void => {
          if (node.on_press) actions.push(JSON.stringify(node.on_press));
          if (node.on_submit) actions.push(JSON.stringify(node.on_submit));
          node.children?.forEach(visit);
          node.tabs?.forEach((tab) => tab.children.forEach(visit));
        };
        visit(doc.body.view);
        if (!actions.includes(JSON.stringify(input.action)))
          return yield* fail("invalid", "Action is not offered by this revision.");
        const count = yield* sql<{
          count: number;
        }>`SELECT count(*) AS count FROM agent_document_actions WHERE document_id = ${doc.id} AND acknowledged = 0`;
        if ((count[0]?.count ?? 0) >= 128) return yield* fail("quota", "Action inbox is full.");
        const updated: AgentDocument = {
          ...doc,
          revision: doc.revision + 1,
          body: { ...doc.body, state: input.state },
        };
        yield* Effect.try({
          try: () => validateDocumentBudget(updated),
          catch: () => fail("quota", "Accepted state exceeds document budget."),
        });
        const usage = yield* sql<{
          bytes: number;
        }>`SELECT coalesce(sum(length(CAST(snapshot AS BLOB))), 0) AS bytes FROM agent_documents WHERE owner_thread_id = ${input.ownerThreadId} AND project_id = ${input.projectId} AND id <> ${doc.id}`;
        if (
          (usage[0]?.bytes ?? 0) + new TextEncoder().encode(JSON.stringify(updated)).byteLength >
          1_048_576
        )
          return yield* fail("quota", "Thread document storage exceeds budget.");
        yield* sql`UPDATE agent_documents SET revision = ${updated.revision}, snapshot = ${JSON.stringify(updated)} WHERE id = ${doc.id} AND revision = ${input.expectedRevision}`;
        const result = yield* sql<{
          sequence: number;
        }>`INSERT INTO agent_document_actions (document_id, action_id, fingerprint, snapshot) VALUES (${doc.id}, ${input.actionId}, ${fingerprint}, ${JSON.stringify(input)}) RETURNING sequence`;
        return result[0]!.sequence;
      }),
    );
    const notification = notifications.get(input.documentId);
    if (notification) for (const waiter of notification) yield* Deferred.succeed(waiter, undefined);
    return sequence;
  });
  const wait = Effect.fn("AgentDocuments.wait")(function* (raw: AgentDocumentWaitInput) {
    const input = yield* decodeWaitInput(raw).pipe(
      Effect.mapError(() => fail("invalid", "Invalid wait.")),
    );
    yield* read({ ...input, operation: "get" });
    while (true) {
      const notification = yield* Deferred.make<void>();
      let waiters = notifications.get(input.documentId);
      if (!waiters) {
        if (notifications.size >= 128) return yield* fail("quota", "Wait capacity exceeded.");
        waiters = new Set();
        notifications.set(input.documentId, waiters);
      }
      if (waiters.size >= 16) return yield* fail("quota", "Wait capacity exceeded.");
      waiters.add(notification);
      const registered = waiters;
      const result = yield* Effect.gen(function* () {
        const docs = yield* read({ ...input, operation: "get" });
        if (docs[0]?.closed) return yield* fail("closed", "Document closed while waiting.");
        const rows = yield* sql<{
          sequence: number;
          snapshot: string;
        }>`SELECT sequence, snapshot FROM agent_document_actions WHERE document_id = ${input.documentId} AND sequence > ${input.afterSequence} AND acknowledged = 0 ORDER BY sequence LIMIT 8`;
        if (rows.length)
          return rows.map((row) => ({
            sequence: row.sequence,
            input: decodeStoredAction(JSON.parse(row.snapshot)),
          }));
        yield* Deferred.await(notification);
        return null;
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            registered.delete(notification);
            if (registered.size === 0 && notifications.get(input.documentId) === registered)
              notifications.delete(input.documentId);
          }),
        ),
      );
      if (result) return result;
    }
  });
  const recover =
    sql`UPDATE agent_documents SET snapshot = json_set(snapshot, '$.closed', json('true'), '$.revision', revision + 1), revision = revision + 1 WHERE json_extract(snapshot, '$.lifetime') = 'ephemeral' AND json_extract(snapshot, '$.closed') = 0`.pipe(
      Effect.asVoid,
      Effect.mapError(() => fail("storage", "Document recovery failed.")),
    );
  // Turn-end releases arrive from a lagging reactor, so they carry the terminal event's
  // sequence and only close documents mounted before it. A release already applied to a
  // document is skipped, so a replay never closes a document reopened since.
  const releaseOwner = Effect.fn("AgentDocuments.releaseOwner")(function* (input: {
    readonly ownerThreadId: string;
    readonly projectId: string;
    readonly operationId: string;
    readonly reason: "turnEnded" | "deleted";
    readonly throughSequence?: number;
  }) {
    const rows = yield* sql<{
      snapshot: string;
      mountedAfterSequence: number;
      applied: number;
    }>`SELECT d.snapshot, d.mounted_after_sequence AS "mountedAfterSequence", EXISTS(SELECT 1 FROM agent_document_operations o WHERE o.document_id = d.id AND o.operation_id = ${input.operationId}) AS applied FROM agent_documents d WHERE d.project_id = ${input.projectId} AND d.owner_thread_id = ${input.ownerThreadId} ORDER BY d.id`;
    for (const row of rows) {
      const doc = decode(row.snapshot);
      if (doc.closed || row.applied) continue;
      if (
        input.reason === "turnEnded" &&
        (doc.lifetime !== "ephemeral" ||
          (input.throughSequence !== undefined &&
            row.mountedAfterSequence >= input.throughSequence))
      )
        continue;
      yield* write({
        ownerThreadId: input.ownerThreadId,
        projectId: input.projectId,
        documentId: doc.id,
        operationId: input.operationId,
        operation: "close",
        expectedRevision: doc.revision,
      });
    }
  });
  return AgentDocuments.of({
    recover,
    releaseOwner: (input) =>
      releaseOwner(input).pipe(
        Effect.mapError((error) =>
          error instanceof AgentDocumentError
            ? error
            : fail("storage", "Owner document release failed."),
        ),
      ),
    action: (input) =>
      action(input).pipe(
        Effect.mapError((error) =>
          error instanceof AgentDocumentError ? error : fail("storage", "Action failed."),
        ),
      ),
    wait: (input) =>
      wait(input).pipe(
        Effect.mapError((error) =>
          error instanceof AgentDocumentError ? error : fail("storage", "Wait failed."),
        ),
      ),
    read: (input) =>
      read(input).pipe(
        Effect.mapError((error) =>
          error instanceof AgentDocumentError ? error : fail("storage", "Document read failed."),
        ),
      ),
    write: (input) =>
      write(input).pipe(
        Effect.mapError((error) =>
          error instanceof AgentDocumentError
            ? error
            : fail("storage", "Document mutation failed."),
        ),
      ),
  });
});
export const layer = Layer.effect(AgentDocuments, make);
