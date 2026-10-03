import { assert, it } from "@effect/vitest";
import { AgentDocumentsRpcGroup, WS_METHODS } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as RpcTest from "effect/unstable/rpc/RpcTest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { migrateAgentDocuments as migrate } from "./testUtils/agentDocumentsSchema.ts";
import * as AgentDocuments from "./orchestration/AgentDocuments.ts";

const database = NodeSqliteClient.layer({ filename: ":memory:" });
const handlers = AgentDocumentsRpcGroup.toLayer(
  Effect.gen(function* () {
    const documents = yield* AgentDocuments.AgentDocuments;
    return {
      [WS_METHODS.agentDocumentsRead]: documents.read,
      [WS_METHODS.agentDocumentsWrite]: documents.write,
      [WS_METHODS.agentDocumentsAction]: documents.action,
      [WS_METHODS.agentDocumentsWait]: documents.wait,
    };
  }),
);
const transport = handlers.pipe(Layer.provide(AgentDocuments.layer), Layer.provideMerge(database));

it.effect(
  "document RPC retains accepted action state and enforces scope and revision failures",
  () =>
    Effect.gen(function* () {
      yield* migrate;
      const client = yield* RpcTest.makeClient(AgentDocumentsRpcGroup);
      const scope = { projectId: "project", ownerThreadId: "thread", documentId: "document" };
      yield* client[WS_METHODS.agentDocumentsWrite]({
        ...scope,
        operation: "mount",
        operationId: "mount",
        expectedRevision: 0,
        title: "Action test",
        placement: "inline",
        lifetime: "session",
        body: {
          kind: "applet",
          view: { type: "button", on_press: { action: "submit" } },
          state: { value: "before" },
        },
      });
      const input = {
        ...scope,
        expectedRevision: 1,
        actionId: "action",
        clientId: "remote-client",
        action: { action: "submit" },
        state: { value: "accepted" },
      };
      const sequence = yield* client[WS_METHODS.agentDocumentsAction](input);
      assert.equal(yield* client[WS_METHODS.agentDocumentsAction](input), sequence);
      const actions = yield* client[WS_METHODS.agentDocumentsWait]({ ...scope, afterSequence: 0 });
      assert.deepEqual(actions, [{ sequence, input }]);
      const foreign = yield* client[WS_METHODS.agentDocumentsRead]({
        ...scope,
        ownerThreadId: "another-thread",
        operation: "get",
      }).pipe(Effect.flip);
      assert.equal(foreign._tag, "AgentDocumentError");
      if (foreign._tag === "AgentDocumentError") assert.equal(foreign.code, "notFound");
      const stale = yield* client[WS_METHODS.agentDocumentsWrite]({
        ...scope,
        operation: "close",
        operationId: "stale",
        expectedRevision: 1,
      }).pipe(Effect.flip);
      assert.equal(stale._tag, "AgentDocumentError");
      if (stale._tag === "AgentDocumentError") assert.equal(stale.code, "conflict");
      assert.equal(
        (yield* client[WS_METHODS.agentDocumentsRead]({ ...scope, operation: "get" }))[0]?.revision,
        2,
      );
    }).pipe(Effect.provide(transport), Effect.scoped),
);
