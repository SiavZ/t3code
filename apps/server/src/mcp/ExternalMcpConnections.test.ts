import * as NodeHttp from "node:http";
import * as Fiber from "effect/Fiber";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { it, expect } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import migrate from "../persistence/Migrations/063_ExternalMcpConnections.ts";
import * as ExternalMcpConnections from "./ExternalMcpConnections.ts";

const fixture = `const rl = require('node:readline').createInterface({input:process.stdin});
let calls=0;
rl.on('line',line=>{const r=JSON.parse(line); if(r.id===undefined)return;
let result;
if(r.method==='initialize')result={protocolVersion:r.params.protocolVersion,capabilities:{tools:{}},serverInfo:{name:'fixture',version:'1'}};
else if(r.method==='tools/list')result={tools:[{name:'echo',description:'Fixture echo',inputSchema:{type:'object',properties:{text:{type:'string'}},required:['text'],additionalProperties:false}}]};
else if(r.method==='tools/call')result={content:[{type:'text',text:r.params.arguments.text}],structuredContent:{calls:++calls}};
else result={};
process.stdout.write(JSON.stringify({jsonrpc:'2.0',id:r.id,result})+'\\n');});`;

it.layer(Layer.mergeAll(NodeServices.layer, NodeSqliteClient.layer({ filename: ":memory:" })))(
  "ExternalMcpConnections",
  (it) => {
    it.effect(
      "owns a real stdio connection, validates schemas, retries completed calls and reloads generations",
      () =>
        Effect.gen(function* () {
          yield* migrate;
          yield* Effect.gen(function* () {
            const service = yield* ExternalMcpConnections.ExternalMcpConnections;
            const config = {
              transport: "stdio" as const,
              command: process.execPath,
              args: ["-e", fixture],
            };
            yield* service.configure({ id: "fixture", name: "Fixture", config, approved: false });
            expect((yield* service.connect({ id: "fixture" }).pipe(Effect.flip)).reason).toBe(
              "not-approved",
            );
            yield* service.configure({ id: "fixture", name: "Fixture", config, approved: true });
            const connected = yield* service.connect({ id: "fixture" });
            expect(connected.state).toBe("connected");
            const tools = yield* service.searchTools({ query: "echo" });
            expect(tools.tools[0]?.name).toBe("echo");
            const input = {
              id: "fixture",
              name: "echo",
              generation: connected.generation,
              invocationId: "first",
              arguments: { text: "hello fixture" },
            };
            const result = yield* service.callTool(input);
            expect(result.result).toEqual({
              content: [{ type: "text", text: "hello fixture" }],
              structuredContent: { calls: 1 },
            });
            expect(yield* service.callTool(input)).toEqual(result);
            expect(
              (yield* service
                .callTool({ ...input, invocationId: "invalid", arguments: { text: 1 } })
                .pipe(Effect.flip)).reason,
            ).toBe("invalid-config");
            const reloaded = yield* service.reload({ id: "fixture" });
            expect(reloaded.generation).toBeGreaterThan(connected.generation);
            expect(
              (yield* service.callTool({ ...input, invocationId: "stale" }).pipe(Effect.flip))
                .reason,
            ).toBe("stale-generation");
            expect((yield* service.disconnect({ id: "fixture" })).state).toBe("disconnected");
            const sql = yield* SqlClient.SqlClient;
            const rows = yield* sql<{
              approved: number;
            }>`SELECT approved FROM external_mcp_connections`;
            expect(rows).toEqual([{ approved: 1 }]);
          }).pipe(Effect.provide(ExternalMcpConnections.layer));
        }),
    );
    it.effect(
      "calls real HTTP MCP and cancels an acknowledged pending call without retrying it",
      () =>
        Effect.gen(function* () {
          yield* migrate;
          let received: () => void = () => undefined;
          const receivedCall = new Promise<void>((resolve) => {
            received = resolve;
          });
          const server = yield* Effect.acquireRelease(
            Effect.promise(
              () =>
                new Promise<NodeHttp.Server>((resolve) => {
                  const server = NodeHttp.createServer(async (request, response) => {
                    if (request.method !== "POST") {
                      response.writeHead(405).end();
                      return;
                    }
                    let body = "";
                    for await (const chunk of request) body += String(chunk);
                    const rpc = JSON.parse(body) as {
                      id?: string | number;
                      method: string;
                      params?: {
                        protocolVersion?: string;
                        name?: string;
                        arguments?: { text?: string };
                      };
                    };
                    if (rpc.id === undefined) {
                      response.writeHead(202).end();
                      return;
                    }
                    if (rpc.method === "tools/call" && rpc.params?.arguments?.text === "wait") {
                      received();
                      return;
                    }
                    const result =
                      rpc.method === "initialize"
                        ? {
                            protocolVersion: rpc.params?.protocolVersion,
                            capabilities: { tools: {} },
                            serverInfo: { name: "http-fixture", version: "1" },
                          }
                        : rpc.method === "tools/list"
                          ? {
                              tools: [
                                {
                                  name: "echo",
                                  inputSchema: {
                                    type: "object",
                                    properties: { text: { type: "string" } },
                                    required: ["text"],
                                  },
                                },
                              ],
                            }
                          : {
                              content: [{ type: "text", text: rpc.params?.arguments?.text ?? "" }],
                            };
                    response
                      .writeHead(200, { "content-type": "application/json" })
                      .end(JSON.stringify({ jsonrpc: "2.0", id: rpc.id, result }));
                  });
                  server.listen(0, "127.0.0.1", () => resolve(server));
                }),
            ),
            (server) =>
              Effect.promise(
                () =>
                  new Promise<void>((resolve) => {
                    server.closeAllConnections();
                    server.close(() => resolve());
                  }),
              ),
          );
          const address = server.address();
          if (!address || typeof address === "string")
            return yield* Effect.die("No fixture address");
          yield* Effect.gen(function* () {
            const service = yield* ExternalMcpConnections.ExternalMcpConnections;
            yield* service.configure({
              id: "http",
              name: "HTTP",
              config: { transport: "http", url: `http://127.0.0.1:${address.port}/mcp` },
              approved: true,
            });
            const connected = yield* service.connect({ id: "http" });
            const input = {
              id: "http",
              name: "echo",
              generation: connected.generation,
              invocationId: "http-call",
              arguments: { text: "http fixture" },
            };
            expect((yield* service.callTool(input)).result).toEqual({
              content: [{ type: "text", text: "http fixture" }],
            });
            const waiting = { ...input, invocationId: "http-wait", arguments: { text: "wait" } };
            const pending = yield* service.callTool(waiting).pipe(Effect.forkChild);
            yield* Effect.promise(() => receivedCall);
            yield* service.cancelCall({ invocationId: "http-wait" });
            expect((yield* Fiber.join(pending).pipe(Effect.flip)).reason).toBe("canceled");
            expect((yield* service.callTool(waiting).pipe(Effect.flip)).reason).toBe("conflict");
            yield* service.disconnect({ id: "http" });
          }).pipe(Effect.provide(ExternalMcpConnections.layer));
        }),
    );
    it.effect(
      "rejects oversized stdio frames and failed spawns without leaving owned children",
      () =>
        Effect.gen(function* () {
          yield* migrate;
          yield* Effect.gen(function* () {
            const service = yield* ExternalMcpConnections.ExternalMcpConnections;
            yield* service.configure({
              id: "oversized",
              name: "Oversized",
              config: {
                transport: "stdio",
                command: process.execPath,
                args: [
                  "-e",
                  "process.stdin.on('data',()=>process.stdout.write('x'.repeat(1048577)))",
                ],
              },
              approved: true,
            });
            const oversized = yield* service.connect({ id: "oversized" }).pipe(Effect.flip);
            expect(["transport-failed", "limit-exceeded"]).toContain(oversized.reason);
            yield* service.configure({
              id: "missing-executable",
              name: "Missing",
              config: {
                transport: "stdio",
                command: "/nonexistent/t3-fixture-executable",
                args: [],
              },
              approved: true,
            });
            expect(
              (yield* service.connect({ id: "missing-executable" }).pipe(Effect.flip)).reason,
            ).toBe("transport-failed");
            expect(
              (yield* service.list()).connections.find(
                (connection) => connection.id === "oversized",
              )?.state,
            ).toBe("failed");
          }).pipe(Effect.provide(ExternalMcpConnections.layer));
        }),
    );
    it.effect("reconstructs approved configs as disconnected with new volatile generations", () =>
      Effect.gen(function* () {
        yield* migrate;
        const previous = yield* Effect.gen(function* () {
          const service = yield* ExternalMcpConnections.ExternalMcpConnections;
          yield* service.configure({
            id: "restart",
            name: "Restart",
            config: { transport: "stdio", command: process.execPath, args: ["-e", fixture] },
            approved: true,
          });
          return yield* service.connect({ id: "restart" });
        }).pipe(Effect.provide(ExternalMcpConnections.layer));
        yield* Effect.gen(function* () {
          const service = yield* ExternalMcpConnections.ExternalMcpConnections;
          const restarted = (yield* service.list()).connections.find(
            (connection) => connection.id === "restart",
          );
          expect(restarted?.state).toBe("disconnected");
          expect(restarted?.generation).not.toBe(previous.generation);
          const connected = yield* service.connect({ id: "restart" });
          expect(
            (yield* service
              .callTool({
                id: "restart",
                name: "echo",
                generation: previous.generation,
                invocationId: "restart-stale",
                arguments: { text: "fixture" },
              })
              .pipe(Effect.flip)).reason,
          ).toBe("stale-generation");
          expect(connected.generation).not.toBe(previous.generation);
        }).pipe(Effect.provide(ExternalMcpConnections.layer));
      }),
    );
  },
);
