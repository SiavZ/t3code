import * as NodeCrypto from "node:crypto";
import type { Client } from "@modelcontextprotocol/sdk/client/index.js";
import * as TransportBounds from "./ExternalMcpTransportBounds.ts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import {
  ExternalMcpConfig,
  ExternalMcpError,
  type ExternalMcpConfigureInput,
  type ExternalMcpSnapshot,
  type ExternalMcpTool,
  type ExternalMcpSearchInput,
  type ExternalMcpSearchResult,
  type ExternalMcpCallInput,
  type ExternalMcpCallResult,
} from "../../../../packages/contracts/src/externalMcp.ts";

export class ExternalMcpConnections extends Context.Service<
  ExternalMcpConnections,
  {
    readonly configure: (
      input: ExternalMcpConfigureInput,
    ) => Effect.Effect<ExternalMcpSnapshot, ExternalMcpError>;
    readonly list: () => Effect.Effect<{ connections: ExternalMcpSnapshot[] }, ExternalMcpError>;
    readonly connect: (input: {
      id: string;
    }) => Effect.Effect<ExternalMcpSnapshot, ExternalMcpError>;
    readonly disconnect: (input: {
      id: string;
    }) => Effect.Effect<ExternalMcpSnapshot, ExternalMcpError>;
    readonly reload: (input: {
      id: string;
    }) => Effect.Effect<ExternalMcpSnapshot, ExternalMcpError>;
    readonly remove: (input: { id: string }) => Effect.Effect<void, ExternalMcpError>;
    readonly searchTools: (
      input: ExternalMcpSearchInput,
    ) => Effect.Effect<ExternalMcpSearchResult, ExternalMcpError>;
    readonly callTool: (
      input: ExternalMcpCallInput,
    ) => Effect.Effect<ExternalMcpCallResult, ExternalMcpError>;
    readonly cancelCall: (input: { invocationId: string }) => Effect.Effect<void>;
  }
>()("t3/mcp/ExternalMcpConnections") {}

type Entry = {
  revision: number;
  config: ExternalMcpConfig;
  approved: boolean;
  snapshot: ExternalMcpSnapshot;
  client?: Client | undefined;
  tools: ExternalMcpTool[];
  controller?: AbortController;
};
const decodeConfig = Schema.decodeUnknownEffect(Schema.fromJsonString(ExternalMcpConfig));
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  let generationCounter = NodeCrypto.randomInt(1, 2 ** 47);
  const nextGeneration = () => ++generationCounter;
  const entries = new Map<string, Entry>();
  const calls = new Map<string, { id: string; controller: AbortController; fingerprint: string }>();
  const seenInvocations = new Map<string, string>();
  const completed = new Map<string, { fingerprint: string; result: ExternalMcpCallResult }>();
  const rows = yield* sql<{
    id: string;
    name: string;
    config_json: string;
    approved: number;
    revision: number;
  }>`SELECT id, name, config_json, approved, revision FROM external_mcp_connections LIMIT 101`.pipe(
    Effect.mapError(() => new ExternalMcpError({ reason: "transport-failed" })),
  );
  if (rows.length > 100) return yield* new ExternalMcpError({ reason: "limit-exceeded" });
  for (const row of rows) {
    const config = yield* decodeConfig(row.config_json).pipe(
      Effect.mapError(() => new ExternalMcpError({ reason: "invalid-config", id: row.id })),
    );
    entries.set(row.id, {
      revision: row.revision,
      config,
      approved: row.approved === 1,
      snapshot: {
        id: row.id,
        name: row.name,
        transport: config.transport,
        state: "disconnected",
        generation: nextGeneration(),
      },
      tools: [],
    });
  }
  const get = (id: string) => {
    const entry = entries.get(id);
    if (!entry) throw new ExternalMcpError({ reason: "not-found", id });
    return entry;
  };
  const attempt = <A>(body: (signal: AbortSignal) => Promise<A>) =>
    Effect.tryPromise({
      try: body,
      catch: (error) =>
        error instanceof ExternalMcpError
          ? error
          : new ExternalMcpError({ reason: "transport-failed" }),
    });
  const close = async (entry: Entry) => {
    entry.controller?.abort();
    for (const call of calls.values()) if (call.id === entry.snapshot.id) call.controller.abort();
    const client = entry.client;
    entry.client = undefined;
    entry.tools = [];
    entry.snapshot = { ...entry.snapshot, state: "disconnected", generation: nextGeneration() };
    await client?.close();
  };
  yield* Effect.addFinalizer(() =>
    Effect.promise(async () => {
      await Promise.all([...entries.values()].map((entry) => close(entry).catch(() => undefined)));
    }),
  );
  const configure = Effect.fn("ExternalMcpConnections.configure")(function* (
    input: ExternalMcpConfigureInput,
  ) {
    if (entries.size >= 100 && !entries.has(input.id))
      return yield* new ExternalMcpError({ reason: "limit-exceeded" });
    const existing = entries.get(input.id);
    if (
      existing &&
      existing.snapshot.state !== "disconnected" &&
      existing.snapshot.state !== "failed"
    )
      return yield* new ExternalMcpError({ reason: "conflict", id: input.id });
    if (input.config.transport === "http") {
      const url = yield* Effect.try({
        try: () => new URL(input.config.transport === "http" ? input.config.url : ""),
        catch: () => new ExternalMcpError({ reason: "invalid-config" }),
      });
      if (!["http:", "https:"].includes(url.protocol) || url.username || url.password)
        return yield* new ExternalMcpError({ reason: "invalid-config" });
    } else if (input.config.args.length > 100 || input.config.args.some((arg) => arg.length > 4096))
      return yield* new ExternalMcpError({ reason: "limit-exceeded" });
    const revision = (existing?.revision ?? 0) + 1;
    const generation = nextGeneration();
    yield* sql`INSERT INTO external_mcp_connections (id, name, config_json, approved, revision, updated_at) VALUES (${input.id}, ${input.name}, ${JSON.stringify(input.config)}, ${input.approved ? 1 : 0}, ${revision}, ${new Date().toISOString()}) ON CONFLICT(id) DO UPDATE SET name = excluded.name, config_json = excluded.config_json, approved = excluded.approved, revision = excluded.revision, updated_at = excluded.updated_at`.pipe(
      Effect.mapError(() => new ExternalMcpError({ reason: "transport-failed" })),
    );
    const snapshot: ExternalMcpSnapshot = {
      id: input.id,
      name: input.name,
      transport: input.config.transport,
      state: "disconnected",
      generation,
    };
    entries.set(input.id, {
      revision,
      config: input.config,
      approved: input.approved,
      snapshot,
      tools: [],
    });
    return snapshot;
  });
  const connect = (input: { id: string }) =>
    attempt(async (signal) => {
      const entry = get(input.id);
      if (!entry.approved) throw new ExternalMcpError({ reason: "not-approved", id: input.id });
      if (entry.snapshot.state === "connected") return { ...entry.snapshot };
      if (entry.snapshot.state === "connecting")
        throw new ExternalMcpError({ reason: "conflict", id: input.id });
      entry.snapshot = { ...entry.snapshot, state: "connecting", generation: nextGeneration() };
      const generation = entry.snapshot.generation;
      const controller = new AbortController();
      entry.controller = controller;
      const abort = () => controller.abort();
      signal.addEventListener("abort", abort, { once: true });
      const [{ Client }, { StreamableHTTPClientTransport }] = await Promise.all([
        import("@modelcontextprotocol/sdk/client/index.js"),
        import("@modelcontextprotocol/sdk/client/streamableHttp.js"),
      ]);
      if (controller.signal.aborted || entry.snapshot.generation !== generation)
        throw new ExternalMcpError({ reason: "canceled", id: input.id });
      const client = new Client({ name: "t3-external-mcp", version: "1.0.0" });
      entry.client = client;
      try {
        const transport =
          entry.config.transport === "stdio"
            ? await TransportBounds.boundedStdioTransport({
                command: entry.config.command,
                args: entry.config.args,
                ...(entry.config.cwd ? { cwd: entry.config.cwd } : {}),
              })
            : new StreamableHTTPClientTransport(new URL(entry.config.url), {
                fetch: TransportBounds.boundedMcpFetch,
                reconnectionOptions: {
                  maxRetries: 0,
                  maxReconnectionDelay: 1000,
                  initialReconnectionDelay: 1000,
                  reconnectionDelayGrowFactor: 1,
                },
              });
        await client.connect(transport as Parameters<Client["connect"]>[0], {
          signal: controller.signal,
          timeout: 15000,
        });
        const tools: ExternalMcpTool[] = [];
        let cursor: string | undefined;
        const cursors = new Set<string>();
        do {
          const page = await client.listTools(cursor ? { cursor } : {}, {
            signal: controller.signal,
            timeout: 15000,
          });
          for (const tool of page.tools) {
            if (tools.length >= 500 || JSON.stringify(tool).length > 65536)
              throw new ExternalMcpError({ reason: "limit-exceeded", id: input.id });
            tools.push({
              connectionId: input.id,
              name: tool.name,
              ...(tool.description ? { description: tool.description.slice(0, 4096) } : {}),
              inputSchema: tool.inputSchema,
              generation,
            });
          }
          cursor = page.nextCursor;
          if (cursor && cursors.has(cursor))
            throw new ExternalMcpError({ reason: "limit-exceeded", id: input.id });
          if (cursor) cursors.add(cursor);
        } while (cursor && cursors.size < 20);
        if (cursor) throw new ExternalMcpError({ reason: "limit-exceeded", id: input.id });
        if (controller.signal.aborted || entry.snapshot.generation !== generation)
          throw new ExternalMcpError({ reason: "canceled", id: input.id });
        entry.tools = tools;
        entry.snapshot = { ...entry.snapshot, state: "connected" };
        client.onclose = () => {
          if (entry.client === client) {
            entry.client = undefined;
            entry.tools = [];
            entry.snapshot = {
              ...entry.snapshot,
              state: "disconnected",
              generation: nextGeneration(),
            };
          }
        };
        client.setNotificationHandler(
          (await import("@modelcontextprotocol/sdk/types.js")).ToolListChangedNotificationSchema,
          () => {
            if (entry.client !== client) return;
            entry.tools = [];
            entry.snapshot = { ...entry.snapshot, generation: nextGeneration() };
            for (const call of calls.values()) if (call.id === input.id) call.controller.abort();
          },
        );
        return { ...entry.snapshot };
      } catch (error) {
        await client.close().catch(() => undefined);
        if (entry.client === client) {
          entry.client = undefined;
          entry.tools = [];
          entry.snapshot = { ...entry.snapshot, state: "failed" };
        }
        throw error;
      } finally {
        signal.removeEventListener("abort", abort);
      }
    });
  const disconnect = (input: { id: string }) =>
    attempt(async () => {
      const entry = get(input.id);
      await close(entry);
      return { ...entry.snapshot };
    });
  const callTool = (input: ExternalMcpCallInput) =>
    attempt(async (signal) => {
      const fingerprint = JSON.stringify(input);
      const previous = completed.get(input.invocationId);
      if (previous) {
        if (previous.fingerprint !== fingerprint)
          throw new ExternalMcpError({ reason: "conflict" });
        return previous.result;
      }
      if (calls.has(input.invocationId) || seenInvocations.has(input.invocationId))
        throw new ExternalMcpError({ reason: "conflict" });
      if (seenInvocations.size >= 1000) throw new ExternalMcpError({ reason: "limit-exceeded" });
      if (calls.size >= 32 || JSON.stringify(input.arguments).length > 65536)
        throw new ExternalMcpError({ reason: "limit-exceeded" });
      const entry = get(input.id);
      if (!entry.client || entry.snapshot.state !== "connected")
        throw new ExternalMcpError({ reason: "not-connected", id: input.id });
      if (entry.snapshot.generation !== input.generation)
        throw new ExternalMcpError({ reason: "stale-generation", id: input.id });
      const tool = entry.tools.find((item) => item.name === input.name);
      if (!tool) throw new ExternalMcpError({ reason: "not-found", id: input.id });
      const { AjvJsonSchemaValidator } = await import("@modelcontextprotocol/sdk/validation/ajv");
      const validators = new AjvJsonSchemaValidator();
      const valid = validators.getValidator(
        tool.inputSchema as Parameters<typeof validators.getValidator>[0],
      )(input.arguments);
      if (!valid.valid) throw new ExternalMcpError({ reason: "invalid-config", id: input.id });
      if (calls.has(input.invocationId) || seenInvocations.has(input.invocationId))
        throw new ExternalMcpError({ reason: "conflict" });
      if (!entry.client || entry.snapshot.generation !== input.generation)
        throw new ExternalMcpError({ reason: "stale-generation", id: input.id });
      seenInvocations.set(input.invocationId, fingerprint);
      const client = entry.client;
      const controller = new AbortController();
      const abort = () => controller.abort();
      signal.addEventListener("abort", abort, { once: true });
      calls.set(input.invocationId, { id: input.id, controller, fingerprint });
      try {
        const result = await client.callTool(
          { name: input.name, arguments: input.arguments },
          undefined,
          { signal: controller.signal, timeout: Math.min(input.timeoutMs ?? 30000, 120000) },
        );
        if (JSON.stringify(result).length > 1048576)
          throw new ExternalMcpError({ reason: "limit-exceeded", id: input.id });
        const output = { result };
        completed.set(input.invocationId, { fingerprint, result: output });
        if (completed.size > 100) completed.delete(completed.keys().next().value!);
        return output;
      } catch (error) {
        if (controller.signal.aborted)
          throw new ExternalMcpError({ reason: "canceled", id: input.id });
        throw error;
      } finally {
        calls.delete(input.invocationId);
        signal.removeEventListener("abort", abort);
      }
    });
  return ExternalMcpConnections.of({
    configure,
    connect,
    disconnect,
    reload: (input) => disconnect(input).pipe(Effect.flatMap(() => connect(input))),
    list: () =>
      Effect.sync(() => ({
        connections: [...entries.values()].map((entry) => ({ ...entry.snapshot })),
      })),
    remove: (input) =>
      disconnect(input).pipe(
        Effect.flatMap(() => sql`DELETE FROM external_mcp_connections WHERE id = ${input.id}`),
        Effect.tap(() => Effect.sync(() => entries.delete(input.id))),
        Effect.asVoid,
        Effect.mapError(() => new ExternalMcpError({ reason: "transport-failed", id: input.id })),
      ),
    searchTools: (input) =>
      Effect.sync(() => {
        const tools = [...entries.values()]
          .filter((entry) => !input.connectionId || entry.snapshot.id === input.connectionId)
          .flatMap((entry) => entry.tools)
          .filter((tool) =>
            `${tool.name} ${tool.description ?? ""}`
              .toLocaleLowerCase()
              .includes(input.query.toLocaleLowerCase()),
          );
        const limit = Math.min(input.limit ?? 30, 100);
        return { tools: tools.slice(0, limit), truncated: tools.length > limit };
      }),
    callTool,
    cancelCall: (input) => Effect.sync(() => calls.get(input.invocationId)?.controller.abort()),
  });
});
export const layer = Layer.effect(ExternalMcpConnections, make);
