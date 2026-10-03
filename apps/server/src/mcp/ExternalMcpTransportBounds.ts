import * as NodeChildProcess from "node:child_process";
import type { Transport } from "@modelcontextprotocol/sdk/shared/transport.js";
import type { JSONRPCMessage } from "@modelcontextprotocol/sdk/types.js";
import { ExternalMcpError } from "../../../../packages/contracts/src/externalMcp.ts";

const MAX_PACKET_BYTES = 1048576;

/** Bound JSON and SSE response bodies before the SDK allocates parsed content. */
export const boundedMcpFetch: typeof fetch = async (input, init) => {
  const response = await fetch(input, init);
  if (response.status === 401 || response.status === 403) {
    await response.body?.cancel();
    throw new ExternalMcpError({ reason: "unsupported-auth" });
  }
  if (Number(response.headers.get("content-length")) > MAX_PACKET_BYTES) {
    await response.body?.cancel();
    throw new ExternalMcpError({ reason: "limit-exceeded" });
  }
  if (!response.body) return response;
  const reader = response.body.getReader();
  let received = 0;
  const body = new ReadableStream<Uint8Array>({
    async pull(controller) {
      try {
        const item = await reader.read();
        if (item.done) {
          controller.close();
          return;
        }
        received += item.value.byteLength;
        if (received > MAX_PACKET_BYTES) {
          await reader.cancel();
          controller.error(new ExternalMcpError({ reason: "limit-exceeded" }));
          return;
        }
        controller.enqueue(item.value);
      } catch (error) {
        controller.error(error);
      }
    },
    cancel: (reason) => reader.cancel(reason),
  });
  return new Response(body, {
    status: response.status,
    statusText: response.statusText,
    headers: response.headers,
  });
};

/** SDK-compatible framing with a hard packet budget and only captured child ownership. */
export async function boundedStdioTransport(config: {
  command: string;
  args: ReadonlyArray<string>;
  cwd?: string;
}): Promise<Transport> {
  const [{ getDefaultEnvironment }, { JSONRPCMessageSchema }] = await Promise.all([
    import("@modelcontextprotocol/sdk/client/stdio.js"),
    import("@modelcontextprotocol/sdk/types.js"),
  ]);
  class BoundedStdio implements Transport {
    onclose?: () => void;
    onerror?: (error: Error) => void;
    onmessage?: (message: JSONRPCMessage) => void;
    private child: NodeChildProcess.ChildProcessWithoutNullStreams | null = null;
    private pending = Buffer.alloc(0);
    async start() {
      if (this.child) throw new Error("Transport already started");
      const child = NodeChildProcess.spawn(config.command, [...config.args], {
        env: getDefaultEnvironment(),
        ...(config.cwd ? { cwd: config.cwd } : {}),
        stdio: "pipe",
      });
      this.child = child;
      child.stderr.resume();
      child.stdout.on("data", (chunk: Buffer) => {
        if (this.pending.length + chunk.length > MAX_PACKET_BYTES) {
          this.onerror?.(new ExternalMcpError({ reason: "limit-exceeded" }));
          void this.close();
          return;
        }
        this.pending = Buffer.concat([this.pending, chunk]);
        let newline = this.pending.indexOf(10);
        while (newline >= 0) {
          const line = this.pending.subarray(0, newline).toString("utf8");
          this.pending = this.pending.subarray(newline + 1);
          try {
            this.onmessage?.(JSONRPCMessageSchema.parse(JSON.parse(line)));
          } catch {
            this.onerror?.(new ExternalMcpError({ reason: "transport-failed" }));
            void this.close();
            return;
          }
          newline = this.pending.indexOf(10);
        }
      });
      child.on("error", (error) => {
        if (!child.pid && this.child === child) {
          this.child = null;
          this.onclose?.();
        }
        this.onerror?.(error);
      });
      child.once("exit", () => {
        if (this.child === child) {
          this.child = null;
          this.pending = Buffer.alloc(0);
          this.onclose?.();
        }
      });
      await new Promise<void>((resolve, reject) => {
        child.once("spawn", resolve);
        child.once("error", reject);
      });
    }
    async send(message: JSONRPCMessage) {
      const encoded = `${JSON.stringify(message)}\n`;
      if (Buffer.byteLength(encoded) > MAX_PACKET_BYTES)
        throw new ExternalMcpError({ reason: "limit-exceeded" });
      const child = this.child;
      if (!child) throw new ExternalMcpError({ reason: "not-connected" });
      await new Promise<void>((resolve, reject) =>
        child.stdin.write(encoded, (error) => (error ? reject(error) : resolve())),
      );
    }
    async close() {
      const child = this.child;
      if (!child || child.exitCode !== null || child.signalCode !== null) return;
      await new Promise<void>((resolve) => {
        const force = setTimeout(() => child.kill("SIGKILL"), 1500);
        force.unref();
        child.once("exit", () => {
          clearTimeout(force);
          resolve();
        });
        child.stdin.end();
        child.kill("SIGTERM");
      });
    }
  }
  return new BoundedStdio();
}
