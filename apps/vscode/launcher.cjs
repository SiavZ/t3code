const { spawn } = require("node:child_process");
const { platform } = require("node:os");
const path = require("node:path");

// The startup URL contains a one-time administrative credential. Never write it
// to the extension's output or persist it to VS Code workspace state.
function pairingUrlFromLine(line) {
  const candidate = line.match(
    /https?:\/\/(?:localhost|127\.0\.0\.1|\[::1\]):\d+\/pair#token=[A-Za-z0-9_-]+/,
  )?.[0];
  if (!candidate) return undefined;
  const url = new URL(candidate);
  return url.protocol === "http:" ? url : undefined;
}

function stopChild(child) {
  if (!child.pid) return;
  if (platform() === "win32") {
    const killer = spawn("taskkill", ["/PID", String(child.pid), "/T", "/F"], {
      stdio: "ignore",
      windowsHide: true,
    });
    killer.on("error", () => child.kill());
    return;
  }
  try {
    // The PID belongs to the detached process group we created, not a process
    // discovered by name. The dev runner's own children stay in that group.
    process.kill(-child.pid, "SIGTERM");
  } catch (error) {
    if (error.code !== "ESRCH") child.kill("SIGTERM");
  }
}

class SourceLauncher {
  constructor(root, options = {}) {
    this.root = root;
    this.node = options.node || "node";
    this.spawnProcess = options.spawnProcess || spawn;
    this.stopProcess = options.stopProcess || stopChild;
    this.onState = options.onState || (() => {});
    this.child = undefined;
    this.starting = undefined;
    this.url = undefined;
    this.status = "stopped";
  }

  setStatus(status) {
    this.status = status;
    this.onState(status);
  }

  start() {
    if (this.child && this.url) return Promise.resolve(this.url);
    if (this.starting) return this.starting;
    const env = { ...process.env };
    env.PATH = [path.join(this.root, "node_modules", ".bin"), env.PATH || ""].join(path.delimiter);
    delete env.T3CODE_HOME;
    delete env.VITE_HTTP_URL;
    delete env.VITE_WS_URL;
    delete env.T3CODE_HOST;
    const child = this.spawnProcess(
      this.node,
      ["scripts/dev-runner.ts", "dev", "--home-dir", path.join(this.root, ".t3", "vscode-dev")],
      {
        cwd: this.root,
        env,
        detached: platform() !== "win32",
        windowsHide: true,
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    this.child = child;
    this.setStatus("starting");
    const starting = new Promise((resolve, reject) => {
      let settled = false;
      const consume = (stream) => {
        let pending = "";
        stream.on("data", (chunk) => {
          pending += chunk.toString();
          const lines = pending.split(/\r?\n/);
          pending = lines.pop().slice(-4096);
          for (const line of lines) {
            const url = pairingUrlFromLine(line);
            if (!url || settled || this.child !== child) continue;
            settled = true;
            this.url = url;
            this.setStatus("ready");
            resolve(url);
          }
        });
      };
      consume(child.stdout);
      consume(child.stderr);
      child.once("error", (error) => {
        if (this.child === child) {
          this.child = undefined;
          this.url = undefined;
          this.setStatus("stopped");
        }
        if (!settled) {
          settled = true;
          reject(new Error(`Could not start T3 Code: ${error.message}`));
        }
      });
      child.once("exit", (code) => {
        if (this.child === child) {
          this.child = undefined;
          this.url = undefined;
          this.setStatus("stopped");
        }
        if (!settled) {
          settled = true;
          reject(
            new Error(
              `T3 Code dev runner exited (${code ?? "signal"}). Run pnpm install in the checkout and check your Node.js version (24 required).`,
            ),
          );
        }
      });
    });
    const pendingStart = starting.finally(() => {
      if (this.starting === pendingStart) this.starting = undefined;
    });
    this.starting = pendingStart;
    return pendingStart;
  }

  stop() {
    const child = this.child;
    if (!child) return;
    this.child = undefined;
    this.url = undefined;
    this.setStatus("stopped");
    this.stopProcess(child);
  }
}

module.exports = { SourceLauncher, pairingUrlFromLine, stopChild };
