const { execFileSync, spawn } = require("node:child_process");
const fs = require("node:fs");
const { platform } = require("node:os");
const path = require("node:path");
const { stripVTControlCharacters } = require("node:util");

// The startup URL contains a one-time administrative credential. Never persist it to VS Code
// workspace state, and pass runner output through redactOutputLine before showing it.
function pairingUrlFromLine(line) {
  const candidate = line.match(
    /https?:\/\/(?:localhost|127\.0\.0\.1|\[::1\]):\d+\/pair#token=[A-Za-z0-9_-]+/,
  )?.[0];
  if (!candidate) return undefined;
  const url = new URL(candidate);
  return url.protocol === "http:" ? url : undefined;
}

/** A runner output line as shown in VS Code: no terminal colors, and every token value masked. */
function redactOutputLine(line) {
  return stripVTControlCharacters(line).replace(
    /(token"?\s*[=:]\s*"?)[^\s&"',}]+/gi,
    "$1<redacted>",
  );
}

const SEE_OUTPUT = "Check the T3 Code Source output for details.";

// The dev runner stays alive when its server process fails (node --watch waits for file changes),
// so a failed start never exits. These lines mean the server gave up; surface them instead of
// waiting for a pairing URL that will never come.
const STARTUP_FAILURES = [
  {
    pattern: /A T3 Code server is already running for .+? \(pid (\d+), (https?:\/\/[^)\s]+)\)/,
    message: (match) =>
      `Another T3 Code server is already using this checkout's dev state (pid ${match[1]}, ${match[2]}). Stop it, then open T3 Code again.`,
  },
  {
    pattern: /Failed running '.+?'\. Waiting for file changes before restarting/,
    message: () => `The T3 Code server failed to start. ${SEE_OUTPUT}`,
  },
];

function startupFailureFromLine(line) {
  for (const failure of STARTUP_FAILURES) {
    const match = line.match(failure.pattern);
    if (match) return failure.message(match);
  }
  return undefined;
}

const STARTUP_TIMEOUT_MS = 5 * 60 * 1000;

/** Rejection of a start that Stop cancelled. Callers treat it as an outcome, not an error. */
function startCancelled() {
  const error = new Error("T3 Code was stopped before it finished starting.");
  error.cancelled = true;
  return error;
}

function devHome(root) {
  return path.join(root, ".t3", "vscode-dev");
}

function runnerRecordPath(root) {
  return path.join(devHome(root), "vscode-launcher-runner.json");
}

// An extension host that VS Code restarts while unresponsive never runs deactivate(), so the
// detached runner it spawned keeps holding the dev state under launchd. The launcher records the
// process group it owns and, on the next start, stops it if it is still this checkout's runner.
function processCommand(pid) {
  try {
    return execFileSync("ps", ["-ww", "-o", "command=", "-p", String(pid)], {
      encoding: "utf8",
    }).trim();
  } catch {
    return undefined;
  }
}

function isOwnedRunner(root, pid, readCommand = processCommand) {
  const command = readCommand(pid);
  // Any Node executable (t3CodeSource.nodePath), but exactly this checkout's runner and dev state.
  return command?.endsWith(` scripts/dev-runner.ts dev --home-dir ${devHome(root)}`) === true;
}

function readRunnerRecord(root) {
  try {
    const record = JSON.parse(fs.readFileSync(runnerRecordPath(root), "utf8"));
    if (!Number.isInteger(record.pid) || record.pid <= 1) return undefined;
    return { pid: record.pid, owner: Number.isInteger(record.owner) ? record.owner : undefined };
  } catch {
    return undefined;
  }
}

function writeRunnerRecord(root, pid) {
  try {
    fs.mkdirSync(devHome(root), { recursive: true });
    fs.writeFileSync(runnerRecordPath(root), JSON.stringify({ pid, owner: process.pid }));
  } catch {}
}

/**
 * Drop this launcher's record. If it replaced another window's still-running runner, put that
 * record back so the runner stays recoverable when its own window later dies.
 */
function clearRunnerRecord(root, pid, previous) {
  try {
    if (readRunnerRecord(root)?.pid !== pid) return;
    if (previous && previous.pid !== pid && isOwnedRunner(root, previous.pid)) {
      fs.writeFileSync(runnerRecordPath(root), JSON.stringify(previous));
    } else {
      fs.rmSync(runnerRecordPath(root));
    }
  } catch {}
}

function processAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === "EPERM";
  }
}

/**
 * A recorded runner is an orphan only when the extension host that started it is gone. A runner
 * whose owner is still alive belongs to another VS Code window and must not be stopped from here.
 */
function isOrphanedRunner(root, record, readCommand = processCommand, ownerAlive = processAlive) {
  if (!record || !isOwnedRunner(root, record.pid, readCommand)) return false;
  return record.owner === undefined || record.owner === process.pid || !ownerAlive(record.owner);
}

function groupAlive(pid) {
  try {
    process.kill(-pid, 0);
    return true;
  } catch {
    return false;
  }
}

/** Stop a runner's process group and wait (up to ~5 s) until it has released its ports. */
async function stopGroup(pid, onSignalError) {
  try {
    process.kill(-pid, "SIGTERM");
  } catch (error) {
    if (error.code !== "ESRCH") onSignalError?.();
    return;
  }
  for (let waited = 0; waited < 5000 && groupAlive(pid); waited += 100) {
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  if (groupAlive(pid)) {
    try {
      process.kill(-pid, "SIGKILL");
    } catch {}
  }
}

/** Stop a runner this launcher spawned. On POSIX the promise settles once its group has exited. */
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
  // The PID belongs to the detached process group we created, not a process
  // discovered by name. The dev runner's own children stay in that group.
  return stopGroup(child.pid, () => child.kill("SIGTERM"));
}

class SourceLauncher {
  constructor(root, options = {}) {
    this.root = root;
    this.node = options.node || "node";
    this.spawnProcess = options.spawnProcess || spawn;
    this.stopProcess = options.stopProcess || stopChild;
    this.onState = options.onState || (() => {});
    this.onOutput = options.onOutput || (() => {});
    this.startupTimeoutMs = options.startupTimeoutMs ?? STARTUP_TIMEOUT_MS;
    this.runnerRecord = options.runnerRecord || {
      read: () => readRunnerRecord(root),
      write: (pid) => writeRunnerRecord(root, pid),
      clear: (pid, previous) => clearRunnerRecord(root, pid, previous),
      isOrphan: (record) => isOrphanedRunner(root, record),
      stopGroup,
    };
    this.child = undefined;
    this.starting = undefined;
    this.cancelStart = undefined;
    this.stopping = undefined;
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
    // Never start next to a runner that still holds this checkout's dev state: one this launcher
    // is still stopping, or one an earlier extension host left behind.
    const previous = this.stopping ? [this.stopping] : [];
    // Only a POSIX runner is a detached process group that can outlive its extension host.
    const orphan = platform() !== "win32" ? this.runnerRecord.read() : undefined;
    if (orphan && this.runnerRecord.isOrphan(orphan)) {
      previous.push(
        Promise.resolve(this.runnerRecord.stopGroup(orphan.pid)).then(() =>
          this.runnerRecord.clear(orphan.pid),
        ),
      );
    }
    if (previous.length === 0) return this.spawnRunner();
    this.setStatus("starting");
    let cancelled = false;
    this.cancelStart = () => (cancelled = true);
    const pendingStart = Promise.all(previous)
      .then(() => {
        if (cancelled) throw startCancelled();
        this.starting = undefined;
        return this.spawnRunner();
      })
      .finally(() => {
        if (this.starting === pendingStart) this.starting = undefined;
      });
    this.starting = pendingStart;
    return pendingStart;
  }

  /** Stop a runner this launcher spawned; the next start waits until it has exited. */
  releaseChild(child) {
    const exited = this.stopProcess(child);
    if (typeof exited?.then !== "function") return;
    const stopping = Promise.resolve(exited)
      .catch(() => {})
      .finally(() => {
        if (this.stopping === stopping) this.stopping = undefined;
      });
    this.stopping = stopping;
  }

  spawnRunner() {
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
    const previousRecord = platform() !== "win32" ? this.runnerRecord.read() : undefined;
    if (platform() !== "win32" && child.pid) this.runnerRecord.write(child.pid);
    const clearRecord = () => this.runnerRecord.clear(child.pid, previousRecord);
    this.clearRecord = clearRecord;
    this.setStatus("starting");
    const starting = new Promise((resolve, reject) => {
      let settled = false;
      // A start that never reports a pairing URL or a known failure still ends: stop the runner
      // we spawned rather than leaving the view on "Starting…" with an orphaned process group.
      const timer = setTimeout(() => {
        if (settled || this.child !== child) return;
        fail(
          new Error(
            `T3 Code did not start within ${Math.round(this.startupTimeoutMs / 1000)} seconds. ${SEE_OUTPUT}`,
          ),
        );
      }, this.startupTimeoutMs);
      timer.unref?.();
      const fail = (error) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (this.child === child) {
          this.child = undefined;
          this.url = undefined;
          this.setStatus("stopped");
          this.releaseChild(child);
          clearRecord();
        }
        reject(error);
      };
      this.cancelStart = () => fail(startCancelled());
      const consume = (stream) => {
        let pending = "";
        stream.on("data", (chunk) => {
          pending += chunk.toString();
          const lines = pending.split(/\r?\n/);
          pending = lines.pop().slice(-4096);
          for (const line of lines) {
            this.onOutput(redactOutputLine(line));
            if (settled || this.child !== child) continue;
            const failure = startupFailureFromLine(line);
            if (failure) {
              fail(new Error(failure));
              continue;
            }
            const url = pairingUrlFromLine(line);
            if (!url) continue;
            settled = true;
            clearTimeout(timer);
            this.url = url;
            this.setStatus("ready");
            resolve(url);
          }
        });
      };
      consume(child.stdout);
      consume(child.stderr);
      child.once("error", (error) => {
        clearTimeout(timer);
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
        clearTimeout(timer);
        clearRecord();
        if (this.child === child) {
          this.child = undefined;
          this.url = undefined;
          this.setStatus("stopped");
        }
        if (!settled) {
          settled = true;
          reject(
            new Error(
              `The T3 Code dev runner exited (${code ?? "signal"}). ${SEE_OUTPUT} The checkout needs its dependencies (vp i) and Node.js 24.`,
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
    // Cancel a pending start too, so the next start launches a fresh runner instead of waiting on
    // this one.
    const cancel = this.cancelStart;
    this.cancelStart = undefined;
    this.starting = undefined;
    cancel?.();
    const child = this.child;
    this.child = undefined;
    this.url = undefined;
    if (this.status !== "stopped") this.setStatus("stopped");
    if (!child) return;
    this.releaseChild(child);
    this.clearRecord?.();
  }
}

module.exports = {
  SourceLauncher,
  isOrphanedRunner,
  isOwnedRunner,
  pairingUrlFromLine,
  redactOutputLine,
  startupFailureFromLine,
  stopChild,
};
