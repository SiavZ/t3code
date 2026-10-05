const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const path = require("node:path");
const test = require("node:test");
const {
  SourceLauncher,
  isOrphanedRunner,
  isOwnedRunner,
  pairingUrlFromLine,
  redactOutputLine,
} = require("./launcher.cjs");

const root = path.resolve(__dirname, "..", "..");
const tokenUrl = "http://localhost:5733/pair#token=SECRET_123";

/** In-memory runner record, so tests never write into the checkout's real dev state. */
function memoryRecord(initial, orphan = () => false) {
  const record = { value: initial, stopped: [] };
  record.api = {
    read: () => record.value,
    write: (pid) => (record.value = { pid }),
    clear: (pid, previous) => {
      if (record.value?.pid === pid) record.value = previous;
    },
    isOrphan: orphan,
    stopGroup: (pid) => record.stopped.push(pid),
  };
  return record;
}

const SourceLauncherForTest = class extends SourceLauncher {
  constructor(dir, options = {}) {
    super(dir, { runnerRecord: memoryRecord().api, ...options });
  }
};

function processFixture() {
  const child = new EventEmitter();
  child.pid = 43210;
  child.stdout = new EventEmitter();
  child.stderr = new EventEmitter();
  child.kill = () => {};
  return child;
}

test("extracts only a local HTTP pairing URL from Effect's startup log", () => {
  const line = `\x1b[32mINFO\x1b[0m Authentication required. pairingUrl: ${tokenUrl}`;
  assert.equal(pairingUrlFromLine(line).href, tokenUrl);
  assert.equal(pairingUrlFromLine("http://evil.example/pair#token=SECRET"), undefined);
  assert.equal(pairingUrlFromLine("https://localhost:5733/pair#token=SECRET"), undefined);
  assert.equal(pairingUrlFromLine("http://localhost:5733/"), undefined);
});

test("redacts every credential form the dev server prints before it reaches VS Code", () => {
  const lines = [
    `\x1b[32mINFO\x1b[0m Authentication required. pairingUrl: ${tokenUrl}`,
    'timestamp=… message="Authentication required." pairingUrl=http://localhost:5733/pair#token=SECRET_123',
    "Token: SECRET_123",
    '{"pairingUrl":"http://localhost:5733/pair#token=SECRET_123","token":"SECRET_123"}',
    "http://localhost:5733/?token=SECRET_123&next=/",
  ];
  for (const line of lines) {
    const shown = redactOutputLine(line);
    assert.doesNotMatch(shown, /SECRET_123/);
    assert.match(shown, /<redacted>/);
    assert.ok(!shown.includes("\x1b"));
  }
  assert.equal(
    redactOutputLine("Server listening on http://127.0.0.1:13773"),
    "Server listening on http://127.0.0.1:13773",
  );
});

test("Stop during startup cancels it, and the next start waits for that runner to exit", async () => {
  const children = [processFixture(), processFixture()];
  const exits = [];
  const order = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: () => {
      order.push("spawn");
      return children.shift();
    },
    stopProcess: () =>
      new Promise((resolve) => {
        exits.push(() => {
          order.push("first runner exited");
          resolve();
        });
      }),
  });
  const first = launcher.start();
  launcher.stop();
  await assert.rejects(first, (error) => error.cancelled === true);
  assert.equal(launcher.status, "stopped");

  const second = launcher.start();
  assert.notEqual(second, first);
  assert.equal(launcher.status, "starting");
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(order, ["spawn"]);
  exits[0]();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(order, ["spawn", "first runner exited", "spawn"]);
  launcher.child.stdout.emit("data", `pairingUrl: ${tokenUrl}\n`);
  assert.equal((await second).href, tokenUrl);
  assert.equal(launcher.status, "ready");
});

test("Stop while clearing a leftover runner cancels the start without spawning", async () => {
  const record = memoryRecord({ pid: 9003 }, () => true);
  let releaseOrphan;
  let spawned = 0;
  const launcher = new SourceLauncher(root, {
    spawnProcess: () => {
      spawned += 1;
      return processFixture();
    },
    stopProcess: () => {},
    runnerRecord: {
      ...record.api,
      stopGroup: () => new Promise((resolve) => (releaseOrphan = resolve)),
    },
  });
  const start = launcher.start();
  assert.equal(launcher.status, "starting");
  launcher.stop();
  assert.equal(launcher.status, "stopped");
  releaseOrphan();
  await assert.rejects(start, (error) => error.cancelled === true);
  assert.equal(spawned, 0);
  assert.equal(record.value, undefined);
});

test("shows runner output, redacted, as it arrives", async () => {
  const child = processFixture();
  const shown = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: () => child,
    stopProcess: () => {},
    onOutput: (line) => shown.push(line),
  });
  const start = launcher.start();
  child.stderr.emit("data", "Error: listen EADDRINUSE: address already in use :::5733\n");
  child.stdout.emit("data", `pairingUrl: ${tokenUrl}\n`);
  await start;
  assert.deepEqual(shown, [
    "Error: listen EADDRINUSE: address already in use :::5733",
    "pairingUrl: http://localhost:5733/pair#token=<redacted>",
  ]);
});

test("starts from the checkout with isolated state and deduplicates startup", async () => {
  const child = processFixture();
  const calls = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: (...args) => {
      calls.push(args);
      return child;
    },
    stopProcess: () => {},
  });
  const first = launcher.start();
  assert.equal(launcher.start(), first);
  assert.equal(calls.length, 1);
  const [command, args, options] = calls[0];
  assert.equal(command, "node");
  assert.deepEqual(args, [
    "scripts/dev-runner.ts",
    "dev",
    "--home-dir",
    path.join(root, ".t3", "vscode-dev"),
  ]);
  assert.equal(options.cwd, root);
  assert.equal(options.env.T3CODE_HOME, undefined);
  assert.equal(options.env.VITE_HTTP_URL, undefined);
  assert.equal(options.env.VITE_WS_URL, undefined);
  assert.ok(options.env.PATH.startsWith(path.join(root, "node_modules", ".bin")));
  child.stdout.emit("data", `INFO pairingUrl: ${tokenUrl.slice(0, 37)}`);
  child.stdout.emit("data", `${tokenUrl.slice(37)}\n`);
  assert.equal((await first).href, tokenUrl);
  assert.equal(launcher.status, "ready");
  assert.equal((await launcher.start()).href, tokenUrl);
  assert.equal(calls.length, 1);
});

test("can retry after startup failure, and stops only the child it started", async () => {
  const children = [processFixture(), processFixture()];
  const stopped = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: () => children.shift(),
    stopProcess: (child) => stopped.push(child),
  });
  const failure = launcher.start();
  launcher.child.emit("exit", 1);
  await assert.rejects(failure, /dev runner exited \(1\)\. Check the T3 Code Source output/);
  assert.equal(launcher.status, "stopped");
  const success = launcher.start();
  const current = launcher.child;
  current.stderr.emit("data", `pairingUrl=${tokenUrl}\n`);
  await success;
  launcher.stop();
  assert.deepEqual(stopped, [current]);
  assert.equal(launcher.status, "stopped");
});

test("fails fast and stops its runner when another server already holds the dev state", async () => {
  const child = processFixture();
  const stopped = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: () => child,
    stopProcess: (target) => stopped.push(target),
  });
  const start = launcher.start();
  // The real dev runner prints this and then stays alive, waiting for file changes.
  child.stderr.emit(
    "data",
    "  A T3 Code server is already running for /repo/.t3/vscode-dev (pid 4242, http://127.0.0.1:13773). Connect to that server, stop it before starting another, or use a different --base-dir.\n",
  );
  await assert.rejects(
    start,
    /already using this checkout's dev state \(pid 4242, http:\/\/127\.0\.0\.1:13773\)/,
  );
  assert.equal(launcher.status, "stopped");
  assert.deepEqual(stopped, [child]);
  // A later pairing line from the abandoned runner cannot flip the state back to ready.
  child.stdout.emit("data", `pairingUrl: ${tokenUrl}\n`);
  assert.equal(launcher.status, "stopped");
});

test("fails fast when the server process crashes but the dev runner keeps watching", async () => {
  const child = processFixture();
  const stopped = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: () => child,
    stopProcess: (target) => stopped.push(target),
  });
  const start = launcher.start();
  child.stdout.emit(
    "data",
    "Failed running 'src/bin.ts'. Waiting for file changes before restarting...\n",
  );
  await assert.rejects(start, /server failed to start/);
  assert.equal(launcher.status, "stopped");
  assert.deepEqual(stopped, [child]);
});

test("gives up after the startup timeout instead of staying on Starting forever", async () => {
  const child = processFixture();
  const stopped = [];
  const launcher = new SourceLauncherForTest(root, {
    spawnProcess: () => child,
    stopProcess: (target) => stopped.push(target),
    startupTimeoutMs: 20,
  });
  await assert.rejects(launcher.start(), /did not start within/);
  assert.equal(launcher.status, "stopped");
  assert.deepEqual(stopped, [child]);
});

test("stops the runner an earlier extension host left behind before starting a new one", async () => {
  const record = memoryRecord({ pid: 9001 }, (entry) => entry.pid === 9001);
  const child = processFixture();
  const order = [];
  const launcher = new SourceLauncher(root, {
    spawnProcess: () => {
      order.push("spawn");
      return child;
    },
    stopProcess: () => {},
    runnerRecord: {
      ...record.api,
      stopGroup: async (pid) => {
        record.api.stopGroup(pid);
        await new Promise((resolve) => setTimeout(resolve, 10));
        order.push("orphan stopped");
      },
    },
  });
  const start = launcher.start();
  assert.equal(launcher.status, "starting");
  assert.equal(launcher.start(), start);
  await new Promise((resolve) => setTimeout(resolve, 30));
  assert.deepEqual(order, ["orphan stopped", "spawn"]);
  assert.deepEqual(record.stopped, [9001]);
  assert.deepEqual(record.value, { pid: child.pid });
  child.stdout.emit("data", `pairingUrl: ${tokenUrl}\n`);
  await start;
  launcher.stop();
  assert.equal(record.value, undefined);
});

test("leaves another window's live runner alone and keeps it recorded when its own start fails", async () => {
  const record = memoryRecord({ pid: 9002 }, () => false);
  const child = processFixture();
  const launcher = new SourceLauncher(root, {
    spawnProcess: () => child,
    stopProcess: () => {},
    runnerRecord: record.api,
  });
  const start = launcher.start();
  assert.deepEqual(record.stopped, []);
  child.emit("exit", 0);
  await assert.rejects(start, /dev runner exited/);
  // The other window's runner is still the one to recover if that window later dies.
  assert.deepEqual(record.value, { pid: 9002 });
});

test("recognises only this checkout's exact dev runner command", () => {
  const devHome = path.join(root, ".t3", "vscode-dev");
  const runner = `node scripts/dev-runner.ts dev --home-dir ${devHome}`;
  assert.equal(
    isOwnedRunner(root, 1, () => runner),
    true,
  );
  // A custom t3CodeSource.nodePath is still this checkout's runner.
  assert.equal(
    isOwnedRunner(root, 1, () => `/opt/homebrew/opt/node@24/bin/${runner}`),
    true,
  );
  assert.equal(
    isOwnedRunner(root, 1, () => runner.replace(devHome, "/elsewhere/.t3/vscode-dev")),
    false,
  );
  assert.equal(
    isOwnedRunner(root, 1, () => `${runner}-other`),
    false,
  );
  assert.equal(
    isOwnedRunner(root, 1, () => "node unrelated.js"),
    false,
  );
  assert.equal(
    isOwnedRunner(root, 1, () => undefined),
    false,
  );
});

test("treats a runner as orphaned only when the window that started it is gone", () => {
  const devHome = path.join(root, ".t3", "vscode-dev");
  const runner = () => `node scripts/dev-runner.ts dev --home-dir ${devHome}`;
  const record = { pid: 500, owner: 600 };
  assert.equal(
    isOrphanedRunner(root, record, runner, () => false),
    true,
  );
  // Another VS Code window still owns it: leave it alone.
  assert.equal(
    isOrphanedRunner(root, record, runner, () => true),
    false,
  );
  // The recorded pid was reused by something else: never stop it.
  assert.equal(
    isOrphanedRunner(
      root,
      record,
      () => "node other.js",
      () => false,
    ),
    false,
  );
  assert.equal(
    isOrphanedRunner(root, undefined, runner, () => false),
    false,
  );
});
