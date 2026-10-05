const assert = require("node:assert/strict");
const { EventEmitter } = require("node:events");
const path = require("node:path");
const test = require("node:test");
const { SourceLauncher, pairingUrlFromLine } = require("./launcher.cjs");

const root = path.resolve(__dirname, "..", "..");
const tokenUrl = "http://localhost:5733/pair#token=SECRET_123";

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

test("starts from the checkout with isolated state and deduplicates startup", async () => {
  const child = processFixture();
  const calls = [];
  const launcher = new SourceLauncher(root, {
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
  const launcher = new SourceLauncher(root, {
    spawnProcess: () => children.shift(),
    stopProcess: (child) => stopped.push(child),
  });
  const failure = launcher.start();
  launcher.child.emit("exit", 1);
  await assert.rejects(failure, /pnpm install/);
  assert.equal(launcher.status, "stopped");
  const success = launcher.start();
  const current = launcher.child;
  current.stderr.emit("data", `pairingUrl=${tokenUrl}\n`);
  await success;
  launcher.stop();
  assert.deepEqual(stopped, [current]);
  assert.equal(launcher.status, "stopped");
});
