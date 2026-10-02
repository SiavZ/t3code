const assert = require("node:assert/strict");
const Module = require("node:module");
const test = require("node:test");

const checkout = require("node:path").resolve(__dirname, "..", "..");
const tokenUrl = "http://localhost:5733/pair#token=SECRET_123";

function loadExtension() {
  const commands = new Map();
  const opened = [];
  const errors = [];
  const launchers = [];
  let onVisibility;
  let provider;
  const view = {
    visible: true,
    onDidChangeVisibility: (listener) => { onVisibility = listener; return { dispose() {} }; },
    dispose() {},
  };
  class FakeLauncher {
    constructor(root, options) {
      this.root = root;
      this.options = options;
      this.status = "stopped";
      launchers.push(this);
    }
    async start() {
      if (!this.child) {
        this.child = {};
        this.status = "ready";
        this.options.onState();
      }
      return new URL(tokenUrl);
    }
    stop() {
      this.child = undefined;
      this.status = "stopped";
      this.options.onState();
    }
  }
  const vscode = {
    workspace: {
      workspaceFolders: [{ uri: { fsPath: checkout } }],
      getConfiguration: () => ({ get: () => "node" }),
    },
    window: {
      createTreeView: (_id, options) => { provider = options.treeDataProvider; return view; },
      showErrorMessage: (message) => errors.push(message),
    },
    commands: {
      registerCommand: (id, action) => { commands.set(id, action); return { dispose() {} }; },
      executeCommand: async (id, options) => opened.push({ id, options }),
    },
    EventEmitter: class {
      event = () => ({ dispose() {} });
      fire() {}
      dispose() {}
    },
    TreeItem: function TreeItem(label) { return { label }; },
    ThemeIcon: function ThemeIcon(id) { return { id }; },
  };
  const originalLoad = Module._load;
  Module._load = function (request, parent, isMain) {
    if (request === "vscode") return vscode;
    if (request === "./launcher.cjs" && parent?.filename === require.resolve("./extension.js")) return { SourceLauncher: FakeLauncher };
    return originalLoad.call(this, request, parent, isMain);
  };
  delete require.cache[require.resolve("./extension.js")];
  try {
    const extension = require("./extension.js");
    return { extension, view, commands, opened, errors, launchers, get provider() { return provider; }, reveal: () => onVisibility({ visible: true }) };
  } finally {
    Module._load = originalLoad;
  }
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

test("Activity Bar view opens the source app, reuses browser tab, and offers stop", async () => {
  const host = loadExtension();
  const context = { subscriptions: [] };
  host.extension.activate(context);
  await flush();
  assert.equal(host.launchers.length, 1);
  assert.equal(host.launchers[0].root, checkout);
  assert.deepEqual(host.opened[0], {
    id: "workbench.action.browser.open",
    options: { url: tokenUrl, reuseUrlFilter: "http://localhost:5733/**" },
  });
  host.reveal();
  await flush();
  assert.equal(host.opened[1].options.url, "http://localhost:5733");
  assert.deepEqual(host.provider.getChildren().map((item) => item.label), ["Open T3 Code", "Stop source server"]);
  host.commands.get("t3CodeSource.stop")();
  assert.equal(host.launchers[0].status, "stopped");
  host.reveal();
  await flush();
  assert.equal(host.opened[2].options.url, tokenUrl);
  assert.deepEqual(host.errors, []);
  host.extension.deactivate();
  assert.equal(host.launchers[0].status, "stopped");
});

test("a non-source workspace fails without starting a server", async () => {
  const host = loadExtension();
  host.extension.activate({ subscriptions: [] });
  await flush();
  host.launchers[0].stop();
  // A subsequent command in a different workspace is rejected before spawning.
  const previous = host.launchers.length;
  // The test helper owns the workspace object through the view's activation.
  // Replace the checkout manifest check by temporarily making the folder absent.
  const originalExistsSync = require("node:fs").existsSync;
  require("node:fs").existsSync = () => false;
  try {
    await host.commands.get("t3CodeSource.open")();
  } finally {
    require("node:fs").existsSync = originalExistsSync;
  }
  assert.equal(host.launchers.length, previous);
  assert.match(host.errors.at(-1), /source checkout/);
});
