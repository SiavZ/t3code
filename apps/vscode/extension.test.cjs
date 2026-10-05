const assert = require("node:assert/strict");
const fs = require("node:fs");
const Module = require("node:module");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const checkout = path.resolve(__dirname, "..", "..");
const tokenUrl = "http://localhost:5733/pair#token=SECRET_123";

function loadExtension(options = {}) {
  const commands = new Map();
  const opened = [];
  const errors = [];
  const launchers = [];
  const state = options.state || new Map();
  let pickerCalls = 0;
  let hangingStarts = options.hangingStarts || 0;
  let onVisibility;
  let provider;
  const view = {
    visible: true,
    onDidChangeVisibility: (listener) => {
      onVisibility = listener;
      return { dispose() {} };
    },
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
      if (hangingStarts > 0 && !this.child) {
        hangingStarts -= 1;
        // A runner that never reports a pairing URL until Stop cancels it.
        this.status = "starting";
        this.options.onState();
        return new Promise((_resolve, reject) => {
          this.cancel = () => reject(Object.assign(new Error("stopped"), { cancelled: true }));
        });
      }
      if (!this.child) {
        this.child = {};
        this.status = "ready";
        this.options.onState();
      }
      return new URL(tokenUrl);
    }
    stop() {
      this.cancel?.();
      this.cancel = undefined;
      this.child = undefined;
      this.status = "stopped";
      this.options.onState();
    }
  }
  const vscode = {
    workspace: {
      workspaceFolders: options.workspaceFolders || [{ uri: { fsPath: checkout } }],
      getConfiguration: () => ({ get: () => "node" }),
    },
    window: {
      createTreeView: (_id, options) => {
        provider = options.treeDataProvider;
        return view;
      },
      createOutputChannel: () => ({
        appendLine() {},
        show() {},
        dispose() {},
      }),
      showErrorMessage: async (message) => {
        errors.push(message);
      },
      showOpenDialog: async () => {
        pickerCalls += 1;
        return options.selection;
      },
    },
    commands: {
      registerCommand: (id, action) => {
        commands.set(id, action);
        return { dispose() {} };
      },
      executeCommand: async (id, options) => opened.push({ id, options }),
    },
    EventEmitter: class {
      event = () => ({ dispose() {} });
      fire() {}
      dispose() {}
    },
    TreeItem: function TreeItem(label) {
      return { label };
    },
    ThemeIcon: function ThemeIcon(id) {
      return { id };
    },
  };
  const originalLoad = Module._load;
  Module._load = function (request, parent, isMain) {
    if (request === "vscode") return vscode;
    if (request === "./launcher.cjs" && parent?.filename === require.resolve("./extension.js"))
      return { SourceLauncher: FakeLauncher };
    return originalLoad.call(this, request, parent, isMain);
  };
  delete require.cache[require.resolve("./extension.js")];
  try {
    const extension = require("./extension.js");
    const context = {
      subscriptions: [],
      globalState: {
        get: (key) => state.get(key),
        update: async (key, value) => {
          state.set(key, value);
        },
      },
    };
    return {
      extension,
      context,
      state,
      view,
      commands,
      opened,
      errors,
      launchers,
      get pickerCalls() {
        return pickerCalls;
      },
      get provider() {
        return provider;
      },
      reveal: () => onVisibility({ visible: true }),
    };
  } finally {
    Module._load = originalLoad;
  }
}

const flush = () => new Promise((resolve) => setImmediate(resolve));

test("Activity Bar view opens the source app, reuses browser tab, and offers stop", async () => {
  const host = loadExtension();
  host.extension.activate(host.context);
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
  assert.deepEqual(
    host.provider.getChildren().map((item) => item.label),
    ["Open T3 Code", "Stop source server", "Choose source checkout"],
  );
  host.commands.get("t3CodeSource.stop")();
  assert.equal(host.launchers[0].status, "stopped");
  host.reveal();
  await flush();
  assert.equal(host.opened[2].options.url, tokenUrl);
  assert.deepEqual(host.errors, []);
  host.extension.deactivate();
  assert.equal(host.launchers[0].status, "stopped");
});

test("another project selects the source checkout once and remembers it", async () => {
  const state = new Map();
  const project = [{ uri: { fsPath: "/some/other/project" } }];
  const host = loadExtension({
    workspaceFolders: project,
    selection: [{ fsPath: checkout }],
    state,
  });
  host.extension.activate(host.context);
  await flush();
  assert.equal(host.launchers[0].root, checkout);
  assert.equal(host.pickerCalls, 1);
  assert.equal(state.get("checkoutPath"), checkout);
  assert.equal(host.opened[0].options.url, tokenUrl);
  host.extension.deactivate();

  const second = loadExtension({ workspaceFolders: project, state });
  second.extension.activate(second.context);
  await flush();
  assert.equal(second.pickerCalls, 0);
  assert.equal(second.launchers[0].root, checkout);
  second.extension.deactivate();
});

test("a checkout found in the workspace is remembered for other projects", async () => {
  const state = new Map();
  const host = loadExtension({ state });
  host.extension.activate(host.context);
  await flush();
  assert.equal(host.pickerCalls, 0);
  host.extension.deactivate();

  const other = loadExtension({
    workspaceFolders: [{ uri: { fsPath: "/some/other/project" } }],
    state,
  });
  other.extension.activate(other.context);
  await flush();
  assert.equal(other.pickerCalls, 0);
  assert.equal(other.launchers[0].root, checkout);
  other.extension.deactivate();
});

test("Stop is offered while starting, and a stopped start does not block the next open", async () => {
  const host = loadExtension({ hangingStarts: 1 });
  host.extension.activate(host.context);
  await flush();
  assert.deepEqual(
    host.provider.getChildren().map((item) => item.label),
    ["Starting T3 Code from source…", "Stop source server"],
  );
  host.commands.get("t3CodeSource.stop")();
  await flush();
  assert.deepEqual(host.errors, []);
  await host.commands.get("t3CodeSource.open")();
  assert.equal(host.opened.at(-1).options.url, tokenUrl);
  assert.equal(host.launchers[0].status, "ready");
  host.extension.deactivate();
});

test("canceling checkout selection does not start a server or show an error", async () => {
  const host = loadExtension({ workspaceFolders: [{ uri: { fsPath: "/some/other/project" } }] });
  host.extension.activate(host.context);
  await flush();
  assert.equal(host.pickerCalls, 1);
  assert.deepEqual(host.launchers, []);
  assert.deepEqual(host.errors, []);
});

test("rejects an unrelated selected directory without starting a server", async () => {
  const host = loadExtension({
    workspaceFolders: [{ uri: { fsPath: "/some/other/project" } }],
    selection: [{ fsPath: "/some/other/project" }],
  });
  host.extension.activate(host.context);
  await flush();
  assert.deepEqual(host.launchers, []);
  assert.match(host.errors.at(-1), /T3 Code source checkout/);
});

test("Choose source checkout switches the source runner from another project", async () => {
  const alternate = fs.mkdtempSync(
    path.join(process.env.JCODE_SCRATCH_DIR || os.tmpdir(), "t3-vscode-checkout-"),
  );
  fs.mkdirSync(path.join(alternate, "scripts"));
  fs.writeFileSync(path.join(alternate, "scripts", "dev-runner.ts"), "");
  fs.writeFileSync(path.join(alternate, "package.json"), '{"name":"@t3tools/monorepo"}');
  const choices = { selection: [{ fsPath: checkout }] };
  try {
    const host = loadExtension(choices);
    host.extension.activate(host.context);
    await flush();
    const first = host.launchers[0];
    choices.selection = [{ fsPath: alternate }];
    await host.commands.get("t3CodeSource.chooseCheckout")();
    assert.equal(first.status, "stopped");
    assert.equal(host.launchers[1].root, alternate);
    assert.equal(host.state.get("checkoutPath"), alternate);
    host.extension.deactivate();
  } finally {
    fs.rmSync(alternate, { recursive: true, force: true });
  }
});
