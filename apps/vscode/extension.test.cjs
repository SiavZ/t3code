const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const test = require("node:test");

function load(integrated) {
  const commands = new Map();
  const opened = [];
  const errors = [];
  let provider;
  let visibility;
  const vscode = {
    commands: {
      getCommands: async () => (integrated ? ["workbench.action.browser.open"] : []),
      executeCommand: async (id, args) => opened.push({ id, ...args }),
      registerCommand: (id, handler) => {
        commands.set(id, handler);
        return { dispose() {} };
      },
    },
    window: {
      createTreeView: (_id, options) => {
        provider = options.treeDataProvider;
        return {
          visible: false,
          dispose() {},
          onDidChangeVisibility(fn) {
            visibility = fn;
            return { dispose() {} };
          },
        };
      },
      showErrorMessage: async (message) => errors.push(message),
    },
    env: { openExternal: async (uri) => opened.push({ url: uri }) },
    Uri: { parse: (value) => value },
    TreeItem: class {
      constructor(label) {
        this.label = label;
      }
    },
    ThemeIcon: class {},
  };
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname, "extension.js"), "utf8"), {
    module,
    URL,
    require(id) {
      assert.equal(id, "vscode", "The button must never load a launcher or process API");
      return vscode;
    },
  });
  module.exports.activate({ subscriptions: [] });
  return { commands, opened, errors, provider, show: () => visibility?.({ visible: true }) };
}

test("the visible button opens the Pi in the integrated browser", async () => {
  const host = load(true);
  const [item] = host.provider.getChildren();
  assert.match(item.label, /Raspberry Pi/);
  await host.commands.get(item.command.command)();
  assert.equal(host.opened[0].url, "http://192.168.50.13:3773/");
  assert.equal(host.opened[0].reuseUrlFilter, "http://192.168.50.13:3773/**");
  assert.deepEqual(host.errors, []);
});

test("external fallback and old choose command both open the Pi without spawning", async () => {
  const host = load(false);
  await host.commands.get("t3CodeSource.open")();
  await host.commands.get("t3CodeSource.chooseCheckout")();
  await host.commands.get("t3CodeSource.stop")();
  assert.deepEqual(
    host.opened.map((x) => x.url),
    ["http://192.168.50.13:3773/", "http://192.168.50.13:3773/"],
  );
});
