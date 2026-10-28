const assert = require("node:assert/strict");
const fs = require("node:fs");
const vm = require("node:vm");
const test = require("node:test");

test("revealing the T3 Activity Bar opens the persistent Pi URL", async () => {
  let reveal;
  const opened = [];
  const module = { exports: {} };
  const vscode = {
    commands: {
      getCommands: async () => ["workbench.action.browser.open"],
      executeCommand: async (command, options) => opened.push(options.url),
      registerCommand: () => ({ dispose() {} }),
    },
    window: {
      createTreeView: () => ({
        visible: false,
        onDidChangeVisibility(fn) {
          reveal = fn;
          return { dispose() {} };
        },
      }),
      showErrorMessage: (message) => {
        throw new Error(message);
      },
    },
  };
  vm.runInNewContext(fs.readFileSync(`${__dirname}/extension.js`, "utf8"), {
    module,
    URL,
    require(id) {
      assert.equal(id, "vscode");
      return vscode;
    },
  });
  module.exports.activate({ subscriptions: [] });
  reveal({ visible: true });
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(opened, ["http://192.168.50.13:3773/"]);
});
