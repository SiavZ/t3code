const fs = require("node:fs");
const path = require("node:path");
const vscode = require("vscode");
const { SourceLauncher } = require("./launcher.cjs");

let launcher;

function sourceRoot() {
  for (const folder of vscode.workspace.workspaceFolders || []) {
    const root = folder.uri.fsPath;
    if (!fs.existsSync(path.join(root, "scripts", "dev-runner.ts"))) continue;
    try {
      if (JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).name === "@t3tools/monorepo") {
        return root;
      }
    } catch { /* Other workspace folders are not T3 Code checkouts. */ }
  }
  return undefined;
}

function activate(context) {
  const changed = new vscode.EventEmitter();
  const provider = {
    onDidChangeTreeData: changed.event,
    getTreeItem: (item) => item,
    getChildren: () => {
      const status = launcher?.status || "stopped";
      if (status === "starting") {
        return [new vscode.TreeItem("Starting T3 Code from source…")];
      }
      const open = new vscode.TreeItem("Open T3 Code");
      open.iconPath = new vscode.ThemeIcon("browser");
      open.command = { command: "t3CodeSource.open", title: "Open T3 Code" };
      if (status !== "ready") return [open];
      const stop = new vscode.TreeItem("Stop source server");
      stop.iconPath = new vscode.ThemeIcon("debug-stop");
      stop.command = { command: "t3CodeSource.stop", title: "Stop source server" };
      return [open, stop];
    },
  };
  const view = vscode.window.createTreeView("t3CodeSource.actions", { treeDataProvider: provider });
  let opening;
  let openedForChild;

  async function open() {
    if (opening) return opening;
    const root = sourceRoot();
    if (!root) {
      vscode.window.showErrorMessage("Open the T3 Code source checkout as a VS Code workspace first.");
      return;
    }
    if (!launcher || launcher.root !== root) {
      launcher?.stop();
      launcher = new SourceLauncher(root, {
        node: vscode.workspace.getConfiguration("t3CodeSource").get("nodePath", "node"),
        onState: () => changed.fire(),
      });
      openedForChild = undefined;
    }
    opening = (async () => {
      const url = await launcher.start();
      const firstOpen = openedForChild !== launcher.child;
      const target = firstOpen ? url.href : url.origin;
      await vscode.commands.executeCommand("workbench.action.browser.open", {
        url: target,
        reuseUrlFilter: `${url.origin}/**`,
      });
      openedForChild = launcher.child;
    })();
    try {
      await opening;
    } catch (error) {
      vscode.window.showErrorMessage(error instanceof Error ? error.message : String(error));
    } finally {
      opening = undefined;
    }
  }

  function stop() {
    launcher?.stop();
    openedForChild = undefined;
  }

  context.subscriptions.push(
    changed,
    view,
    vscode.commands.registerCommand("t3CodeSource.open", open),
    vscode.commands.registerCommand("t3CodeSource.stop", stop),
    view.onDidChangeVisibility(({ visible }) => {
      if (visible) void open();
    }),
    { dispose: stop },
  );
  if (view.visible) void open();
}

function deactivate() {
  launcher?.stop();
}

module.exports = { activate, deactivate };
