const fs = require("node:fs");
const path = require("node:path");
const vscode = require("vscode");
const { SourceLauncher } = require("./launcher.cjs");

let launcher;

function isSourceRoot(root) {
  if (!fs.existsSync(path.join(root, "scripts", "dev-runner.ts"))) return false;
  try {
    return (
      JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8")).name ===
      "@t3tools/monorepo"
    );
  } catch {
    return false;
  }
}

async function sourceRoot(context, choose = false) {
  if (!choose) {
    const saved = context.globalState.get("checkoutPath");
    if (saved && isSourceRoot(saved)) return saved;
  }
  for (const folder of vscode.workspace.workspaceFolders || []) {
    if (!choose && isSourceRoot(folder.uri.fsPath)) {
      await context.globalState.update("checkoutPath", folder.uri.fsPath);
      return folder.uri.fsPath;
    }
  }
  const selection = await vscode.window.showOpenDialog({
    canSelectFiles: false,
    canSelectFolders: true,
    canSelectMany: false,
    openLabel: "Use T3 Code source checkout",
  });
  const root = selection?.[0]?.fsPath;
  if (!root) return undefined;
  if (!isSourceRoot(root)) {
    vscode.window.showErrorMessage(
      "Select the T3 Code source checkout, containing package.json and scripts/dev-runner.ts.",
    );
    return undefined;
  }
  await context.globalState.update("checkoutPath", root);
  return root;
}

function activate(context) {
  const changed = new vscode.EventEmitter();
  const output = vscode.window.createOutputChannel("T3 Code Source");
  const provider = {
    onDidChangeTreeData: changed.event,
    getTreeItem: (item) => item,
    getChildren: () => {
      const status = launcher?.status || "stopped";
      const stop = new vscode.TreeItem("Stop source server");
      stop.iconPath = new vscode.ThemeIcon("debug-stop");
      stop.command = { command: "t3CodeSource.stop", title: "Stop source server" };
      if (status === "starting") {
        return [new vscode.TreeItem("Starting T3 Code from source…"), stop];
      }
      const open = new vscode.TreeItem("Open T3 Code");
      open.iconPath = new vscode.ThemeIcon("browser");
      open.command = { command: "t3CodeSource.open", title: "Open T3 Code" };
      const choose = new vscode.TreeItem("Choose source checkout");
      choose.iconPath = new vscode.ThemeIcon("folder-opened");
      choose.command = { command: "t3CodeSource.chooseCheckout", title: "Choose source checkout" };
      if (status !== "ready") return [open, choose];
      return [open, stop, choose];
    },
  };
  const view = vscode.window.createTreeView("t3CodeSource.actions", { treeDataProvider: provider });
  let opening;
  let openedForChild;

  async function openSource(choose) {
    const root = await sourceRoot(context, choose);
    if (!root) return;
    if (!launcher || launcher.root !== root) {
      launcher?.stop();
      launcher = new SourceLauncher(root, {
        node: vscode.workspace.getConfiguration("t3CodeSource").get("nodePath", "node"),
        onState: () => changed.fire(),
        onOutput: (line) => output.appendLine(line),
      });
      openedForChild = undefined;
    }
    const url = await launcher.start();
    const firstOpen = openedForChild !== launcher.child;
    const target = firstOpen ? url.href : url.origin;
    await vscode.commands.executeCommand("workbench.action.browser.open", {
      url: target,
      reuseUrlFilter: `${url.origin}/**`,
    });
    openedForChild = launcher.child;
  }

  function open(choose = false) {
    if (opening) return opening;
    const current = openSource(choose)
      .catch((error) => {
        // Stop cancels a pending start on purpose; that is not an error to report.
        if (error?.cancelled) return;
        const message = error instanceof Error ? error.message : String(error);
        // Not awaited: the notification can stay up indefinitely, and Open must work meanwhile.
        void vscode.window.showErrorMessage(message, "Show Output").then((choice) => {
          if (choice === "Show Output") output.show();
        });
      })
      .finally(() => {
        if (opening === current) opening = undefined;
      });
    opening = current;
    return current;
  }

  function stop() {
    // A stopped start must not swallow the next Open click.
    opening = undefined;
    launcher?.stop();
    openedForChild = undefined;
  }

  context.subscriptions.push(
    changed,
    output,
    view,
    vscode.commands.registerCommand("t3CodeSource.open", () => open()),
    vscode.commands.registerCommand("t3CodeSource.chooseCheckout", () => open(true)),
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
