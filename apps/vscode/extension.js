const vscode = require("vscode");

const PI_T3_URL = "http://192.168.50.13:3773";
const INTEGRATED_BROWSER = "workbench.action.browser.open";

async function openPi() {
  const url = new URL(PI_T3_URL);
  if ((await vscode.commands.getCommands(true)).includes(INTEGRATED_BROWSER)) {
    await vscode.commands.executeCommand(INTEGRATED_BROWSER, {
      url: url.href,
      reuseUrlFilter: `${url.origin}/**`,
    });
    return;
  }
  await vscode.env.openExternal(vscode.Uri.parse(url.href, true));
}

function activate(context) {
  const open = () =>
    openPi().catch((error) => {
      const message = error instanceof Error ? error.message : String(error);
      void vscode.window.showErrorMessage(`Could not open T3 Code on the Pi: ${message}`);
    });
  const provider = {
    getTreeItem: (item) => item,
    getChildren: () => {
      const item = new vscode.TreeItem("Open T3 Code on Raspberry Pi");
      item.iconPath = new vscode.ThemeIcon("browser");
      item.command = { command: "t3CodeSource.open", title: "Open T3 Code on Raspberry Pi" };
      return [item];
    },
  };
  const view = vscode.window.createTreeView("t3CodeSource.actions", { treeDataProvider: provider });
  context.subscriptions.push(
    view,
    vscode.commands.registerCommand("t3CodeSource.open", open),
    vscode.commands.registerCommand("t3CodeSource.chooseCheckout", open),
    vscode.commands.registerCommand("t3CodeSource.stop", () => {}),
    view.onDidChangeVisibility(({ visible }) => {
      if (visible) void open();
    }),
  );
  if (view.visible) void open();
}

module.exports = { activate };
