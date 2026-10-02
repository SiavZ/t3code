# T3 Code in VS Code, from source

This optional local extension adds a T3 icon to VS Code's left Activity Bar. Clicking it starts this checkout's web and server dev runner without opening a terminal, then opens the app in VS Code's Integrated Browser. Changes to the web app reload through the normal dev server. The installed T3 Code desktop app is not used.

1. Install Node.js 24, `vp`, and the checkout's dependencies (`vp i`) as described in the checkout's `docs/operations/development.md`.
2. From `apps/vscode`, run `npx @vscode/vsce@4.0.0 package --no-dependencies`.
3. Install the resulting `.vsix` with VS Code's **Extensions: Install from VSIX...** command, then open the T3 Code repository as a workspace and click the T3 icon. Reload VS Code if it was open during installation.

The first click uses the dev server's one-time pairing URL. Later clicks focus the existing browser tab. **Stop source server** in the T3 view shuts down only the process this extension started. The server also stops when the extension host closes. Development state is isolated under this checkout's `.t3/vscode-dev`, never in the installed app's live data directory.

The browser is an editor tab rather than the narrow sidebar itself. The Activity Bar view provides **Open T3 Code** and **Stop source server** actions. In a remote VS Code workspace, install the extension on the workspace side so the server runs where the checkout lives. If VS Code cannot find Node.js, set `t3CodeSource.nodePath` to your Node.js 24 executable.
