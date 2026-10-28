# T3 Code on Raspberry Pi in VS Code

This extension opens the existing T3 Code service at `http://192.168.50.13:3773` in VS Code's Integrated Browser. If the Integrated Browser is unavailable, it opens the same address in your default browser.

Click the T3 Activity Bar icon or run **T3 Code: Open Raspberry Pi**. The extension runs in the local UI host, so it works from ordinary local workspaces and Remote SSH workspaces. It does not start or stop any server, select a source checkout, or require Node.js on the client.

Package from this directory with `npx --yes @vscode/vsce@4.0.0 package --no-dependencies`, then install the VSIX through VS Code. After updating an already-running extension host, run **Developer: Reload Window** once to load the new version.

The Pi service must already be running and reachable from the Mac. Pairing or authentication is handled by the T3 app itself.
