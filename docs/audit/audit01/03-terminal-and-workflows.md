# Terminals and user workflows

## A01-TERM-001 — Multiple real PTY terminals per agent/thread

**Status:** Equivalent

**Dux behavior:** Every provider runs in a PTY and each agent can own multiple companion shell terminals in the same worktree.

**T3 Code behavior:** The terminal manager keys sessions by thread and terminal ID, supports open/attach and live event streams, and publishes metadata for all terminal sessions. The web keybinding surface includes new terminal and split-terminal actions.

**Impact:** The core companion-terminal workflow exists, with T3 Code additionally exposing the terminal remotely to web and mobile clients.

**Evidence:**

- Dux: `README.md:17-17`, `README.md:111-113`
- T3 Code: `apps/server/src/terminal/Manager.ts:123-198`, `packages/contracts/src/keybindings.ts:50-57`

## A01-TERM-002 — Terminal history, input, resize, clear, restart, and close

**Status:** Equivalent

**Dux behavior:** PTYs retain capped scrollback, accept interactive input and resize events, and can be managed as live panes.

**T3 Code behavior:** The terminal API provides input, resize, clear, restart, close, and streamed snapshots. History is persisted per thread/terminal ID with a configurable line limit and bounded retention of inactive sessions.

**Impact:** T3 Code covers the interactive and reconnectable terminal lifecycle rather than offering a display-only log.

**Evidence:**

- Dux: `src/pty.rs:1337-1401`, `README.md:281-294`
- T3 Code: `apps/server/src/terminal/Manager.ts:149-198`, `apps/server/src/terminal/Manager.ts:1111-1129`, `apps/server/src/terminal/Manager.ts:1209-1218`, `apps/server/src/terminal/Manager.ts:2490-2654`

## A01-TERM-003 — Pane toggling, maximizing, and terminal splits

**Status:** Equivalent

**Dux behavior:** Dux has left, center, and right panes that can be focused, resized, collapsed, or made fullscreen.

**T3 Code behavior:** The client has configurable actions for sidebar and right-panel toggles, right-panel maximization, diff/preview toggles, and horizontal or vertical terminal splits.

**Impact:** The exact layout model differs, but both let users reclaim screen space and work across agent, terminal, and change-review surfaces.

**Evidence:**

- Dux: `README.md:56-62`
- T3 Code: `packages/contracts/src/keybindings.ts:50-69`

## A01-TERM-004 — Terminal clipboard integration

**Status:** Equivalent

**Dux behavior:** Clipboard writes run off the UI thread and fall back to OSC 52 when no display clipboard is available, including remote SSH/headless use.

**T3 Code behavior:** The Ghostty surface implements platform copy/paste shortcuts and host clipboard paste with bracketed-paste encoding and race suppression.

**Impact:** Both provide practical terminal clipboard behavior. Dux's OSC 52 fallback is terminal-host-oriented; T3 Code delegates clipboard access to its web/desktop host.

**Evidence:**

- Dux: `src/clipboard.rs:83-196`
- T3 Code: `apps/web/src/terminal/ghostty/surface.ts:331-349`, `apps/web/src/terminal/ghostty/surface.ts:814-834`

## A01-TERM-005: User-configurable companion-terminal command and arguments

**Status:** Partial / different

**Dux behavior:** The companion terminal's executable and argument vector are configuration fields. They default to `$SHELL` or `/bin/sh` plus `-l`, and every new companion terminal uses the configured values.

**T3 Code behavior:** The terminal manager selects the host's `$SHELL` or a platform fallback and retries a built-in candidate list when spawning fails. Terminal open and restart requests can supply cwd, dimensions, and environment variables, but not a shell executable or argument vector.

**Impact:** T3 Code starts a resilient platform-appropriate shell, but users cannot select a different shell or required startup flags through T3 Code when the desired profile differs from the server process environment.

**Evidence:**

- Dux: `src/config.rs:199-204`, `src/config.rs:909-915`, `src/config.rs:2616-2627`, `src/app/sessions.rs:759-786`
- T3 Code: `packages/contracts/src/terminal.ts:27-84`, `apps/server/src/terminal/Manager.ts:448-573`, `apps/server/src/terminal/Manager.ts:1152-1161`, `apps/server/src/terminal/Manager.ts:1863-1874`

## A01-FLOW-001 — Reusable macros across agent and terminal surfaces

**Status:** Partial / different

**Dux behavior:** Named snippets can target the agent pane, a companion terminal, or both from one searchable macro surface.

**T3 Code behavior:** Checked-in project scripts run named shell commands in a T3 terminal, while the composer separately persists and restores stashed prompts. Neither source models one reusable item with an `agent`/`terminal`/`both` destination.

**Impact:** Shell shortcuts and prompt reuse both exist, but users cannot define a single cross-surface macro catalog equivalent to Dux.

**Evidence:**

- Dux: `README.md:90-101`
- T3 Code: `packages/contracts/src/t3ProjectFile.ts:27-59`, `apps/web/src/promptStashStore.ts:35-64`, `apps/web/src/components/chat/ComposerStashMenu.tsx:26-36`

## A01-FLOW-002 — Open files and workspaces in a preferred external editor

**Status:** Equivalent

**Dux behavior:** Dux detects installed editors, honors a preferred editor, validates the target path, and launches the editor detached from the TUI.

**T3 Code behavior:** The client chooses and persists an available preferred editor per environment and routes the target to the server's external launcher.

**Impact:** Both products support leaving the built-in diff/browser surface for a native editor without copying paths manually.

**Evidence:**

- Dux: `src/editor.rs:64-135`
- T3 Code: `apps/web/src/editorPreferences.ts:41-115`, `apps/server/src/process/externalLauncher.ts:496-503`
