# Interface and configuration

## A01-UI-001 — Searchable command palette

**Status:** Equivalent

**Dux behavior:** A fuzzy-searchable palette exposes actions that may not have dedicated keybindings.

**T3 Code behavior:** The command palette has command, file, and project-content modes, reads configured shortcuts, and resolves them against live terminal/preview focus context.

**Impact:** Both products provide a keyboard-first action discovery surface.

**Evidence:**

- Dux: `README.md:123-125`
- T3 Code: `apps/web/src/components/CommandPalette.tsx:382-473`

## A01-UI-002 — Rebindable, context-aware keybindings

**Status:** Equivalent

**Dux behavior:** All actions accept configurable single keys, special keys, and modifier combinations, with an in-app reference overlay.

**T3 Code behavior:** The keybinding schema covers navigation, panes, terminals, preview, palette, files, search, themes, composer, editors, models, thread jumps, and project scripts. Rules can include bounded `when` expressions.

**Impact:** T3 Code meets the remapping outcome and adds context-sensitive bindings, although its discoverability surface is settings/palette rather than Dux's `?` overlay.

**Evidence:**

- Dux: `README.md:246-256`
- T3 Code: `packages/contracts/src/keybindings.ts:4-8`, `packages/contracts/src/keybindings.ts:37-112`, `apps/web/src/components/CommandPalette.tsx:440-473`

## A01-UI-003 — Custom themes and import

**Status:** Equivalent

**Dux behavior:** Users can override semantic theme tokens, with a complete bundled reference theme.

**T3 Code behavior:** T3 Code has a theme editor and imports native or VS Code JSON theme files, including bounded file reads, batch import, light/dark pairing, and conflict handling.

**Impact:** The theme-customization outcome is present and T3 Code adds an interactive/import workflow.

**Evidence:**

- Dux: `README.md:181-244`
- T3 Code: `packages/contracts/src/keybindings.ts:66-70`, `apps/web/src/components/settings/ThemeImportDialog.tsx:189-245`

## A01-UI-004 — Live keyboard and mouse input diagnostics

**Status:** Partial / different

**Dux behavior:** A command-palette action opens a live input debugger. It timestamps normalized keyboard combinations, shows the action each key resolves to in every binding scope, and records mouse event kinds and coordinates; the event history can be scrolled without adding the scroll gestures themselves.

**T3 Code behavior:** The keybinding settings UI records one keyboard shortcut for the rule being edited and warns when that rule conflicts with other bindings. It does not provide a live global event stream, show how one key resolves across all active scopes, or inspect mouse delivery.

**Impact:** T3 Code covers shortcut entry and static conflict detection, but diagnosing whether the OS, browser, terminal surface, or active binding context consumed an unexpected keyboard or mouse event requires external tooling.

**Evidence:**

- Dux: `src/keybindings.rs:1442-1451`, `src/app/input.rs:1826-1876`, `src/app/input.rs:5066-5106`, `src/app/render.rs:4738-4797`
- T3 Code: `apps/web/src/components/settings/KeybindingsSettings.tsx:268-293`, `apps/web/src/components/settings/KeybindingsSettings.tsx:948-958`

## A01-CFG-001 — Exhaustive portable config and config lifecycle CLI

**Status:** Partial / different

**Dux behavior:** First launch writes one exhaustively commented TOML file covering providers, keys, panes, limits, and defaults. CLI commands print its path, diff it from defaults, preview regeneration, and perform guarded reset/factory reset.

**T3 Code behavior:** T3 Code uses typed settings/keybinding/provider stores and GUI surfaces. Its checked-in `t3.json` intentionally exposes only project icon, local/worktree default, and project scripts. No single portable all-settings document or equivalent path/diff/regenerate CLI was found.

**Impact:** T3 Code provides validated settings but is less inspectable and portable as one reviewable configuration artifact; project configuration covers a narrower set of defaults than Dux.

**Evidence:**

- Dux: `README.md:43-45`, `README.md:127-146`
- T3 Code: `packages/contracts/src/t3ProjectFile.ts:62-94`, `apps/server/src/config.ts:117-127`
