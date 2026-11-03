# Workspaces and sessions

## A01-WS-001 — Local/shared and isolated worktree modes

**Status:** Equivalent

**Dux behavior:** A project can launch agents in the registered checkout or in isolated Git worktrees, with global and per-project defaults.

**T3 Code behavior:** `t3.json` supports a `local` or `worktree` default, with project and global overrides. The server creates isolated worktrees when requested.

**Impact:** The core choice between shared files and isolated branches is present in both products.

**Evidence:**

- Dux: `README.md:54-54`, `README.md:148-163`
- T3 Code: `packages/contracts/src/t3ProjectFile.ts:77-93`, `apps/server/src/vcs/GitVcsDriverCore.ts:2759-2794`

## A01-WS-002 — Managed worktree creation and removal

**Status:** Equivalent

**Dux behavior:** Dux creates managed worktrees, guards whole-workspace deletion against registered checkout overlap, detects dirty worktrees, and removes worktrees while optionally preserving the branch.

**T3 Code behavior:** The Git driver creates worktrees from a ref or new branch and removes them with optional force.

**Impact:** Normal isolated-thread lifecycle management has an equivalent server implementation.

**Evidence:**

- Dux: `src/git.rs:216-285`, `src/git.rs:475-559`, `src/git.rs:622-689`
- T3 Code: `apps/server/src/vcs/GitVcsDriverCore.ts:2759-2794`, `apps/server/src/vcs/GitVcsDriverCore.ts:2977-2989`

## A01-WS-003 — Forking copies the current dirty file state

**Status:** Gap

**Dux behavior:** Forking always creates an isolated worktree and mirrors the current worktree contents, so uncommitted tracked and untracked work can seed an alternate approach.

**T3 Code behavior:** Worktree creation runs `git worktree add` from a ref or new branch. The production creation path does not mirror the source checkout's current file contents into the destination.

**Impact:** A T3 Code user must commit, stash, or manually copy dirty work before using a new thread/worktree as a true fork of the current state.

**Evidence:**

- Dux: `README.md:119-121`, `src/git.rs:456-469`
- T3 Code: `apps/server/src/vcs/GitVcsDriverCore.ts:2759-2772`

## A01-WS-004 — Orphan worktree cleanup inventory

**Status:** Partial / different

**Dux behavior:** Dux has an opt-in orphan-pruning action that inventories only managed, Git-registered worktrees, reports dirty state, excludes protected paths, and requires per-item confirmation.

**T3 Code behavior:** Thread deletion detects when the thread is the last reference to its worktree, asks whether to remove it, and removes it after deleting the thread. This is safe local cleanup, but it is not a project-wide inventory for worktrees orphaned by earlier failures or external actions.

**Impact:** The common delete-thread cleanup path is covered; stale managed worktrees outside that path require external discovery.

**Evidence:**

- Dux: `README.md:165-167`, `src/git.rs:622-689`
- T3 Code: `apps/web/src/hooks/useThreadActions.ts:308-334`, `apps/web/src/hooks/useThreadActions.ts:410-443`

## A01-WS-005 — Durable exact provider-session resume

**Status:** Equivalent

**Dux behavior:** Shared sessions capture provider UUIDs and reconnect by exact ID, avoiding recency-based history selection when multiple agents share a checkout.

**T3 Code behavior:** Provider runtime state is persisted per thread. The Codex adapter sends the saved provider thread ID to `thread/resume`, and the Claude adapter supplies its saved session ID as `resume`.

**Impact:** Both products can reconnect a UI thread to the intended provider conversation rather than whichever history is newest.

**Evidence:**

- Dux: `README.md:76-79`, `README.md:161-163`
- T3 Code: `apps/server/src/provider/Layers/CodexSessionRuntime.ts:463-499`, `apps/server/src/provider/Layers/ClaudeAdapter.ts:4154-4178`, `apps/server/src/persistence/ProviderSessionRuntime.ts:238-330`

## A01-WS-006 — Bounded boot-time auto-resume

**Status:** Gap

**Dux behavior:** At launch, Dux can respawn persisted sessions with a stale-worktree filter, bounded concurrency, and dispatch staggering to prevent a provider-process/API thundering herd.

**T3 Code behavior:** T3 Code durably stores resumable provider runtime rows. `ProviderService.startSession` consults that persisted binding when a start request arrives, but no production boot scheduler equivalent to Dux's stale/concurrency/stagger pipeline was found.

**Impact:** After a server restart, T3 Code does not offer Dux's configurable, bounded restoration of all eligible live agent processes.

**Evidence:**

- Dux: `src/auto_resume.rs:1-21`, `src/auto_resume.rs:30-46`, `src/auto_resume.rs:78-132`, `README.md:300-311`
- T3 Code: `apps/server/src/persistence/ProviderSessionRuntime.ts:249-309`, `apps/server/src/provider/Layers/ProviderService.ts:588-658`

## A01-WS-007 — Consent and warning for concurrent shared-checkout writers

**Status:** Gap

**Dux behavior:** Starting a second live agent in the same shared checkout requires confirmation, and Dux shows a persistent warning while its store can see multiple live writers.

**T3 Code behavior:** Local-checkout threads are a first-class environment mode and have a dedicated new-local-chat action. Provider sessions start with the requested or persisted cwd, but no second-writer confirmation or persistent same-checkout warning was found in the production creation/start surfaces.

**Impact:** Two live T3 Code agents can operate on the same working tree, index, and branch without the Dux guard that makes this collision risk explicit.

**Evidence:**

- Dux: `README.md:163-167`
- T3 Code: `packages/contracts/src/t3ProjectFile.ts:77-81`, `packages/contracts/src/keybindings.ts:70-74`, `apps/server/src/provider/Layers/ProviderService.ts:620-658`

## A01-WS-008: Durable user-controlled session order

**Status:** Partial / different

**Dux behavior:** Users can move any agent up or down within its project or sort all agents by update time, creation time, or name. Every resulting order is written to SQLite and restored on the next launch.

**T3 Code behavior:** Projects have a manual sort mode, but the general thread sort contract only offers updated or created time. T3 Code separately persists a user-controlled order for pinned threads, not for the full thread list.

**Impact:** T3 Code users can curate a pinned block, but cannot preserve an arbitrary working order for all threads in a project as they can for Dux agents.

**Evidence:**

- Dux: `src/app/mod.rs:2590-2707`, `src/storage.rs:748-758`, `src/storage.rs:818-833`
- T3 Code: `packages/contracts/src/settings.ts:33-39`, `packages/client-runtime/src/state/threadSort.ts:72-89`, `packages/client-runtime/src/state/threadSort.ts:106-112`, `packages/client-runtime/src/state/threadSort.ts:223-255`

## A01-WS-009: Startup recovery of stranded provider histories

**Status:** Gap

**Dux behavior:** On startup, a background recovery worker scans Claude transcripts and Codex rollouts for sessions whose provider UUID was never captured or whose history remained under an old worktree path. It refuses ambiguous matches, copies Claude artifacts without changing the originals, and stores the recovered exact provider ID only when the binding is missing.

**T3 Code behavior:** T3 Code resumes from the cursor and cwd already stored in the thread's provider-runtime row. The startup path does not scan provider-owned transcript stores to reconstruct a missing binding or relocate history after a workspace path change.

**Impact:** A lost or pre-migration T3 Code binding leaves provider history outside the product's resumable thread model even when the provider still has the transcript on disk.

**Evidence:**

- Dux: `README.md:161-163`, `src/app/mod.rs:1655-1663`, `src/app/workers.rs:2218-2247`, `src/resume_recovery.rs:398-475`
- T3 Code: `apps/server/src/persistence/ProviderSessionRuntime.ts:35-52`, `apps/server/src/provider/Layers/ProviderService.ts:620-658`

## A01-WS-010: Opt-in conversation seeding into a fresh worktree

**Status:** Gap

**Dux behavior:** The Claude wrapper can opt in to copying a parent checkout's session JSONL files into a new worktree's Claude history directory. It skips seeding when the destination already has history and reports partial-copy warnings without blocking provider startup.

**T3 Code behavior:** The production "new thread on branch" action carries the branch, worktree path, and environment mode into a new thread. Its thread-creation payload has no source-thread or history-seed field, and the action does not copy or fork conversation history.

**Impact:** T3 Code can start another thread on the same code state, but cannot offer the existing conversation as optional context for the new line of work.

**Evidence:**

- Dux: `dux-amq/README.md:90-109`, `dux-amq/wrappers/claude-amq:89-160`
- T3 Code: `apps/web/src/hooks/useThreadActionMenu.ts:190-204`, `packages/contracts/src/orchestration.ts:1125-1137`

## A01-WS-011: Automatic fresh-session fallback after a failed resume

**Status:** Partial / different

**Dux behavior:** A resumed provider process that exits with at most five visible lines, or produces no output before its configurable resume timeout, is replaced once with a fresh session. Substantial output suppresses the fallback, and the replacement is not allowed to retry forever.

**T3 Code behavior:** Codex starts a fresh thread for recognized missing-thread errors, and OpenCode creates a session when its saved session is absent. Recovery is adapter-specific: a Codex timeout is deliberately propagated, while Claude passes the saved resume ID directly to the SDK without a shared low-output or timeout fallback.

**Impact:** T3 Code safely recovers known stale bindings for some providers, but a hung or differently reported resume failure can stop the thread for manual recovery where Dux starts a fresh provider session and keeps the agent live.

**Evidence:**

- Dux: `src/app/workers.rs:916-974`, `src/app/workers.rs:1718-1777`, `src/app/input.rs:10681-10939`
- T3 Code: `apps/server/src/provider/Layers/CodexSessionRuntime.ts:442-499`, `apps/server/src/provider/Layers/OpenCodeAdapter.ts:1275-1332`, `apps/server/src/provider/Layers/ClaudeAdapter.ts:4154-4178`, `apps/server/src/provider/Layers/CodexSessionRuntime.test.ts:573-648`

## A01-WS-012: Project-level deletion of managed worktrees

**Status:** Partial / different

**Dux behavior:** Dux distinguishes removing an empty project registration from deleting a project. Project deletion cascades through every agent and requests removal of each managed worktree before removing the project from the app.

**T3 Code behavior:** Removing a project cascades deletion through its threads, but the project settings flow explicitly leaves files on disk. The server-side thread-deletion reactor stops the provider and closes terminal history; it does not remove the thread's worktree. T3 Code offers worktree removal in the separate per-thread deletion flow, not in project-level removal.

**Impact:** Removing a populated project from T3 Code can leave every managed worktree on disk. Users must delete threads individually with worktree cleanup first or remove the remaining worktrees outside the project-removal workflow.

**Evidence:**

- Dux: `src/app/sessions.rs:1655-1744`
- T3 Code: `apps/web/src/components/settings/ProjectSettingsPanel.tsx:670-735`, `apps/web/src/components/settings/ProjectSettingsPanel.tsx:1151-1170`, `apps/server/src/orchestration/decider.ts:301-349`, `apps/server/src/orchestration/Layers/ThreadDeletionReactor.ts:40-65`
