# Git and source control

## A01-GIT-001 — Change review and standalone staging-area control

**Status:** Partial / different

**Dux behavior:** The right pane is an explicit staging area: users can stage and unstage files independently, inspect highlighted diffs, then commit the staged set.

**T3 Code behavior:** T3 Code reports staged and unstaged changes and renders review diffs. Its commit dialog lets users select files; immediately before commit the server resets the index and adds either the selected paths or all changes. No equivalent persistent stage/unstage UI was found.

**Impact:** Selective commits and rich review are covered, but users who deliberately maintain an index state cannot manage that state as a first-class T3 Code workflow.

**Evidence:**

- Dux: `README.md:103-107`, `src/git.rs:691-979`
- T3 Code: `apps/server/src/vcs/GitVcsDriverCore.ts:1768-1816`, `apps/server/src/vcs/GitVcsDriverCore.ts:1818-1860`, `apps/web/src/components/GitActionsControl.tsx:1625-1636`

## A01-GIT-002 — Commit, push, and conservative pull workflows

**Status:** Equivalent

**Dux behavior:** Users can commit, push, and pull in the Git pane without leaving the app.

**T3 Code behavior:** The Git manager composes branch, commit, push, and PR phases with dirty/detached-state validation. Pull is explicitly fast-forward-only and requires an upstream.

**Impact:** The normal safe source-control loop is fully represented; T3 Code's action pipeline provides more explicit phase progress.

**Evidence:**

- Dux: `README.md:103-109`, `src/git.rs:981-1012`
- T3 Code: `apps/server/src/git/GitManager.ts:2154-2213`, `apps/server/src/git/GitManager.ts:2277-2343`, `apps/server/src/vcs/GitVcsDriverCore.ts:2018-2066`

## A01-GIT-003 — AI-authored commit, branch, and pull-request text

**Status:** Equivalent

**Dux behavior:** Dux sends the staged diff to a provider in oneshot mode to draft an editable Conventional Commit message.

**T3 Code behavior:** The typed text-generation service produces commit messages from staged context and also generates branch names, PR titles/bodies, and thread titles. Git actions select the configured writer model.

**Impact:** Dux's AI commit-message outcome is present; T3 Code extends it to the rest of the publish workflow.

**Evidence:**

- Dux: `README.md:107-107`, `src/provider.rs:141-200`
- T3 Code: `apps/server/src/textGeneration/TextGeneration.ts:13-117`, `apps/server/src/git/GitManager.ts:2219-2245`

## A01-GIT-004 — Branch management and pull-request integration

**Status:** Equivalent

**Dux behavior:** Dux tracks GitHub PR state for agent branches and supports branch/session rename and worktree operations.

**T3 Code behavior:** The Git driver creates, switches, and renames refs. The publish action can create a PR, and production source-control implementations create PRs through GitHub, Bitbucket, and Azure DevOps integrations.

**Impact:** T3 Code meets the Dux branch/PR outcome and goes beyond read-only PR tracking by creating change requests across several hosts.

**Evidence:**

- Dux: `README.md:103-109`, `src/git.rs:1086-1100`
- T3 Code: `apps/server/src/vcs/GitVcsDriverCore.ts:2991-3105`, `apps/server/src/git/GitManager.ts:2308-2321`, `apps/server/src/sourceControl/GitHubCli.ts:444-459`, `apps/server/src/sourceControl/BitbucketApi.ts:965-1010`, `apps/server/src/sourceControl/AzureDevOpsCli.ts:503-522`

## A01-GIT-005: Discard changes to one reviewed file

**Status:** Partial / different

**Dux behavior:** From the unstaged-file review list, users can confirm discarding one path. Dux restores a tracked path from the index or deletes that one untracked file or directory; staged files must be unstaged first.

**T3 Code behavior:** T3 Code can revert a thread to a message checkpoint, restoring the entire worktree and staging state and deleting untracked paths across the worktree. Its checkpoint input has no path selector, and no per-file discard action is exposed by the change-review surface.

**Impact:** T3 Code provides a broader time-based rewind, but undoing one bad file without reverting unrelated files and later conversation state requires an external Git client or editor.

**Evidence:**

- Dux: `src/app/input.rs:1006-1021`, `src/app/input.rs:4753-4773`, `src/git.rs:938-960`
- T3 Code: `apps/server/src/checkpointing/CheckpointStore.ts:25-41`, `apps/server/src/vcs/GitVcsDriver.ts:790-824`, `apps/web/src/components/chat/MessagesTimeline.tsx:1060-1081`

## A01-GIT-006: Project-scoped AI commit-writing instructions

**Status:** Partial / different

**Dux behavior:** The AI commit-message prompt has a global default and an optional override on each registered project. Dux resolves the prompt for the selected session's project before appending the staged diff and invoking the provider.

**T3 Code behavior:** T3 Code offers conventional, repository-conventions, and custom source-control writing styles. Custom instructions are stored in the primary settings and reused for commit and change-request text; there is no project-scoped custom-instruction field.

**Impact:** Both products let users steer generated commit text. A T3 Code environment cannot keep different explicit writing policies for different projects without changing the shared setting between commits.

**Evidence:**

- Dux: `src/config.rs:981-996`, `src/app/input.rs:1054-1082`
- T3 Code: `apps/web/src/components/settings/SourceControlWritingSettings.tsx:43-51`, `apps/web/src/components/settings/SourceControlWritingSettings.tsx:73-136`, `apps/server/src/git/GitManager.ts:630-657`, `packages/contracts/src/t3ProjectFile.ts:62-94`

## A01-GIT-007: Search and cycle changed-file paths

**Status:** Partial / different

**Dux behavior:** The Git files pane has live, case-insensitive path search across both staged and unstaged changes. The first match is selected as the query changes, Enter retains the query, and the next-match action cycles through every match with wraparound.

**T3 Code behavior:** T3 Code can search project paths elsewhere, but its changed-file surfaces render the complete change list for scrolling and direct selection. The web commit dialog and mobile review navigator have no change-scoped path filter or next-match traversal.

**Impact:** Users can locate a repository path generally, but reviewing a large change set requires leaving the change context or scrolling it manually; they cannot jump through matching changed paths while preserving the staged/unstaged review context.

**Evidence:**

- Dux: `src/app/mod.rs:2855-2921`, `src/app/input.rs:794-813`
- T3 Code: `apps/web/src/state/queries.ts:256-299`, `apps/web/src/components/GitActionsControl.tsx:1868-1959`, `apps/mobile/src/features/review/ReviewSheet.tsx:212-336`
