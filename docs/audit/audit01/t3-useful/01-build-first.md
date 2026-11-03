# Build first

Ranks 1 through 6 stop silent state damage, make destructive behavior honest, and improve the daily review loop. Complete them in order unless separate owners can work without changing the same contract.

## 1. A01-GIT-001. Commit reviewed files without replacing the real index

Keep the current selection UI. Serialize the commit by canonical checkout and reject a stale reviewed HEAD or diff fingerprint. A selected-file commit must leave unrelated staged entries exactly as it found them. This preserves the user's staging decisions without adding Dux's persistent staging UI. See [A01-GIT-001](../04-git-and-source-control.md).

## 2. A01-WS-007. Warn about concurrent writers in one checkout

Use the server's live runtime registry and canonical checkout paths. Before starting a second provider in the same checkout, return a typed conflict naming the incumbent thread and require confirmation against that exact conflict. Every connected client must see the warning while both writers remain live. Do not persist another ownership model. This guard covers one T3 environment and cannot detect a process writing through another environment or network mount. See [A01-WS-007](../01-workspaces-and-sessions.md).

## 3. A01-OPS-005. Lock each writable T3 home to one server

Acquire one nonblocking kernel-backed lock before migrations or any other state change. A second headless or desktop server targeting the same T3 home must exit before touching state and report the incumbent PID when available. Lock metadata is diagnostic only. The operating system decides ownership and releases it after a crash. See [A01-OPS-005](../07-operations-and-data-lifecycle.md).

## 4. A01-DATA-002. State what thread deletion erases

Document and show that thread deletion stops the provider and terminals, marks the thread deleted, and may remove its orphaned worktree. It does not prove erasure of provider history, rotated traces, repositories, or backups. Do not call the action a purge or promise erasure across stores T3 does not own. A future regulatory or enterprise purge is a separate design. See [A01-DATA-002](../07-operations-and-data-lifecycle.md).

## 5. A01-GIT-007. Search within changed files

Filter the changed-file metadata already loaded by the client and keep diff bodies lazy. Web needs a review-scoped focus binding and next or previous match. Mobile needs a visible search control with labeled navigation. This needs no server command, offline queue, or new persisted state. See [A01-GIT-007](../04-git-and-source-control.md).

## 6. A01-WS-011. Make resume outcomes explicit

An adapter should report resumed, started fresh because its binding was missing, or failed. Only an adapter that can identify a missing provider session may replace the old binding. A timeout or unknown error stays a failure. Keep the old binding until the replacement returns its new cursor. Checkpoint recovery remains separate because it cannot restore provider-owned conversation history. See [A01-WS-011](../01-workspaces-and-sessions.md).
