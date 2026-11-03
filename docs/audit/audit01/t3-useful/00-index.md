# Dux ideas that fit T3 Code

This directory filters Audit 01 through T3 Code's product goals. It is a shortlist, not a parity backlog. A Dux feature belongs here only when it improves remote agent control, multi-client safety, provider reliability, code review, open distribution, or supportability without weakening T3 Code's typed contracts and simple architecture.

Equivalent findings are omitted because T3 Code already has the useful outcome. The raw comparison and source evidence remain in the [Audit 01 inventory](../00-inventory.md).

## Delivery order

Rank is delivery order, not abstract importance. Build first addresses current safety, reliability, or daily review problems. Build next is useful after its listed prerequisites. Demand-gated work needs repeated user or support evidence. Adapt keeps the outcome but uses an operating-system or distribution control instead of copying Dux. Cost, reach, risk, reversibility, and prerequisites break close calls. Sizes are relative: small fits one narrow client or server boundary, medium crosses a contract or workflow, and large changes several adapters or clients.

| Rank | Finding                                                                                                  | Group        | Size   | Reach                                     | Why it sits here                                                                                                                                              |
| ---: | -------------------------------------------------------------------------------------------------------- | ------------ | ------ | ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|    1 | [A01-GIT-001](01-build-first.md#1-a01-git-001-commit-reviewed-files-without-replacing-the-real-index)    | Build first  | Medium | Server, every client                      | The current commit path can silently replace the user's staging decisions. Preserve unrelated index entries before adding another Git control.                |
|    2 | [A01-WS-007](01-build-first.md#2-a01-ws-007-warn-about-concurrent-writers-in-one-checkout)               | Build first  | Medium | Server, every client                      | Two live agents can silently edit the same checkout. Conflict identity must come from running provider sessions, not stale thread records.                    |
|    3 | [A01-OPS-005](01-build-first.md#3-a01-ops-005-lock-each-writable-t3-home-to-one-server)                  | Build first  | Medium | Desktop and headless server               | Two servers can compete over the same mutable state. The lock must precede migrations and other writes.                                                       |
|    4 | [A01-DATA-002](01-build-first.md#4-a01-data-002-state-what-thread-deletion-erases)                       | Build first  | Small  | Server, every client                      | This closes a misleading destructive-action boundary without attempting a risky cross-provider purge.                                                         |
|    5 | [A01-GIT-007](01-build-first.md#5-a01-git-007-search-within-changed-files)                               | Build first  | Small  | Every client                              | It improves changed-file review using metadata clients already hold. It adds no server work or persisted state.                                               |
|    6 | [A01-WS-011](01-build-first.md#6-a01-ws-011-make-resume-outcomes-explicit)                               | Build first  | Large  | Server, every client and provider adapter | Resume reliability is central to remote control. Unknown failures must stay non-destructive.                                                                  |
|    7 | [A01-OPS-001](02-useful-next.md#7-a01-ops-001-refuse-high-growth-work-when-disk-space-is-critically-low) | Build next   | Medium | Server, every client                      | It protects unattended hosts, but false refusals have operational cost. Start with disk only and one server policy.                                           |
|    8 | [A01-DATA-001](02-useful-next.md#8-a01-data-001-keep-recoverable-database-backups)                       | Build next   | Medium | Server                                    | Backups matter after the state owner and low-disk behavior are settled. Restore proof matters more than backup settings.                                      |
|    9 | [A01-WS-003](02-useful-next.md#9-a01-ws-003-fork-code-from-an-existing-checkpoint)                       | Build next   | Large  | Server, every client                      | This is a strong agent workflow, but it crosses checkpoint lifetime, Git worktrees, contracts, and navigation. Excluding untracked files keeps it reversible. |
|   10 | [A01-OPS-007](02-useful-next.md#10-a01-ops-007-use-native-release-verification)                          | Build next   | Medium | Desktop and CLI release pipelines         | Release trust benefits every user. Channel owners and signing credentials must exist before the workflow can fail closed.                                     |
|   11 | [A01-OPS-008](02-useful-next.md#11-a01-ops-008-name-active-work-before-desktop-shutdown)                 | Demand-gated | Small  | Desktop host only                         | The existing quit guard already covers accidental exits. Add runtime counts only if users still terminate active local work.                                  |
|   12 | [A01-OPS-002](02-useful-next.md#12-a01-ops-002-copy-a-redacted-diagnostic-summary)                       | Demand-gated | Medium | Server and support clients                | Existing diagnostics and provider probes supply the fields. Build the export only when support requests show a stable allowlist.                              |
|   13 | [A01-GIT-005](02-useful-next.md#13-a01-git-005-discard-one-reviewed-file)                                | Demand-gated | Medium | Server, every client                      | The terminal and editors already cover it. A destructive path API is justified only by repeated review exits.                                                 |
|   14 | [A01-OPS-006](03-adapt-before-adopting.md#14-a01-ops-006-document-encrypted-deployments)                 | Adapt        | Small  | Self-hosted environments                  | Native disk encryption already does the job. T3 only needs an accurate state map when operators ask for this threat model.                                    |

## Recommendation map

| Recommendation | Count | File                                     |
| -------------- | ----: | ---------------------------------------- |
| Build first    |     6 | [Ranks 1 through 6](01-build-first.md)   |
| Build next     |     4 | [Ranks 7 through 10](02-useful-next.md)  |
| Demand-gated   |     3 | [Ranks 11 through 13](02-useful-next.md) |
| Adapt          |     1 | [Rank 14](03-adapt-before-adopting.md)   |

## Product test

The shortlist favors behavior that works when the server is remote, two clients are connected, a provider crashes, or the desktop host shuts down. The environment server owns filesystem mutations. Destructive commands carry the revision or content fingerprint the user reviewed. Clients reload server state after reconnecting.

Daily work gets priority over parity. Forking code at a checkpoint, reviewing changed files, and publishing a commit belong in T3. New settings and background workers need evidence that the existing prompt, terminal, Git, provider, or operating-system behavior does not already solve the problem.

Keep client work cheap. Search and navigation should use file metadata already in memory. Do not fetch diff bodies or query the server for each keystroke.

Provider differences stay in adapters. T3 should report each resume outcome without copying provider history or pretending every provider supports the same operations.

Diagnostics should reuse the fields T3 already collects. Credentials, environment variables, prompts, transcripts, diffs, command lines, and absolute paths do not belong in a support export.

A desktop-only behavior belongs here only when desktop owns the environment lifecycle. That is why the active-work quit warning stays.

Put policy in the layer that owns it. The server protects its database and state root. The operating system owns service restart, process limits, and disk encryption. Release automation uses the signing and provenance mechanisms of each distribution channel.

The audit findings not listed here should not become roadmap items by default. T3 already covers the common need behind per-thread instructions, permission modes, provider compatibility checks, commit-writing rules, and prompt reuse through prompts, `AGENTS.md`, typed runtime modes, adapter-specific checks, repository conventions, and prompt stashes. Project scripts cover named shell commands. Typed settings and `t3.json` do not need a second all-settings format. Git already inventories worktrees. A user can explicitly restart a thread, so boot schedulers and provider-history scans need incident data before they exist. Generic PTY providers, copied provider conversations, a persistent staging UI, automatic terminal clearing, regex-driven PTY automation, scheduled cross-thread wakeups, AMQ routing, full-list manual ordering, cascading worktree deletion, bulk process termination, configurable shell arguments, and a global input logger do not justify their contracts, workers, privacy risk, or dangerous state changes.
