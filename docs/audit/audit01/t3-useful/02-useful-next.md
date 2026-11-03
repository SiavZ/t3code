# Build next or when demand clears the gate

Ranks 7 through 10 are worthwhile after the first safety tranche. Ranks 11 through 13 stay out of the roadmap until real support or workflow demand clears their stated gate.

## 7. A01-OPS-001. Refuse high-growth work when disk space is critically low

Check free space before worktree creation, provider or terminal launch, update snapshots, and database backup rotation. Use conservative defaults with server-side overrides, not another client settings screen. Return the same typed refusal to every client. Start with disk only. Do not add CPU admission, automatic throttling, or another process supervisor. Service managers already handle host process limits. See [A01-OPS-001](../07-operations-and-data-lifecycle.md).

## 8. A01-DATA-001. Keep recoverable database backups

After the state-root lock and disk policy are in place, reuse SQLite's online backup API to make one consistent backup at a fixed daily cadence and retain a small fixed set. Check the new copy before replacing the oldest backup. Prove one stopped-server restore. Do not copy a live `state.sqlite` without its WAL state, run a full integrity scan on every request, or add a backup settings screen. State plainly that backups exclude repositories and provider-owned history. See [A01-DATA-001](../07-operations-and-data-lifecycle.md).

## 9. A01-WS-003. Fork code from an existing checkpoint

Add "Fork from here" to checkpointed messages. Create the new thread and worktree from that checkpoint's Git ref, then open the returned thread ID. Keep the ref alive until worktree creation finishes and fail without a partial thread if it disappears. Start a fresh provider conversation. Exclude untracked files rather than building a copy manifest or history-seeding system. Users who need untracked content can commit it first. See [A01-WS-003](../01-workspaces-and-sessions.md).

## 10. A01-OPS-007. Use native release verification

Require the existing platform signing credentials for first-party stable desktop releases and enable package-manager provenance where the publisher already supports it. A stable release job should fail before publishing when its required signer is absent. Add a checksum only when a download path will verify it. Add an SBOM only when a distributor or user will consume it. Do not build an application verification subsystem. See [A01-OPS-007](../07-operations-and-data-lifecycle.md).

## 11. A01-OPS-008. Name active work before desktop shutdown

Build this only if users still lose active local work despite the existing hold-or-double-tap quit guard. When desktop owns the bundled server, derive provider and terminal counts from the state already used by the client and put those counts in the guard. Keep the existing graceful shutdown. Do not add orchestration state or a final snapshot. A desktop connected to a separate remote environment must not imply that quitting stops remote work. See [A01-OPS-008](../07-operations-and-data-lifecycle.md).

## 12. A01-OPS-002. Copy a redacted diagnostic summary

Build this only after support repeatedly asks users for the same facts. Add a copy or download action for version identifiers, provider probe summaries, resource telemetry, and trace health that T3 already exposes. Produce bounded text or JSON from an allowlist. Do not add a database scan, archive logs, or create another file in T3 home. Exclude credentials, environment variables, prompts, transcripts, diffs, command lines, and absolute paths. See [A01-OPS-002](../07-operations-and-data-lifecycle.md).

## 13. A01-GIT-005. Discard one reviewed file

Build this only if review users repeatedly leave T3 to undo one path. Use a server-side Git restore for one validated path, require the reviewed fingerprint, and confirm deletion for an untracked file. Reuse the existing checkpoint before mutation. Do not accept directories or add a general file-deletion API. Until demand is clear, the terminal, external editor, and whole-checkpoint restore cover the outcome. See [A01-GIT-005](../04-git-and-source-control.md).
