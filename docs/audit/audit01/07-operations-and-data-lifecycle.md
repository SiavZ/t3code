# Operations and data lifecycle

## A01-DATA-001 — SQLite durability, integrity checks, and periodic backup

**Status:** Partial / different

**Dux behavior:** The session database uses WAL and synchronous NORMAL, runs an integrity check on every launch, and periodically creates a SQLite backup on a configurable cadence.

**T3 Code behavior:** T3 Code enables a busy timeout, foreign keys, WAL, and migrations. It snapshots the main database plus WAL/SHM before a service-update trial and can restore that snapshot, but the production backup call is tied to update trials; no launch integrity check or periodic backup worker was found.

**Impact:** Normal multi-process SQLite durability is covered. Corruption detection and recovery from ordinary host failure have less explicit automation than Dux.

**Evidence:**

- Dux: `README.md:313-323`, `src/storage.rs:455-460`, `src/app/workers.rs:3805-3841`
- T3 Code: `apps/server/src/persistence/Layers/Sqlite.ts:33-40`, `apps/server/src/serviceLauncher.ts:53-165`, `apps/server/src/serviceLauncher.ts:375-387`

## A01-OPS-001 — Host-wide resource admission limits

**Status:** Partial / different

**Dux behavior:** Configurable limits cap active panes, companion terminals, and total scrollback; disk warning/high-water thresholds warn and then refuse new agents.

**T3 Code behavior:** The terminal manager caps history lines and retained inactive sessions, and the diagnostics stack records process CPU/RSS history. No corresponding host-wide active-provider/terminal admission cap or disk high-water refusal was found.

**Impact:** T3 Code observes resource use and bounds some retained state, but it lacks Dux's proactive guard against launching more live work than the host or disk budget allows.

**Evidence:**

- Dux: `README.md:281-298`
- T3 Code: `apps/server/src/terminal/Manager.ts:1111-1129`, `apps/server/src/diagnostics/ProcessResourceMonitor.ts:31-84`, `apps/web/src/components/settings/DiagnosticsSettings.tsx:994-1030`

## A01-OPS-002 — One-shot support diagnostics bundle

**Status:** Partial / different

**Dux behavior:** `dux doctor` emits human or JSON output, optionally anonymizes it, and checks database integrity plus AMQ binary/queue, kernel, encryption, and recent lifecycle counters.

**T3 Code behavior:** The Diagnostics settings page exposes live process, resource telemetry, and structured trace health and can open the logs directory. No equivalent one-command anonymized support bundle spanning database and external integrations was found.

**Impact:** Interactive troubleshooting is strong, but users cannot produce one attachable, privacy-filtered triage artifact equivalent to `dux doctor --anonymize`.

**Evidence:**

- Dux: `README.md:325-340`
- T3 Code: `apps/web/src/components/settings/DiagnosticsSettings.tsx:987-1030`, `apps/web/src/components/settings/DiagnosticsSettings.tsx:1125-1174`

## A01-OPS-003 — Structured rotating observability files

**Status:** Equivalent

**Dux behavior:** Dux writes sanitized structured JSON Lines and rotates the log daily.

**T3 Code behavior:** T3 Code writes an NDJSON server trace with configurable minimum level, batch interval, size, and retained-file count, and can also delegate traces to OTLP.

**Impact:** Both products produce machine-readable rotating local observability data. Rotation policy differs (daily versus size/count), while T3 Code adds remote trace export.

**Evidence:**

- Dux: `README.md:258-279`
- T3 Code: `apps/server/src/config.ts:30-68`, `apps/server/src/config.ts:105-127`, `apps/server/src/config.ts:170-180`, `apps/server/src/observability/Layers/Observability.ts:30-68`

## A01-DATA-002 — Session-specific hard purge across all owned stores

**Status:** Gap

**Dux behavior:** A dry-runnable hard purge removes an isolated worktree, provider transcript directories, an exact-owner AMQ inbox, and the SQLite row while redacting session-tagged log fields. Deletion is ordered so the durable row remains recoverable until the last step, and shared history requires explicit residual/workspace-wide consent.

**T3 Code behavior:** Thread deletion stops the provider session, deletes terminal history, clears client state, and optionally removes its orphaned worktree. The server's deletion reactor likewise only stops the provider and closes terminals, while the `thread.deleted` projection marks the row with `deletedAt` rather than hard-deleting it. No user-facing cascade was found for provider-owned transcript/state or for redacting session records in rotated observability files.

**Impact:** Deleting a T3 Code thread is not evidence that all session-associated bytes have been erased, which matters for privacy/export-control workflows that require provable per-session deletion.

**Evidence:**

- Dux: `src/purge.rs:1-51`, `src/purge.rs:573-624`, `README.md:342-374`
- T3 Code: `apps/web/src/hooks/useThreadActions.ts:336-372`, `apps/web/src/hooks/useThreadActions.ts:410-443`, `apps/server/src/orchestration/Layers/ThreadDeletionReactor.ts:40-65`, `apps/server/src/orchestration/Layers/ProjectionPipeline.ts:837-850`

## A01-OPS-004: Live process telemetry and multi-target termination

**Status:** Partial / different

**Dux behavior:** A live resource monitor aggregates CPU, RSS, and child-process counts for Dux, each agent, and each companion terminal. A separate searchable selector can mark selected or all visible runtimes and kill the confirmed set together.

**T3 Code behavior:** Resource Telemetry shows a richer process tree and lets the user send `SIGINT` or `SIGKILL` to an individual provider or terminal root. The server refreshes and matches PID plus start time before signaling, refuses its own PID, and rejects non-T3 descendants. The UI does not expose Dux's filtered multi-select bulk action.

**Impact:** Both products can inspect and stop runaway processes. Dux is faster for clearing a chosen group, while T3 Code applies stronger identity checks to each individual signal.

**Evidence:**

- Dux: `src/app/mod.rs:2227-2263`, `src/app/mod.rs:4015-4064`, `src/app/sessions.rs:2238-2263`, `src/app/input.rs:4692-4744`
- T3 Code: `apps/web/src/components/settings/ResourceTelemetryDiagnostics.tsx:524-557`, `apps/server/src/diagnostics/ProcessDiagnostics.ts:54-151`

## A01-OPS-005: Single-owner exclusion for a shared state root

**Status:** Partial / different

**Dux behavior:** The TUI and every state-mutating CLI command take a nonblocking OS lock keyed by `DUX_HOME`. A collision exits before shared state is touched and reports the holder PID; crashes release the kernel lock automatically.

**T3 Code behavior:** The desktop bootstrap uses Electron's user-data-scoped single-instance lock and redirects a second app launch. The standalone server writes its PID, origin, and port to `server-runtime.json` only after activation and listening; that descriptor is not acquired as an exclusive state-root lock.

**Impact:** Normal desktop launches are serialized, but two headless or manually launched servers can target the same T3 state directory without the early incumbent diagnostic and exclusion Dux applies to all mutating entry points.

**Evidence:**

- Dux: `src/lockfile.rs:1-16`, `src/lockfile.rs:114-166`, `src/main.rs:50-116`
- T3 Code: `apps/desktop/src/app/DesktopClerk.ts:90-145`, `apps/server/src/server.ts:495-525`, `apps/server/src/serverRuntimeState.ts:50-80`

## A01-OPS-006: Opt-in full-state encryption-at-rest setup

**Status:** Partial / different

**Dux behavior:** The production guide identifies the persistent state that remains readable after a cloud-IAM or snapshot compromise and provides an opt-in gocryptfs helper plus a LUKS playbook. The helper validates the passphrase file's permissions, refuses an ambiguous existing cipher directory, and idempotently initializes and mounts an encrypted state root covering provider transcripts, AMQ data, and Dux storage.

**T3 Code behavior:** Operators can place the T3 base directory and configurable Codex or Claude homes on an encrypted mount, and the desktop app uses Electron safe storage for saved-environment bearer tokens. T3 Code does not ship a first-party full-state encryption helper, migration/recovery playbook, or encrypted-mount check for its database, logs, provider histories, and backups.

**Impact:** Host- or cloud-level disk encryption remains available, but operators seeking protection from an attachable disk or copied snapshot must design, migrate, and verify the full T3/provider state layout themselves.

**Evidence:**

- Dux: `dux-amq/README.md:234-254`, `docs/operations/encryption-at-rest.md:42-56`, `dux-amq/scripts/install-gocryptfs.sh:29-108`
- T3 Code: `apps/server/src/cli/config.ts:273-287`, `packages/contracts/src/settings.ts:323-345`, `packages/contracts/src/settings.ts:377-384`, `apps/desktop/src/electron/ElectronSafeStorage.ts:70-85`, `apps/desktop/src/settings/DesktopSavedEnvironments.ts:213-233`

## A01-OPS-007: Verifiable release archives and dependency inventory

**Status:** Partial / different

**Dux behavior:** Every release target is packaged twice and compared, published with a per-target CycloneDX SBOM and GitHub OIDC build-provenance attestation, and listed in a release-wide `SHA256SUMS`. The shell installer verifies the selected archive against that file before extraction.

**T3 Code behavior:** The desktop release can use macOS and Windows platform signing and publishes Electron updater manifests alongside application artifacts. Signing becomes optional when its secrets are absent. The CLI publisher supports package provenance, but the production release invocation does not enable it, and the workflow publishes no SBOM or GitHub build attestation.

**Impact:** T3 Code has native package trust and update-integrity mechanisms, but an operator cannot obtain the Dux combination of an independently checkable archive checksum, dependency inventory, and source-to-artifact attestation from the current release output.

**Evidence:**

- Dux: `.github/workflows/release.yml:90-167`, `.github/workflows/release.yml:182-218`, `install.sh:58-76`, `install.sh:120-142`
- T3 Code: `.github/workflows/release.yml:554-598`, `.github/workflows/release.yml:600-655`, `.github/workflows/release.yml:665-742`, `apps/server/scripts/cli.ts:182-214`

## A01-OPS-008: Active-runtime-aware quit confirmation

**Status:** Partial / different

**Dux behavior:** Quit is immediate when nothing is running. When agents or companion terminals are active, Dux instead opens a cancelable confirmation that reports the number and type of runtimes that will be interrupted.

**T3 Code behavior:** The desktop app offers a configurable hold-or-double-tap quit guard and waits for graceful backend shutdown before exiting. That guard does not query or report active provider and terminal runtimes, so the confirmation is the same whether or not work is running.

**Impact:** T3 Code guards against accidental quit and tears down cleanly, but the user is not told which active work the decision may interrupt before approving shutdown.

**Evidence:**

- Dux: `src/app/input.rs:361-374`, `src/app/render.rs:6713-6725`
- T3 Code: `apps/desktop/src/window/QuitHold.ts:40-170`, `apps/desktop/src/app/DesktopLifecycle.ts:75-123`
