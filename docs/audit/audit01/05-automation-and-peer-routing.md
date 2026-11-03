# Automation and peer routing

## A01-AUTO-001 — Regex-triggered terminal-output automation

**Status:** Gap

**Dux behavior:** Each session can observe recent terminal output with bounded regexes, detect only fresh occurrences, delay actions with backoff, suppress repeats with cooldown, enforce attempt budgets, and send configured text back to the PTY.

**T3 Code behavior:** The terminal manager exposes open/attach, input, resize, clear, restart, close, and event subscriptions. No production output-rule engine or rule state machine equivalent to Dux's matcher was found.

**Impact:** T3 Code cannot declaratively answer recurring provider prompts, rate-limit messages, or completion markers based on terminal output outside provider-specific adapter code.

**Evidence:**

- Dux: `src/watch/engine.rs:1-55`, `src/watch/engine.rs:272-325`, `src/watch/engine.rs:327-397`
- T3 Code: `apps/server/src/terminal/Manager.ts:123-198`

## A01-AUTO-002 — Per-session automatic terminal clear on task completion

**Status:** Gap

**Dux behavior:** A session setting can automatically clear terminal output after the built-in task-completion rule fires.

**T3 Code behavior:** T3 Code has a manual terminal `clear` operation, but its runtime-mode and terminal contracts contain no task-completion-triggered auto-clear setting.

**Impact:** Long-running restored sessions retain their terminal history until a user or client explicitly clears it.

**Evidence:**

- Dux: `README.md:115-117`, `src/watch/engine.rs:45-55`
- T3 Code: `packages/contracts/src/orchestration.ts:119-129`, `apps/server/src/terminal/Manager.ts:159-178`

## A01-AUTO-003: Periodic cross-session orchestrator checkpoints

**Status:** Gap

**Dux behavior:** A configurable watchdog wakes one live Orchestrator session per project every 15 minutes by default when same-project workers exist. It supplies the live worker roster and asks the orchestrator to poll each worker for status, blockers, ETA, next proof, and escalation needs while avoiding busy sessions and active user input.

**T3 Code behavior:** T3 Code folds provider-native subagent events into a live agent panel with status, activity, and usage. Its orchestration command surface starts turns from submitted messages and has no application-owned timer that wakes a coordinator to supervise other independent threads.

**Impact:** T3 Code makes native subagent work visible, but it does not keep a separate coordinator thread actively checking independent workers after the user steps away.

**Evidence:**

- Dux: `src/config.rs:703-732`, `src/app/orchestrator.rs:44-74`, `src/app/orchestrator.rs:103-281`
- T3 Code: `packages/contracts/src/orchestration.ts:825-965`, `packages/client-runtime/src/state/subagentRuntime.ts:1-18`, `packages/client-runtime/src/state/subagentRuntime.ts:720-755`, `apps/web/src/components/AgentsPanel.tsx:523-540`

## A01-PEER-001 — Host-level, transport-neutral peer routing

**Status:** Gap

**Dux behavior:** `dux peer send/list/sync-amq` resolves immutable session handles, chooses Claude Peers or AMQ, and forces AMQ when a shared workspace makes cwd-based peer identity ambiguous.

**T3 Code behavior:** T3 Code's architecture routes client commands to provider adapters and orchestration events. No host-level cross-provider peer address book, immutable peer handle, or transport-selection command was found.

**Impact:** Provider-native delegation may still work, but one T3 Code thread cannot address another provider/thread through a stable application-owned peer API equivalent to Dux.

**Evidence:**

- Dux: `src/peer.rs:71-158`, `src/peer.rs:389-459`, `README.md:169-179`
- T3 Code: `AGENTS.md:121-134`, `packages/contracts/src/providerInstance.ts:85-95`

## A01-PEER-002 — Durable inbound AMQ queue delivery

**Status:** Gap

**Dux behavior:** Dux watches and polls an AMQ queue, atomically claims files, rejects symlinks, oversized payloads, and unsafe controls, reclaims crash-left inflight items, expires stale messages, and injects valid bodies into the target agent.

**T3 Code behavior:** No AMQ queue drainer, queue ownership model, inflight recovery path, or external message injection transport was found in T3 Code production source.

**Impact:** Messages sent while a target is detached/offline do not have a Dux-compatible durable cross-agent delivery path in T3 Code.

**Evidence:**

- Dux: `src/amq_inject.rs:183-273`, `src/amq_inject.rs:301-320`, `src/amq_inject.rs:322-462`, `src/amq_inject.rs:665-762`
- T3 Code: `AGENTS.md:121-134`, `apps/server/src/terminal/Manager.ts:123-198`
