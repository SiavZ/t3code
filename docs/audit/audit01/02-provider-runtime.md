# Provider runtime

## A01-PROV-001 — Add any interactive CLI by configuration alone

**Status:** Gap

**Dux behavior:** Every agent runs through a generic PTY provider definition. A command, arguments, and resume argument templates are enough to add an arbitrary CLI without changing Dux code.

**T3 Code behavior:** Provider driver names are forward-compatible slugs, but the runtime explicitly marks a configured instance unavailable when its driver is not registered in the build. The built-in text-generation surface names Codex, Claude, Cursor, Grok, and OpenCode adapters.

**Impact:** Adding a new interactive agent CLI to T3 Code requires a provider adapter/build change, not just configuration. That buys structured protocol integration but does not match Dux's generic CLI escape hatch.

**Evidence:**

- Dux: `README.md:17-17`, `README.md:64-79`
- T3 Code: `packages/contracts/src/providerInstance.ts:58-70`, `apps/server/src/provider/Layers/ProviderInstanceRegistryLive.ts:118-165`, `apps/server/src/textGeneration/TextGeneration.ts:11-11`

## A01-PROV-002 — Multiple configured provider instances and provider-specific settings

**Status:** Equivalent

**Dux behavior:** Users can configure providers, choose a project default, and change the provider used by an existing agent on its next launch.

**T3 Code behavior:** Provider instances have distinct routing IDs independent of driver kind, so multiple instances may use the same driver. Each registered driver validates and materializes its own typed configuration, and the selected instance ID is persisted in the thread's runtime binding.

**Impact:** Both products support selectable, persistently configured provider runtimes; T3 Code models them as typed adapter instances rather than generic commands.

**Evidence:**

- Dux: `README.md:81-88`
- T3 Code: `apps/server/src/provider/builtInProviderCatalog.ts:5-16`, `apps/server/src/provider/Layers/ProviderInstanceRegistryLive.ts:118-186`, `apps/server/src/provider/Layers/ProviderService.ts:314-337`, `packages/contracts/src/settings.ts:310-405`

## A01-PROV-003 — Per-session autonomy and permission modes

**Status:** Partial / different

**Dux behavior:** Each session persists an Attended/Orchestrator/Worker context mode, YOLO permission setting, watch-rule arm state, auto-clear, and an AMQ envelope-verification override; corrupt settings fail closed.

**T3 Code behavior:** The orchestration contract offers approval-required, auto-accept-edits, auto-review, and full-access runtime modes plus default/plan interaction. Its declared default is full-access, and provider adapters translate the modes to native sandbox and approval semantics. Watch/AMQ/auto-clear settings have no corresponding session fields.

**Impact:** Core approval and sandbox control is present and more protocol-aware, but Dux's broader per-session automation/security bundle and fail-closed default posture are only partially represented.

**Evidence:**

- Dux: `README.md:115-117`
- T3 Code: `packages/contracts/src/orchestration.ts:119-129`, `apps/server/src/provider/Layers/CodexSessionRuntime.ts:265-315`

## A01-PROV-004: Per-session custom system prompt

**Status:** Partial / different

**Dux behavior:** A session persists its own multiline system prompt. Dux exports the effective value only to that session's PTY, and the Claude wrapper passes it verbatim through `--append-system-prompt`; changing it warns that the provider must respawn.

**T3 Code behavior:** A Claude provider instance has global launch arguments, and the adapter passes those arguments alongside the fixed `claude_code` system-prompt preset. Thread creation and provider-session contracts do not contain a per-thread custom system-prompt field.

**Impact:** A T3 Code user can change an entire Claude provider instance's launch configuration, but cannot attach a durable instruction to one thread without creating or reconfiguring an instance.

**Evidence:**

- Dux: `src/model.rs:520-532`, `src/model.rs:608-621`, `dux-amq/wrappers/claude-amq:299-314`
- T3 Code: `packages/contracts/src/settings.ts:364-405`, `packages/contracts/src/orchestration.ts:1125-1137`, `apps/server/src/provider/Layers/ClaudeAdapter.ts:4154-4178`

## A01-PROV-005: Provider-version floor before launch

**Status:** Partial / different

**Dux behavior:** The bundled Claude, Codex, and Gemini wrappers parse the installed CLI version before normal startup and refuse an unparseable or below-minimum version with an upgrade instruction.

**T3 Code behavior:** Provider probes and maintenance advisories report versions and available updates. OpenCode enforces a minimum version, while Claude gates individual newer models by version; no comparable whole-provider minimum launch floor is applied consistently across the built-in providers.

**Impact:** T3 Code has better ongoing update visibility, but an old Claude or Codex installation may remain launchable where the Dux wrapper fails closed on its declared compatibility floor.

**Evidence:**

- Dux: `dux-amq/wrappers/claude-amq:33-53`, `dux-amq/wrappers/codex-amq:20-40`, `dux-amq/wrappers/gemini-amq:20-40`
- T3 Code: `apps/server/src/provider/Layers/OpenCodeProvider.ts:350-389`, `apps/server/src/provider/Layers/ClaudeProvider.ts:334-367`, `apps/server/src/provider/providerMaintenance.ts:388-430`
