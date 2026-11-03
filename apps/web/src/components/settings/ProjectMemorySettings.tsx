import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentId, MemoryEntry, ProjectId } from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import { useState } from "react";

import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { toastManager } from "../ui/toast";
import { ScopedSwitch } from "./ScopedSwitch";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsRow, SettingsSection } from "./settingsLayout";
import { searchableSetting } from "./settingsSearch";
import { useScopedSettings, useUpdateScopedSettings } from "./useScopedSettings";

const createdAtFormatter = new Intl.DateTimeFormat(undefined, {
  dateStyle: "medium",
  timeStyle: "short",
});

function reportFailure(title: string, result: AtomCommandResult<unknown, unknown>) {
  if (result._tag !== "Failure" || isAtomCommandInterrupted(result)) return;
  const error = squashAtomCommandFailure(result);
  toastManager.add({
    type: "error",
    title,
    description: error instanceof Error ? error.message : "Project memory is unavailable.",
  });
}

export function MemorySettings() {
  const { targets, scope } = useSettingsScope();
  const settings = useScopedSettings();
  const updateSettings = useUpdateScopedSettings();
  const target = targets.length === 1 ? targets[0] : undefined;
  const projectScope = scope.kind === "project" || scope.kind === "checkout";
  return (
    <SettingsSection title="Project memory">
      <SettingsRow
        serverScoped
        settingKeys={["enableAgentMemoryAccess"]}
        {...searchableSetting("agent-memory-access")}
        description="Let agents remember and recall project notes across threads and providers. Applies when the agent session next starts. Projects can override it."
        control={
          <ScopedSwitch
            settingKeys={["enableAgentMemoryAccess"]}
            aria-label="Agent memory"
            checked={settings.enableAgentMemoryAccess}
            onCheckedChange={(enabled) => updateSettings({ enableAgentMemoryAccess: enabled })}
          />
        }
      />
      {projectScope && target?.projectId ? (
        <ProjectMemorySettings
          key={`${target.environmentId}:${target.projectId}`}
          environmentId={target.environmentId}
          projectId={target.projectId}
        />
      ) : (
        <p className="text-sm text-muted-foreground">
          Choose one project and environment to review stored memory.
        </p>
      )}
    </SettingsSection>
  );
}

function ProjectMemorySettings(props: {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId;
}) {
  const [entries, setEntries] = useState<ReadonlyArray<MemoryEntry> | null>(null);
  const [truncated, setTruncated] = useState(false);
  const [loading, setLoading] = useState(false);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const canRead = useAtomValue(serverEnvironment.listMemory.permissionAtom(props.environmentId));
  const canDelete = useAtomValue(
    serverEnvironment.deleteMemory.permissionAtom(props.environmentId),
  );
  const listMemory = useAtomCommand(serverEnvironment.listMemory, { reportFailure: false });
  const deleteMemory = useAtomCommand(serverEnvironment.deleteMemory, { reportFailure: false });

  const load = async () => {
    setLoading(true);
    const result = await listMemory({
      environmentId: props.environmentId,
      input: { projectId: props.projectId },
    });
    setLoading(false);
    if (result._tag === "Success") {
      setEntries(result.value.entries);
      setTruncated(result.value.truncated);
    } else reportFailure("Could not load project memory", result);
  };

  const remove = async (entry: MemoryEntry) => {
    setDeletingId(entry.id);
    const result = await deleteMemory({
      environmentId: props.environmentId,
      input: { projectId: props.projectId, id: entry.id },
    });
    setDeletingId(null);
    if (result._tag === "Success") {
      setEntries((current) => current?.filter((candidate) => candidate.id !== entry.id) ?? null);
    } else reportFailure("Could not delete the memory entry", result);
  };

  return (
    <>
      <SettingsRow
        title="Stored memory"
        description={
          entries === null
            ? "Review and delete what agents remembered for this project."
            : entries.length === 0
              ? "Agents have not remembered anything for this project."
              : `${entries.length}${truncated ? "+" : ""} entries, newest first.`
        }
        control={
          <Button
            type="button"
            size="sm"
            variant="outline"
            disabled={loading || deletingId !== null || !canRead}
            onClick={() => void load()}
          >
            {loading ? "Loading…" : entries === null ? "Show" : "Refresh"}
          </Button>
        }
      />
      {entries?.map((entry) => (
        <SettingsRow
          key={entry.id}
          title={<span className="whitespace-pre-wrap break-words">{entry.content}</span>}
          description={`${entry.category} · ${createdAtFormatter.format(new Date(entry.createdAt))}`}
          control={
            <Button
              type="button"
              size="sm"
              variant="destructive-outline"
              disabled={loading || deletingId !== null || !canDelete}
              aria-label="Delete memory entry"
              onClick={() => void remove(entry)}
            >
              {deletingId === entry.id ? "Deleting…" : "Delete"}
            </Button>
          }
        />
      ))}
    </>
  );
}
