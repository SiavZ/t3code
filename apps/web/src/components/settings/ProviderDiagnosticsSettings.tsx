import { parityOperations } from "@t3tools/client-runtime/operations/parity";
import { createEnvironmentCommand } from "@t3tools/client-runtime/state/runtime";
import {
  AuthAccessWriteScope,
  providerDoctorApprovalReview,
  type EnvironmentId,
  type ProviderDoctorInput,
  type ProviderDoctorResult,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import { useRef, useState } from "react";
import { connectionAtomRuntime } from "../../connection/runtime";
import { randomUUID } from "../../lib/utils";
import { useEnvironments } from "../../state/environments";
import { useEnvironmentSessionState } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import {
  AlertDialog,
  AlertDialogClose,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogPopup,
  AlertDialogTitle,
} from "../ui/alert-dialog";
import { SettingsRow, SettingsSection } from "./settingsLayout";
import {
  createProviderDiagnosticInput,
  ProviderDiagnosticRequestError,
  runConfirmedProviderDiagnostic,
} from "./providerDiagnostics.logic";

const runDiagnostic = createEnvironmentCommand(connectionAtomRuntime, {
  label: "provider-diagnostics:human-run",
  execute: (request: { input: ProviderDoctorInput; confirmedReview?: string }) =>
    request.confirmedReview !== undefined
      ? runConfirmedProviderDiagnostic(request.input, request.confirmedReview, {
          grant: parityOperations.integrations.approvalGrant,
          run: parityOperations.providerDoctor.runApproved,
        })
      : request.input.tier === "full"
        ? Effect.fail(new ProviderDiagnosticRequestError({ reason: "unconfirmed" }))
        : parityOperations.providerDoctor.run(request.input),
});

const cancelDiagnostic = createEnvironmentCommand(connectionAtomRuntime, {
  label: "provider-diagnostics:human-cancel",
  execute: (request: { runId: string }) => parityOperations.providerDoctor.cancel(request),
});

/** Manual diagnostics never change provider configuration or the current conversation. */
export function ProviderDiagnosticsSettings(props: {
  environmentId: EnvironmentId;
  instanceId: ProviderInstanceId;
  readOnly: boolean;
}) {
  const { environments } = useEnvironments();
  const session = useEnvironmentSessionState(props.environmentId);
  const connected = environments.some(
    (entry) =>
      entry.environmentId === props.environmentId && entry.connection.phase === "connected",
  );
  const admin =
    session.data?.authenticated === true &&
    session.data.scopes?.includes(AuthAccessWriteScope) === true;
  const run = useAtomCommand(runDiagnostic, { reportFailure: false });
  const cancel = useAtomCommand(cancelDiagnostic, { reportFailure: false });
  const [model, setModel] = useState("");
  const [pending, setPending] = useState(false);
  // Only an approved full run is cancellable: cancel is owned by the approving human session.
  const [cancellable, setCancellable] = useState<string | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const lock = useRef(false);
  const [review, setReview] = useState<{ input: ProviderDoctorInput; canonical: string } | null>(
    null,
  );
  const [result, setResult] = useState<ProviderDoctorResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const disabled = !connected || props.readOnly || pending;
  async function execute(input: ProviderDoctorInput, confirmedReview?: string) {
    if (lock.current || !connected || props.readOnly || (input.tier === "full" && !admin)) return;
    lock.current = true;
    setPending(true);
    setCancellable(confirmedReview !== undefined ? input.runId : null);
    setError(null);
    setResult(null);
    setReview(null);
    try {
      const outcome = await run({
        environmentId: props.environmentId,
        input: { input, ...(confirmedReview !== undefined ? { confirmedReview } : {}) },
      });
      if (outcome._tag === "Failure")
        setError(
          "Diagnostic failed, was cancelled, or approval expired. Start a new run and confirm again if required.",
        );
      else setResult(outcome.value);
    } finally {
      lock.current = false;
      setPending(false);
      setCancellable(null);
      setCancelling(false);
    }
  }
  async function stop() {
    if (cancellable === null || cancelling) return;
    setCancelling(true);
    const outcome = await cancel({
      environmentId: props.environmentId,
      input: { runId: cancellable },
    });
    if (outcome._tag === "Failure") {
      setCancelling(false);
      setError("Could not cancel the diagnostic. It may have already finished.");
    }
  }
  function start(tier: ProviderDoctorInput["tier"]) {
    if (disabled || (tier === "full" && !admin)) return;
    try {
      const input = createProviderDiagnosticInput(props.instanceId, model, tier, randomUUID());
      if (tier === "full")
        setReview({ input, canonical: providerDoctorApprovalReview(input).review });
      else void execute(input);
    } catch {
      setError("Enter a model of at most 200 characters before a full diagnostic.");
    }
  }
  return (
    <SettingsSection title="Provider diagnostics">
      <p className="text-sm text-muted-foreground">
        Checks are manual and run on the selected environment. They do not change provider
        configuration or your current conversation. Diagnostic results are not a native-tool parity
        guarantee.
      </p>
      <SettingsRow
        title="Offline checks"
        description="Inspect cached provider availability and configuration without inference."
        control={
          <Button size="sm" variant="outline" disabled={disabled} onClick={() => start("offline")}>
            Run offline checks
          </Button>
        }
      />
      <SettingsRow
        title="Catalog checks"
        description="Refresh this provider's catalog. May start a process or make network requests, but does not run charged inference."
        control={
          <Button size="sm" variant="outline" disabled={disabled} onClick={() => start("catalog")}>
            Check catalog
          </Button>
        }
      />
      <SettingsRow
        title="Diagnostic model"
        description="Exact model for a full diagnostic. This does not change your selected chat model."
        control={
          <Input
            size="sm"
            aria-label="Diagnostic model"
            maxLength={200}
            value={model}
            disabled={disabled}
            onChange={(event) => setModel(event.target.value)}
          />
        }
      />
      <SettingsRow
        title="Full diagnostic"
        description={
          admin
            ? "A disposable diagnostic turn may consume quota or incur billing. Review and approve the exact request before it starts."
            : "Full diagnostics require this environment's administrator access."
        }
        control={
          <Button
            size="sm"
            variant="outline"
            disabled={disabled || !admin || model.trim().length === 0}
            onClick={() => start("full")}
          >
            Review full diagnostic
          </Button>
        }
      />
      {pending && (
        <div className="flex items-center gap-2">
          <p role="status" className="text-sm text-muted-foreground">
            {cancelling ? "Cancelling diagnostic…" : "Running diagnostic…"}
          </p>
          {cancellable !== null && (
            <Button size="sm" variant="outline" disabled={cancelling} onClick={() => void stop()}>
              Cancel diagnostic
            </Button>
          )}
        </div>
      )}
      {!connected && (
        <p role="status" className="text-sm text-muted-foreground">
          Connect this environment to run diagnostics.
        </p>
      )}
      {error && (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      )}
      {result && (
        <div className="space-y-2">
          <p className="text-sm text-muted-foreground">
            {result.tier} · {result.checkedAt} · Potential cost: {result.potentialCost}
          </p>
          <ul>
            {result.stages.map((stage) => (
              <li key={stage.name} className="py-1 text-sm">
                <strong>
                  {stage.name}: {stage.status}
                </strong>
                <p className="text-muted-foreground">{stage.detail}</p>
              </li>
            ))}
          </ul>
        </div>
      )}
      <AlertDialog
        open={review !== null}
        onOpenChange={(open) => {
          if (!open) setReview(null);
        }}
      >
        <AlertDialogPopup>
          <AlertDialogHeader>
            <AlertDialogTitle>Approve a potentially charged diagnostic?</AlertDialogTitle>
            <AlertDialogDescription>
              This runs a disposable full diagnostic for the exact provider and model below. It may
              consume quota or incur billing. No arbitrary shell tools or native-tool parity are
              promised. Approval is bound to this run and authenticated human session, and is never
              reused for another run.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <div className="max-h-48 overflow-auto px-6 pb-4">
            <pre className="whitespace-pre-wrap break-all text-xs">{review?.canonical}</pre>
          </div>
          <AlertDialogFooter>
            <AlertDialogClose render={<Button variant="outline" />}>Cancel</AlertDialogClose>
            <Button
              disabled={disabled || !admin || review === null}
              onClick={() => {
                if (review) void execute(review.input, review.canonical);
              }}
            >
              Approve and run once
            </Button>
          </AlertDialogFooter>
        </AlertDialogPopup>
      </AlertDialog>
    </SettingsSection>
  );
}
