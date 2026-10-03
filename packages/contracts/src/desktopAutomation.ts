import { Schema } from "effect";

export const DesktopAction = Schema.Union([
  Schema.Struct({ kind: Schema.Literal("observe"), app: Schema.String }),
  Schema.Struct({ kind: Schema.Literal("press"), app: Schema.String, element: Schema.String }),
  Schema.Struct({
    kind: Schema.Literal("set-value"),
    app: Schema.String,
    element: Schema.String,
    value: Schema.String,
  }),
  Schema.Struct({ kind: Schema.Literal("script"), app: Schema.String, source: Schema.String }),
]);
export type DesktopAction = typeof DesktopAction.Type;
export const DesktopHost = Schema.Struct({
  hostId: Schema.String,
  displayName: Schema.String,
  environmentId: Schema.String,
  generation: Schema.String,
  operations: Schema.Array(Schema.Literals(["observe", "press", "set-value", "script"])),
});
export type DesktopHost = typeof DesktopHost.Type;
export const DesktopLease = Schema.Struct({
  leaseId: Schema.String,
  hostId: Schema.String,
  generation: Schema.String,
  environmentId: Schema.String,
  threadId: Schema.String,
  app: Schema.String,
  expiresAt: Schema.Number,
});
export type DesktopLease = typeof DesktopLease.Type;
export const DesktopLeaseInput = Schema.Struct({
  hostId: Schema.String,
  threadId: Schema.String,
  app: Schema.String,
  approvalId: Schema.String,
});
export const DesktopInvokeInput = Schema.Struct({
  threadId: Schema.String,
  leaseId: Schema.String,
  action: DesktopAction,
});
export const DesktopLeaseIdInput = Schema.Struct({ leaseId: Schema.String });
export const DesktopHostAuthorizeInput = Schema.Struct({
  hostId: Schema.String,
  generation: Schema.String,
  requestId: Schema.String,
});
export const DesktopHostDisconnectInput = Schema.Struct({
  hostId: Schema.String,
  generation: Schema.String,
});
export const DesktopHostRequest = Schema.Struct({
  requestId: Schema.String,
  leaseId: Schema.String,
  action: DesktopAction,
});
export const DesktopHostResponseInput = Schema.Struct({
  hostId: Schema.String,
  generation: Schema.String,
  requestId: Schema.String,
  result: Schema.Unknown,
  failed: Schema.optional(Schema.Boolean),
});
export const DesktopResult = Schema.Unknown;
export const DesktopAutomationError = Schema.Struct({
  _tag: Schema.Literal("DesktopAutomationError"),
  reason: Schema.Literals([
    "host-unavailable",
    "consent-required",
    "stale-lease",
    "scope-denied",
    "unsupported-action",
    "disconnected",
    "execution",
    "timeout",
  ]),
});
export const DesktopLocalConsentInput = Schema.Struct({
  environmentId: Schema.String,
  apps: Schema.Array(Schema.String),
  scriptsEnabled: Schema.Boolean,
});
export const DesktopLocalRegistration = Schema.Struct({
  ...DesktopHost.fields,
  expiresAt: Schema.Number,
});
export const DesktopLocalExecuteInput = Schema.Struct({
  generation: Schema.String,
  request: DesktopHostRequest,
});
