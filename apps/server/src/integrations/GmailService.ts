import { Context, Effect, Layer, Option, Schema } from "effect";
import { makeCurrentCheck } from "./IntegrationConfigurationGuard.ts";
import * as Semaphore from "effect/Semaphore";
import * as NodeCrypto from "node:crypto";
const { randomUUID } = NodeCrypto;
import { isIntegrationSecretRefFor } from "./IntegrationSecrets.ts";
import * as Secrets from "../auth/ServerSecretStore.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";

export class GmailError extends Schema.TaggedError<GmailError>()("GmailError", {
  reason: Schema.Literals([
    "unconfigured",
    "authentication-required",
    "approval-required",
    "invalid-response",
    "invalid-input",
    "state-mismatch",
    "transport",
    "unknown-outcome",
  ]),
}) {}
export class GmailConfiguration extends Context.Service<
  GmailConfiguration,
  {
    readonly enabled: boolean;
    readonly accountId: string;
    readonly clientId: string;
    readonly clientSecretRef: string;
    readonly tokenSecretRef: string;
    readonly redirectUri: string;
    readonly scopes: ReadonlyArray<string>;
  }
>()("t3/integrations/GmailConfiguration") {}
const Tokens = Schema.Struct({
  access_token: Schema.String,
  refresh_token: Schema.optional(Schema.String),
  expires_in: Schema.Number,
  scope: Schema.optional(Schema.String),
});
const StoredTokens = Schema.Struct({
  accountId: Schema.String,
  access_token: Schema.String,
  refresh_token: Schema.optional(Schema.String),
  expires_at: Schema.Number,
  scope: Schema.String,
});
const Message = Schema.Struct({
  id: Schema.String,
  threadId: Schema.optional(Schema.String),
  snippet: Schema.optional(Schema.String),
  raw: Schema.optional(Schema.String),
});
const Draft = Schema.Struct({ id: Schema.String, message: Message });
export interface GmailMutation {
  readonly operation: "draft" | "send-draft" | "trash" | "modify-labels";
  readonly targetId: string;
  readonly raw?: string | undefined;
  readonly addLabelIds?: ReadonlyArray<string> | undefined;
  readonly removeLabelIds?: ReadonlyArray<string> | undefined;
  readonly approvalId: string;
}
export class GmailService extends Context.Service<
  GmailService,
  {
    readonly status: () => Effect.Effect<
      {
        readonly accountId: string;
        readonly state: "unconfigured" | "authentication-required" | "configured";
      },
      GmailError
    >;
    readonly beginConnect: (
      humanSessionId: string,
      approvalId: string,
    ) => Effect.Effect<{ readonly url: string; readonly state: string }, GmailError>;
    readonly completeConnect: (
      humanSessionId: string,
      state: string,
      code: string,
    ) => Effect.Effect<void, GmailError>;
    readonly disconnect: () => Effect.Effect<void, GmailError>;
    readonly search: (query: string, pageToken?: string) => Effect.Effect<unknown, GmailError>;
    readonly read: (messageId: string) => Effect.Effect<typeof Message.Type, GmailError>;
    readonly attachment: (
      messageId: string,
      attachmentId: string,
    ) => Effect.Effect<{ readonly size: number; readonly data: string }, GmailError>;
    readonly labels: () => Effect.Effect<unknown, GmailError>;
    readonly threads: (query: string) => Effect.Effect<unknown, GmailError>;
    readonly reviewMutation: (
      input: Omit<GmailMutation, "approvalId">,
    ) => Effect.Effect<{ readonly operation: string; readonly review: string }, GmailError>;
    readonly mutate: (input: GmailMutation) => Effect.Effect<unknown, GmailError>;
  }
>()("t3/integrations/GmailService") {}

/** Protocol: https://developers.google.com/workspace/gmail/api/reference/rest */
const make = Effect.gen(function* () {
  const config = yield* GmailConfiguration;
  const current = yield* makeCurrentCheck("gmail");
  const secrets = yield* Secrets.ServerSecretStore;
  const http = yield* Http.IntegrationHttp;
  const approvals = yield* Approvals.WorkflowApprovals;
  const credentialMutex = yield* Semaphore.make(1);
  const states = new Map<string, { session: string; expiresAt: number }>();
  const error = (reason: GmailError["reason"]) => new GmailError({ reason });
  const configured = () =>
    current().pipe(
      Effect.flatMap((unchanged) =>
        unchanged &&
        config.enabled &&
        config.clientId &&
        config.redirectUri &&
        isIntegrationSecretRefFor(config.clientSecretRef, "gmail-client") &&
        isIntegrationSecretRefFor(config.tokenSecretRef, "gmail-token")
          ? Effect.void
          : Effect.fail(error("unconfigured")),
      ),
    );
  const secret = (ref: string) =>
    secrets.get(ref).pipe(
      Effect.mapError(() => error("authentication-required")),
      Effect.flatMap((value) =>
        Option.isSome(value)
          ? Effect.succeed(Buffer.from(value.value).toString("utf8"))
          : Effect.fail(error("authentication-required")),
      ),
    );
  const decodeTokens = (raw: unknown) =>
    Schema.decodeUnknownEffect(Tokens)(raw).pipe(Effect.mapError(() => error("invalid-response")));
  const persistTokens = (tokens: typeof Tokens.Type, previousRefresh?: string) =>
    configured().pipe(
      Effect.andThen(
        secrets
          .set(
            config.tokenSecretRef,
            Buffer.from(
              JSON.stringify({
                accountId: config.accountId,
                access_token: tokens.access_token,
                ...(tokens.refresh_token || previousRefresh
                  ? { refresh_token: tokens.refresh_token ?? previousRefresh }
                  : {}),
                expires_at: Date.now() + tokens.expires_in * 1000,
                scope: tokens.scope ?? config.scopes.join(" "),
              }),
            ),
          )
          .pipe(Effect.mapError(() => error("authentication-required"))),
      ),
    );
  const exchange = (params: Readonly<Record<string, string>>) =>
    Effect.gen(function* () {
      yield* configured();
      const clientSecret = yield* secret(config.clientSecretRef);
      const raw = yield* http
        .request({
          url: "https://oauth2.googleapis.com/token",
          method: "POST",
          headers: { "content-type": "application/x-www-form-urlencoded" },
          body: new URLSearchParams({
            ...params,
            client_id: config.clientId,
            client_secret: clientSecret,
          }).toString(),
        })
        .pipe(Effect.mapError(() => error("authentication-required")));
      return yield* decodeTokens(raw);
    });
  const token = () =>
    Effect.gen(function* () {
      yield* configured();
      const json = yield* secret(config.tokenSecretRef);
      const tokens = yield* Schema.decodeUnknownEffect(StoredTokens)(
        yield* Effect.try({
          try: () => JSON.parse(json) as unknown,
          catch: () => error("authentication-required"),
        }),
      ).pipe(Effect.mapError(() => error("authentication-required")));
      if (tokens.accountId !== config.accountId)
        return yield* Effect.fail(error("authentication-required"));
      if (tokens.expires_at > Date.now() + 30_000) return tokens.access_token;
      if (!tokens.refresh_token) return yield* Effect.fail(error("authentication-required"));
      const refreshed = yield* exchange({
        grant_type: "refresh_token",
        refresh_token: tokens.refresh_token,
      });
      yield* persistTokens(refreshed, tokens.refresh_token);
      return refreshed.access_token;
    }).pipe(credentialMutex.withPermits(1));
  const request = (path: string, method: Http.HttpRequest["method"] = "GET", body?: unknown) =>
    Effect.gen(function* () {
      const access = yield* token();
      yield* configured();
      return yield* http
        .request({
          url: `https://gmail.googleapis.com/gmail/v1/users/me/${path}`,
          method,
          headers: { authorization: `Bearer ${access}`, "content-type": "application/json" },
          ...(body === undefined ? {} : { body: JSON.stringify(body) }),
        })
        .pipe(Effect.mapError(() => error(method === "GET" ? "transport" : "unknown-outcome")));
    });
  const reviewMutation = (input: Omit<GmailMutation, "approvalId">) =>
    Effect.gen(function* () {
      yield* configured();
      if (input.operation === "draft" && (!input.raw || !/^[A-Za-z0-9_-]+={0,2}$/.test(input.raw)))
        return yield* Effect.fail(error("invalid-input"));
      if (!input.targetId || (input.raw?.length ?? 0) > 10_000_000)
        return yield* Effect.fail(error("invalid-input"));
      const draft =
        input.operation === "send-draft"
          ? yield* request(`drafts/${encodeURIComponent(input.targetId)}?format=raw`).pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(Draft)),
              Effect.mapError(() => error("invalid-response")),
            )
          : undefined;
      if (draft && !draft.message.raw) return yield* Effect.fail(error("invalid-response"));
      return {
        operation: `gmail.${input.operation}`,
        review: JSON.stringify({
          accountId: config.accountId,
          ...input,
          ...(draft ? { draft } : {}),
        }),
      };
    });
  return GmailService.of({
    reviewMutation,
    status: () =>
      Effect.gen(function* () {
        if (
          !(yield* current()) ||
          !config.enabled ||
          !isIntegrationSecretRefFor(config.tokenSecretRef, "gmail-token")
        )
          return { accountId: config.accountId, state: "unconfigured" as const };
        const tokens = yield* secrets
          .get(config.tokenSecretRef)
          .pipe(Effect.mapError(() => error("authentication-required")));
        return {
          accountId: config.accountId,
          state: Option.isSome(tokens)
            ? ("configured" as const)
            : ("authentication-required" as const),
        };
      }),
    beginConnect: (session, approvalId) =>
      Effect.gen(function* () {
        yield* configured();
        yield* approvals
          .consume(
            approvalId,
            "gmail.connect",
            JSON.stringify({
              accountId: config.accountId,
              clientId: config.clientId,
              redirectUri: config.redirectUri,
              scopes: config.scopes,
            }),
          )
          .pipe(Effect.mapError(() => error("approval-required")));
        for (const [key, pending] of states) if (pending.expiresAt < Date.now()) states.delete(key);
        if (states.size >= 100) return yield* Effect.fail(error("invalid-input"));
        const state = randomUUID();
        states.set(state, { session, expiresAt: Date.now() + 300_000 });
        const params = new URLSearchParams({
          client_id: config.clientId,
          redirect_uri: config.redirectUri,
          response_type: "code",
          scope: config.scopes.join(" "),
          access_type: "offline",
          state,
        });
        return { state, url: `https://accounts.google.com/o/oauth2/v2/auth?${params}` };
      }),
    completeConnect: (session, state, code) =>
      Effect.gen(function* () {
        yield* configured();
        const pending = states.get(state);
        if (!pending || pending.session !== session || pending.expiresAt < Date.now())
          return yield* Effect.fail(error("state-mismatch"));
        states.delete(state);
        const tokens = yield* exchange({
          grant_type: "authorization_code",
          code,
          redirect_uri: config.redirectUri,
        });
        const profile = yield* http
          .request({
            url: "https://gmail.googleapis.com/gmail/v1/users/me/profile",
            method: "GET",
            headers: { authorization: `Bearer ${tokens.access_token}` },
          })
          .pipe(
            Effect.flatMap(
              Schema.decodeUnknownEffect(Schema.Struct({ emailAddress: Schema.String })),
            ),
            Effect.mapError(() => error("authentication-required")),
          );
        if (profile.emailAddress.toLowerCase() !== config.accountId.toLowerCase())
          return yield* Effect.fail(error("authentication-required"));
        yield* persistTokens(tokens);
      }).pipe(credentialMutex.withPermits(1)),
    disconnect: () =>
      Effect.gen(function* () {
        states.clear();
        if (!isIntegrationSecretRefFor(config.tokenSecretRef, "gmail-token"))
          return yield* Effect.fail(error("unconfigured"));
        yield* secrets
          .remove(config.tokenSecretRef)
          .pipe(Effect.mapError(() => error("authentication-required")));
      }).pipe(credentialMutex.withPermits(1)),
    search: (q, pageToken) =>
      request(
        `messages?${new URLSearchParams({ q, maxResults: "50", ...(pageToken ? { pageToken } : {}) })}`,
      ),
    read: (id) =>
      request(`messages/${encodeURIComponent(id)}?format=raw`).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Message)),
        Effect.mapError(() => error("invalid-response")),
      ),
    attachment: (messageId, attachmentId) =>
      request(
        `messages/${encodeURIComponent(messageId)}/attachments/${encodeURIComponent(attachmentId)}`,
      ).pipe(
        Effect.flatMap(
          Schema.decodeUnknownEffect(Schema.Struct({ size: Schema.Number, data: Schema.String })),
        ),
        Effect.flatMap((attachment) =>
          attachment.size >= 0 &&
          attachment.size <= 1_000_000 &&
          /^[A-Za-z0-9_-]*={0,2}$/.test(attachment.data) &&
          Buffer.from(attachment.data, "base64url").length === attachment.size
            ? Effect.succeed(attachment)
            : Effect.fail(error("invalid-response")),
        ),
        Effect.mapError(() => error("invalid-response")),
      ),
    labels: () => request("labels"),
    threads: (q) => request(`threads?${new URLSearchParams({ q, maxResults: "50" })}`),
    mutate: (input) =>
      Effect.gen(function* () {
        const { approvalId, ...reviewInput } = input;
        const review = yield* reviewMutation(reviewInput);
        yield* approvals
          .consume(approvalId, review.operation, review.review)
          .pipe(Effect.mapError(() => error("approval-required")));
        switch (input.operation) {
          case "draft":
            return yield* request("drafts", "POST", { message: { raw: input.raw } });
          case "send-draft":
            return yield* request("drafts/send", "POST", { id: input.targetId });
          case "trash":
            return yield* request(
              `messages/${encodeURIComponent(input.targetId)}/trash`,
              "POST",
              {},
            );
          case "modify-labels":
            return yield* request(`messages/${encodeURIComponent(input.targetId)}/modify`, "POST", {
              addLabelIds: input.addLabelIds ?? [],
              removeLabelIds: input.removeLabelIds ?? [],
            });
        }
      }),
  });
});
export const layer = Layer.effect(GmailService, make);
