import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Gmail from "./GmailService.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import { testPersistence, testSecrets } from "./integrationTestSupport.ts";

const config = {
  enabled: true,
  accountId: "fixture@example.test",
  clientId: "fixture-client",
  clientSecretRef: "integration-gmail-client-fixture",
  tokenSecretRef: "integration-gmail-token-fixture",
  redirectUri: "https://fixture.example.test/oauth",
  scopes: ["https://www.googleapis.com/auth/gmail.modify"],
};
let draftReads = 0;
describe("Gmail HTTP workflow", () => {
  it.effect(
    "binds OAuth to the human session and verified account then requires fresh mutation approval",
    () =>
      Effect.gen(function* () {
        const gmail = yield* Gmail.GmailService;
        const approvals = yield* Approvals.WorkflowApprovals;
        const grant = yield* approvals.grant({
          humanSessionId: "human",
          operation: "gmail.connect",
          review: JSON.stringify({
            accountId: config.accountId,
            clientId: config.clientId,
            redirectUri: config.redirectUri,
            scopes: config.scopes,
          }),
        });
        const connection = yield* gmail.beginConnect("human", grant);
        expect(new URL(connection.url).hostname).toBe("accounts.google.com");
        expect(
          (yield* Effect.flip(gmail.completeConnect("intruder", connection.state, "fixture-code")))
            .reason,
        ).toBe("state-mismatch");
        yield* gmail.completeConnect("human", connection.state, "fixture-code");
        expect((yield* gmail.status()).state).toBe("configured");
        expect((yield* gmail.read("message-1")).id).toBe("message-1");
        expect(yield* gmail.attachment("message-1", "attachment-1")).toEqual({
          size: 7,
          data: "Zml4dHVyZQ",
        });
        const send = { operation: "send-draft" as const, targetId: "draft-1" };
        const preview = yield* gmail.reviewMutation(send);
        const sendApproval = yield* approvals.grant({ humanSessionId: "human", ...preview });
        expect(
          (yield* Effect.flip(gmail.mutate({ ...send, approvalId: sendApproval }))).reason,
        ).toBe("approval-required");
        const input = { operation: "trash" as const, targetId: "message-1" };
        expect((yield* Effect.flip(gmail.mutate({ ...input, approvalId: "forged" }))).reason).toBe(
          "approval-required",
        );
        const approvalId = yield* approvals.grant({
          humanSessionId: "human",
          operation: "gmail.trash",
          review: JSON.stringify({ accountId: config.accountId, ...input }),
        });
        yield* gmail.mutate({ ...input, approvalId });
        expect((yield* Effect.flip(gmail.mutate({ ...input, approvalId }))).reason).toBe(
          "approval-required",
        );
        yield* gmail.disconnect();
        expect((yield* gmail.status()).state).toBe("authentication-required");
      }).pipe(
        Effect.provide(
          Gmail.layer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                testPersistence(),
                testSecrets({ [config.clientSecretRef]: "fixture-secret" }).layer,
                Layer.succeed(Gmail.GmailConfiguration, config),
                Layer.succeed(Http.IntegrationHttp, {
                  request: (request) =>
                    Effect.sync(() => {
                      if (request.url === "https://oauth2.googleapis.com/token") {
                        expect(request.method).toBe("POST");
                        expect(new URLSearchParams(request.body).get("code")).toBe("fixture-code");
                        return {
                          access_token: "fixture-access",
                          refresh_token: "fixture-refresh",
                          expires_in: 3600,
                        };
                      }
                      expect(request.headers?.authorization).toBe("Bearer fixture-access");
                      if (request.url.endsWith("/profile"))
                        return { emailAddress: config.accountId };
                      if (request.url.endsWith("/messages/message-1?format=raw"))
                        return { id: "message-1", raw: "Zml4dHVyZQ" };
                      if (request.url.endsWith("/messages/message-1/attachments/attachment-1"))
                        return { size: 7, data: "Zml4dHVyZQ" };
                      if (request.url.endsWith("/drafts/draft-1?format=raw"))
                        return {
                          id: "draft-1",
                          message: {
                            id: "message-1",
                            raw: ++draftReads === 1 ? "Zml4dHVyZQ" : "Y2hhbmdlZA",
                          },
                        };
                      expect(request.url).toBe(
                        "https://gmail.googleapis.com/gmail/v1/users/me/messages/message-1/trash",
                      );
                      expect(request.method).toBe("POST");
                      expect(JSON.parse(request.body!)).toEqual({});
                      return { id: "message-1", labelIds: ["TRASH"] };
                    }),
                }),
              ),
            ),
          ),
        ),
      ),
  );
});
