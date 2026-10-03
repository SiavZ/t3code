import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { Deferred, Effect, Layer, Stream } from "effect";
import { EnvironmentId, ThreadId, PreviewTabId, ProviderInstanceId } from "@t3tools/contracts";
import * as Preview from "../mcp/PreviewAutomationBroker.ts";
import * as Browser from "./BrowserTaskService.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import { testPersistence, testSecrets } from "./integrationTestSupport.ts";

it.effect(
  "executes bounded whole-task iterations through actual preview broker messages and independently proves completion",
  () => {
    const scope = {
      environmentId: EnvironmentId.make("env"),
      threadId: ThreadId.make("thread"),
      providerSessionId: "fixture-provider",
      providerInstanceId: ProviderInstanceId.make("fixture"),
      capabilities: new Set(["preview"] as const),
      issuedAt: 1,
    };
    const tabId = PreviewTabId.make("fixture-tab");
    let navigated = false;
    let decisions = 0;
    const dependencies = Layer.mergeAll(
      testPersistence(),
      testSecrets({ "integration-browser-fixture": "fixture-key" }).layer,
      Layer.succeed(Browser.BrowserExecutorConfiguration, {
        enabled: true,
        model: "fixture-configured-model",
        apiKeySecretRef: "integration-browser-fixture",
      }),
      Preview.layer.pipe(Layer.provide(NodeServices.layer)),
      Layer.succeed(Http.IntegrationHttp, {
        request: (request) =>
          Effect.sync(() => {
            expect(request.url).toBe("https://api.openai.com/v1/chat/completions");
            const body = JSON.parse(request.body!);
            const observation = JSON.parse(body.messages[1].content).untrustedObservation;
            expect(observation).not.toHaveProperty("screenshot");
            decisions++;
            return {
              choices: [
                {
                  message: {
                    content: JSON.stringify(
                      navigated
                        ? { kind: "complete", evidence: "Done" }
                        : { kind: "candidate", index: 0 },
                    ),
                  },
                },
              ],
            };
          }),
      }),
    );
    return Effect.scoped(
      Effect.gen(function* () {
        const broker = yield* Preview.PreviewAutomationBroker;
        const browser = yield* Browser.BrowserTaskService;
        const approvals = yield* Approvals.WorkflowApprovals;
        const connected = yield* Deferred.make<void>();
        const stream = yield* broker.connect({
          clientId: "fixture-host",
          environmentId: scope.environmentId,
        });
        yield* Stream.runForEach(stream, (event) => {
          if (event.type === "connected") return Deferred.succeed(connected, undefined);
          const request = event.request;
          if (request.operation === "navigate") navigated = true;
          return broker.respond({
            clientId: "fixture-host",
            connectionId: event.connectionId,
            requestId: request.requestId,
            ok: true,
            result:
              request.operation === "open"
                ? { available: true, tabId }
                : request.operation === "snapshot"
                  ? {
                      url: navigated
                        ? "https://fixture.example.test/done"
                        : "https://fixture.example.test/",
                      title: "Fixture",
                      loading: false,
                      visibleText: navigated ? "Done" : "Start",
                      interactiveElements: [],
                      accessibilityTree: null,
                      consoleEntries: [],
                      networkEntries: [],
                      actionTimeline: [],
                      screenshot: {
                        mimeType: "image/png",
                        data: "fixture-not-transmitted",
                        width: 1,
                        height: 1,
                      },
                    }
                  : { url: "https://fixture.example.test/done" },
          });
        }).pipe(Effect.forkScoped);
        yield* Deferred.await(connected);
        yield* broker.invoke({ scope, operation: "open", input: {} });
        const input = {
          taskId: "task",
          tabId,
          goal: "Reach Done",
          completion: { kind: "visible-text" as const, text: "Done" },
          context: "fixture",
          candidates: [
            { operation: "navigate" as const, input: { url: "https://fixture.example.test/done" } },
          ],
          maxSteps: 3,
        };
        const review = JSON.stringify({
          environmentId: scope.environmentId,
          threadId: scope.threadId,
          ...input,
          approvalId: undefined,
          model: "fixture-configured-model",
          cost: "unknown-model-api-cost",
        });
        const approvalId = yield* approvals.grant({
          humanSessionId: "human",
          operation: "browser.run",
          review,
        });
        const result = yield* browser.run(scope, { ...input, approvalId });
        expect(result).toEqual({ taskId: "task", state: "completed", steps: 1, evidence: "Done" });
        expect(decisions).toBe(2);
        expect((yield* browser.get(scope, "task")).state).toBe("completed");
        expect(
          (yield* Effect.flip(
            browser.get({ ...scope, threadId: ThreadId.make("foreign") }, "task"),
          )).reason,
        ).toBe("not-found");
      }),
    ).pipe(Effect.provide(Browser.layer.pipe(Layer.provideMerge(dependencies))));
  },
);
