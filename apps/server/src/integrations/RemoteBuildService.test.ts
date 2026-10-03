import { expect, it } from "@effect/vitest";
import { Effect, Layer } from "effect";
import * as Build from "./RemoteBuildService.ts";
import * as Snapshots from "./SourceSnapshots.ts";
import * as Http from "./IntegrationHttp.ts";
import * as Approvals from "./WorkflowApprovals.ts";
import { testPersistence, testSecrets } from "./integrationTestSupport.ts";

it.effect(
  "sends only documented Jcode v1 messages after exact snapshot approval and rejects billing retries",
  () => {
    const config = {
      enabled: true,
      protocol: "jcode-compile-v1" as const,
      baseUrl: "https://build.example.test/v1",
      apiKeySecretRef: "integration-build-fixture",
    };
    const snapshot = {
      snapshotId: "snapshot",
      root: "/fixture",
      digest: "fixture-digest",
      files: [{ path: "src/main.ts", content_base64: Buffer.from("fixture").toString("base64") }],
      excluded: [".env"],
      bytes: 7,
    };
    let posts = 0;
    const deps = Layer.mergeAll(
      testPersistence(),
      testSecrets({ [config.apiKeySecretRef]: "fixture-key" }).layer,
      Layer.succeed(Build.RemoteBuildConfiguration, config),
      Layer.succeed(Snapshots.SourceSnapshots, { prepare: () => Effect.succeed(snapshot) }),
      Layer.succeed(Http.IntegrationHttp, {
        request: (request) =>
          Effect.sync(() => {
            expect(request.headers?.authorization).toBe("Bearer fixture-key");
            if (request.url.endsWith("/me"))
              return { status: "active", capabilities: { remote_compile: true } };
            if (request.url.endsWith("/compute/usage"))
              return { compute: { unit: "microcredits", available_microcredits: 1000 } };
            expect(request.url).toBe(`${config.baseUrl}/compile`);
            expect(request.method).toBe("POST");
            expect(JSON.parse(request.body!)).toEqual({
              request_id: "request",
              command: "cargo check",
              timeout_seconds: 30,
              files: snapshot.files,
            });
            expect(request.timeoutMs).toBe(720_000);
            posts++;
            return { exit_code: 0, stdout: "fixture-log", stderr: "", cleanup_confirmed: true };
          }),
      }),
    );
    return Effect.gen(function* () {
      const build = yield* Build.RemoteBuildService;
      const approvals = yield* Approvals.WorkflowApprovals;
      expect((yield* build.status()).artifacts).toBe(false);
      const prepared = yield* build.prepare("/fixture");
      expect(prepared.paths).toEqual(["src/main.ts"]);
      expect(posts).toBe(0);
      const input = {
        snapshotId: "snapshot",
        requestId: "request",
        command: "cargo check",
        timeoutSeconds: 30,
        unknownCostAcknowledged: true as const,
      };
      expect(
        (yield* Effect.flip(build.submit({ ...input, approvalId: "agent-forged" }))).reason,
      ).toBe("approval-required");
      const grant = () =>
        approvals.grant({
          humanSessionId: "human",
          operation: "build.submit",
          review: JSON.stringify({
            backend: config.baseUrl,
            protocol: config.protocol,
            digest: snapshot.digest,
            command: input.command,
            timeoutSeconds: input.timeoutSeconds,
            price: "backend-metered-no-client-ceiling",
            unknownCostAcknowledged: input.unknownCostAcknowledged,
          }),
        });
      expect((yield* build.submit({ ...input, approvalId: yield* grant() })).stdout).toBe(
        "fixture-log",
      );
      expect(
        (yield* Effect.flip(build.submit({ ...input, approvalId: yield* grant() }))).reason,
      ).toBe("already-attempted");
      expect(posts).toBe(1);
      yield* build.discard("snapshot");
      expect((yield* Effect.flip(build.submit({ ...input, approvalId: "anything" }))).reason).toBe(
        "snapshot-missing",
      );
    }).pipe(Effect.provide(Build.layer.pipe(Layer.provideMerge(deps))));
  },
);
