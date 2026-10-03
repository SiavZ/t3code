import { describe, expect, it } from "vite-plus/test";
import {
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  ThreadTurnStartCommand,
  type AgentDocument,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { documentPromptCommand } from "./prompts.ts";
const document: AgentDocument = {
  id: "doc",
  projectId: "project",
  ownerThreadId: "owner",
  revision: 1,
  title: "Actions",
  placement: "end",
  lifetime: "persistent",
  closed: false,
  body: { kind: "applet", view: { type: "text", text: "prompt" }, state: {} },
};
const owner = {
  id: ThreadId.make("owner"),
  projectId: ProjectId.make("project"),
  modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
  runtimeMode: "full-access" as const,
  interactionMode: "default" as const,
};
const base = {
  document,
  owner,
  messageId: "message",
  newThreadId: "new-thread",
  createdAt: "2026-10-03T05:00:00.000Z",
};
describe("independent document prompts", () => {
  it("targets the document owner and preserves unrelated draft and source snapshots", () => {
    const draft = {
      threadId: "selected-other-thread",
      text: "unsent draft",
      attachments: ["draft-image"],
    };
    const before = structuredClone({ draft, document, owner });
    const command = documentPromptCommand({
      ...base,
      action: { action: "host.send_prompt", prompt: "Send this independent prompt" },
    });
    expect(command.threadId).toBe("owner");
    expect(command.message).toEqual({
      messageId: "message",
      role: "user",
      text: "Send this independent prompt",
      attachments: [],
    });
    expect(command.bootstrap).toBeUndefined();
    expect({ draft, document, owner }).toEqual(before);
    expect(() =>
      Schema.decodeUnknownSync(ThreadTurnStartCommand)({
        ...command,
        type: "thread.turn.start",
        commandId: "command",
      }),
    ).not.toThrow();
  });
  it("atomically bootstraps a fresh thread with owning project and provider settings", () => {
    const command = documentPromptCommand({
      ...base,
      action: { action: "host.start_chat", prompt: "New conversation" },
    });
    expect(command.threadId).toBe("new-thread");
    expect(command.bootstrap?.createThread).toMatchObject({
      projectId: "project",
      modelSelection: owner.modelSelection,
      runtimeMode: owner.runtimeMode,
      interactionMode: owner.interactionMode,
      branch: null,
      worktreePath: null,
    });
    expect(() =>
      Schema.decodeUnknownSync(ThreadTurnStartCommand)({
        ...command,
        type: "thread.turn.start",
        commandId: "command",
      }),
    ).not.toThrow();
  });
  it("rejects a mismatched owner or project rather than retargeting the click", () => {
    for (const changed of [
      { ...owner, id: ThreadId.make("different") },
      { ...owner, projectId: ProjectId.make("other") },
    ])
      expect(() =>
        documentPromptCommand({
          ...base,
          owner: changed,
          action: { action: "host.send_prompt", prompt: "Send" },
        }),
      ).toThrow("owning thread");
  });
  it("rejects closed documents and empty prompts before durable acceptance", () => {
    expect(() =>
      documentPromptCommand({
        ...base,
        document: { ...document, closed: true },
        action: { action: "host.send_prompt", prompt: "Send" },
      }),
    ).toThrow("closed");
    expect(() =>
      documentPromptCommand({ ...base, action: { action: "host.send_prompt", prompt: "  " } }),
    ).toThrow("empty");
  });
});
