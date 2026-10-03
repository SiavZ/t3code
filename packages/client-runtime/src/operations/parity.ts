import { WS_METHODS } from "@t3tools/contracts";
import {
  request,
  subscribe,
  type EnvironmentRpcInput,
  type EnvironmentUnaryRpcTag,
} from "../rpc/client.ts";

const unary =
  <Tag extends EnvironmentUnaryRpcTag>(tag: Tag) =>
  (input: EnvironmentRpcInput<Tag>) =>
    request(tag, input);

/** Typed operations reuse the selected environment's authenticated local or remote session. */
export const parityOperations = {
  globalMemory: {
    read: unary(WS_METHODS.memoryGlobalRead),
    write: unary(WS_METHODS.memoryGlobalWrite),
  },
  coordination: {
    read: unary(WS_METHODS.coordinationRead),
    write: unary(WS_METHODS.coordinationWrite),
    mailboxRead: unary(WS_METHODS.coordinationMailboxRead),
    mailboxWrite: unary(WS_METHODS.coordinationMailboxWrite),
  },
  memory: {
    remember: unary(WS_METHODS.memoryRemember),
    recall: unary(WS_METHODS.memoryRecall),
    search: unary(WS_METHODS.memorySearch),
    forget: unary(WS_METHODS.memoryForget),
    tag: unary(WS_METHODS.memoryTag),
    link: unary(WS_METHODS.memoryLink),
    related: unary(WS_METHODS.memoryRelated),
  },
  quality: {
    read: unary(WS_METHODS.qualityRead),
    update: unary(WS_METHODS.qualityUpdate),
    subscribeChanges: (input: EnvironmentRpcInput<typeof WS_METHODS.qualitySubscribeChanges>) =>
      subscribe(WS_METHODS.qualitySubscribeChanges, input),
  },
  scheduled: {
    create: unary(WS_METHODS.scheduledCreate),
    list: unary(WS_METHODS.scheduledList),
    get: unary(WS_METHODS.scheduledGet),
    cancel: unary(WS_METHODS.scheduledCancel),
  },
  unattendedGrants: {
    create: unary(WS_METHODS.unattendedGrantCreate),
    list: unary(WS_METHODS.unattendedGrantList),
    revoke: unary(WS_METHODS.unattendedGrantRevoke),
  },
  ambient: {
    configure: unary(WS_METHODS.ambientConfigure),
    get: unary(WS_METHODS.ambientGet),
    stop: unary(WS_METHODS.ambientStop),
  },
  backgroundJobs: {
    start: unary(WS_METHODS.backgroundJobStart),
    list: unary(WS_METHODS.backgroundJobList),
    get: unary(WS_METHODS.backgroundJobGet),
    output: unary(WS_METHODS.backgroundJobOutput),
    cancel: unary(WS_METHODS.backgroundJobCancel),
    wait: unary(WS_METHODS.backgroundJobWait),
    subscribe: unary(WS_METHODS.backgroundJobSubscribe),
    cleanup: unary(WS_METHODS.backgroundJobCleanup),
  },
};
