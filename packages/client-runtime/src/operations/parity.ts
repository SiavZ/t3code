import { WS_METHODS } from "@t3tools/contracts";
import {
  request,
  runSessionBoundStream,
  subscribe,
  type EnvironmentRpcInput,
  type EnvironmentUnaryRpcTag,
} from "../rpc/client.ts";

const unary =
  <Tag extends EnvironmentUnaryRpcTag>(tag: Tag) =>
  (input: EnvironmentRpcInput<Tag>) =>
    request(tag, input);

const connectDesktopOnce = (input: EnvironmentRpcInput<typeof WS_METHODS.desktopConnect>) =>
  runSessionBoundStream(WS_METHODS.desktopConnect, input);

/** Typed operations reuse the selected environment's authenticated local or remote session. */
export const parityOperations = {
  globalMemory: {
    read: unary(WS_METHODS.memoryGlobalRead),
    write: unary(WS_METHODS.memoryGlobalWrite),
  },
  agentDocuments: {
    read: unary(WS_METHODS.agentDocumentsRead),
    write: unary(WS_METHODS.agentDocumentsWrite),
    action: unary(WS_METHODS.agentDocumentsAction),
    wait: unary(WS_METHODS.agentDocumentsWait),
    prepareAsset: unary(WS_METHODS.agentDocumentsPrepareAsset),
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
  search: {
    workspace: unary(WS_METHODS.agentSearch),
    history: unary(WS_METHODS.historySearch),
    importHistory: unary(WS_METHODS.historyImport),
    readHistory: unary(WS_METHODS.historyRead),
  },
  skills: {
    list: unary(WS_METHODS.skillsList),
    read: unary(WS_METHODS.skillsRead),
    load: unary(WS_METHODS.skillsLoad),
    reload: unary(WS_METHODS.skillsReload),
  },
  externalMcp: {
    configure: unary(WS_METHODS.externalMcpConfigure),
    list: unary(WS_METHODS.externalMcpList),
    connect: unary(WS_METHODS.externalMcpConnect),
    disconnect: unary(WS_METHODS.externalMcpDisconnect),
    reload: unary(WS_METHODS.externalMcpReload),
    remove: unary(WS_METHODS.externalMcpRemove),
    search: unary(WS_METHODS.externalMcpSearch),
    call: unary(WS_METHODS.externalMcpCall),
    cancel: unary(WS_METHODS.externalMcpCancel),
  },
  runtime: {
    handoff: unary(WS_METHODS.runtimeHandoff),
    fork: unary(WS_METHODS.runtimeFork),
  },
  integrations: {
    approvalGrant: unary(WS_METHODS.integrationApprovalGrant),
  },
  catalog: {
    status: unary(WS_METHODS.catalogStatus),
    search: unary(WS_METHODS.catalogSearch),
    details: unary(WS_METHODS.catalogDetails),
    select: unary(WS_METHODS.catalogSelect),
    selectOffCatalog: unary(WS_METHODS.catalogSelectOffCatalog),
    selections: unary(WS_METHODS.catalogSelections),
    clearSelection: unary(WS_METHODS.catalogClearSelection),
    suggest: unary(WS_METHODS.catalogSuggest),
  },
  gmail: {
    status: unary(WS_METHODS.gmailStatus),
    beginConnect: unary(WS_METHODS.gmailBeginConnect),
    completeConnect: unary(WS_METHODS.gmailCompleteConnect),
    disconnect: unary(WS_METHODS.gmailDisconnect),
    search: unary(WS_METHODS.gmailSearch),
    read: unary(WS_METHODS.gmailRead),
    attachment: unary(WS_METHODS.gmailAttachment),
    labels: unary(WS_METHODS.gmailLabels),
    threads: unary(WS_METHODS.gmailThreads),
    reviewMutation: unary(WS_METHODS.gmailReviewMutation),
    mutate: unary(WS_METHODS.gmailMutate),
  },
  remoteBuild: {
    status: unary(WS_METHODS.remoteBuildStatus),
    prepare: unary(WS_METHODS.remoteBuildPrepare),
    discard: unary(WS_METHODS.remoteBuildDiscard),
    submit: unary(WS_METHODS.remoteBuildSubmit),
  },
  images: {
    status: unary(WS_METHODS.imagesStatus),
    create: unary(WS_METHODS.imagesCreate),
    delete: unary(WS_METHODS.imagesDelete),
  },
  desktopAutomation: {
    connect: connectDesktopOnce,
    connectOnce: connectDesktopOnce,
    authorize: unary(WS_METHODS.desktopAuthorize),
    respond: unary(WS_METHODS.desktopRespond),
    disconnect: unary(WS_METHODS.desktopDisconnect),
    hosts: unary(WS_METHODS.desktopHosts),
    lease: unary(WS_METHODS.desktopLease),
    revoke: unary(WS_METHODS.desktopRevoke),
    invoke: unary(WS_METHODS.desktopInvoke),
  },
  browserTasks: {
    run: unary(WS_METHODS.browserRun),
    get: unary(WS_METHODS.browserGet),
    cancel: unary(WS_METHODS.browserCancel),
  },
};
