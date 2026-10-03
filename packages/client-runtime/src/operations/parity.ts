import { WS_METHODS } from "@t3tools/contracts";
import { request, type EnvironmentRpcInput, type EnvironmentUnaryRpcTag } from "../rpc/client.ts";

const unary =
  <Tag extends EnvironmentUnaryRpcTag>(tag: Tag) =>
  (input: EnvironmentRpcInput<Tag>) =>
    request(tag, input);

/** Typed operations reuse the selected environment's authenticated local or remote session. */
export const parityOperations = {
  coordination: {
    read: unary(WS_METHODS.coordinationRead),
    write: unary(WS_METHODS.coordinationWrite),
    mailboxRead: unary(WS_METHODS.coordinationMailboxRead),
    mailboxWrite: unary(WS_METHODS.coordinationMailboxWrite),
  },
  unattendedGrants: {
    create: unary(WS_METHODS.unattendedGrantCreate),
    list: unary(WS_METHODS.unattendedGrantList),
    revoke: unary(WS_METHODS.unattendedGrantRevoke),
  },
};
