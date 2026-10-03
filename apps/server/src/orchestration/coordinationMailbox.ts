import {
  CoordinationError,
  type CoordinationMailbox,
  type CoordinationMailboxWriteInput,
  type ThreadId,
} from "@t3tools/contracts";
import * as NodeCrypto from "node:crypto";

export function applyMailboxWrite(
  current: CoordinationMailbox | null,
  input: CoordinationMailboxWriteInput,
  members: ReadonlySet<ThreadId>,
): CoordinationMailbox {
  if (!members.has(input.callerThreadId))
    throw new CoordinationError({
      code: "forbidden",
      detail: "Caller is outside the active persisted root lineage.",
    });
  const mailbox = current ?? {
    rootThreadId: input.rootThreadId,
    revision: 0,
    envelopes: [],
    channels: [],
    context: [],
  };
  if (mailbox.rootThreadId !== input.rootThreadId || mailbox.revision !== input.expectedRevision)
    throw new CoordinationError({ code: "conflict", detail: "Mailbox revision mismatch." });
  let next: CoordinationMailbox = { ...mailbox, revision: mailbox.revision + 1 };
  if (input.operation === "message") {
    let recipients = input.recipientThreadIds;
    if (input.channelId !== null) {
      const channel = mailbox.channels.find((channel) => channel.id === input.channelId);
      if (!channel || channel.closed || !channel.members.includes(input.callerThreadId))
        throw new CoordinationError({
          code: "forbidden",
          detail: "Channel is closed or sender is not a member.",
        });
      if (recipients.length)
        throw new CoordinationError({
          code: "invalid",
          detail: "Channel fan-out cannot specify separate recipients.",
        });
      recipients = channel.members.filter((id) => id !== input.callerThreadId);
    }
    recipients = [...new Set(recipients)];
    if (!recipients.length || recipients.some((id) => !members.has(id)))
      throw new CoordinationError({
        code: "forbidden",
        detail: "Recipients must be active members of this root.",
      });
    for (const recipient of recipients)
      if (
        mailbox.envelopes.filter(
          (envelope) => envelope.recipientThreadId === recipient && envelope.delivery === "pending",
        ).length >= 64
      )
        throw new CoordinationError({ code: "busy", detail: "Recipient unread quota reached." });
    const envelopes = recipients.map((recipientThreadId) => ({
      id: NodeCrypto.createHash("sha256")
        .update(JSON.stringify([input.rootThreadId, input.commandId, recipientThreadId]))
        .digest("hex"),
      senderThreadId: input.callerThreadId,
      recipientThreadId,
      channelId: input.channelId,
      text: input.text,
      delivery: "pending" as const,
    }));
    next = { ...next, envelopes: [...mailbox.envelopes, ...envelopes] };
  } else if (input.operation === "ack") {
    for (const id of input.envelopeIds) {
      const envelope = mailbox.envelopes.find((envelope) => envelope.id === id);
      if (!envelope || envelope.recipientThreadId !== input.callerThreadId)
        throw new CoordinationError({
          code: "forbidden",
          detail: "Only the recipient may acknowledge an envelope.",
        });
    }
    next = {
      ...next,
      envelopes: mailbox.envelopes.map((envelope) =>
        input.envelopeIds.includes(envelope.id) && envelope.delivery === "pending"
          ? { ...envelope, delivery: "read" }
          : envelope,
      ),
    };
  } else if (input.operation === "pruneRead") {
    next = {
      ...next,
      envelopes: mailbox.envelopes.filter(
        (envelope) =>
          envelope.delivery === "pending" ||
          (input.callerThreadId !== input.rootThreadId &&
            envelope.recipientThreadId !== input.callerThreadId),
      ),
    };
  } else if (input.operation === "contextWrite") {
    const context = mailbox.context.filter((entry) => entry.key !== input.key);
    next = {
      ...next,
      context:
        input.value === null ? context : [...context, { key: input.key, value: input.value }],
    };
  } else {
    if (input.operation === "channelCreate") {
      if (
        input.callerThreadId !== input.rootThreadId ||
        mailbox.channels.some((channel) => channel.id === input.channelId) ||
        input.members.some((id) => !members.has(id))
      )
        throw new CoordinationError({
          code: "forbidden",
          detail: "Only root may create a unique channel of active members.",
        });
      next = {
        ...next,
        channels: [
          ...mailbox.channels,
          {
            id: input.channelId,
            name: input.name,
            members: [...new Set(input.members)],
            closed: false,
          },
        ],
      };
    } else {
      const channel = mailbox.channels.find((channel) => channel.id === input.channelId);
      if (!channel) throw new CoordinationError({ code: "notFound", detail: "Channel not found." });
      if (input.operation === "channelClose") {
        if (input.callerThreadId !== input.rootThreadId)
          throw new CoordinationError({
            code: "forbidden",
            detail: "Only root may close or reopen a channel.",
          });
        next = {
          ...next,
          channels: mailbox.channels.map((entry) =>
            entry.id === channel.id ? { ...entry, closed: input.closed } : entry,
          ),
        };
      } else {
        if (
          (input.callerThreadId !== input.rootThreadId &&
            input.memberThreadId !== input.callerThreadId) ||
          !members.has(input.memberThreadId)
        )
          throw new CoordinationError({
            code: "forbidden",
            detail: "Membership changes must target self or be root authorized.",
          });
        if (channel.closed && input.joined)
          throw new CoordinationError({
            code: "forbidden",
            detail: "Cannot join a closed channel.",
          });
        next = {
          ...next,
          channels: mailbox.channels.map((entry) =>
            entry.id === channel.id
              ? {
                  ...entry,
                  members: input.joined
                    ? [...new Set([...entry.members, input.memberThreadId])]
                    : entry.members.filter((id) => id !== input.memberThreadId),
                }
              : entry,
          ),
        };
      }
    }
  }
  if (
    next.envelopes.length > 1024 ||
    next.channels.length > 32 ||
    next.context.length > 32 ||
    next.channels.some((channel) => channel.members.length > 64) ||
    new TextEncoder().encode(JSON.stringify(next)).length > 262_144
  )
    throw new CoordinationError({
      code: "invalid",
      detail:
        "Mailbox exceeds bounded document limits. Acknowledge and archive old messages before adding more.",
    });
  return next;
}
