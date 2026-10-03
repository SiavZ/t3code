import * as NodeCrypto from "node:crypto";

/** Stable request identity without retaining a recoverable request body in durable receipts. */
export const requestFingerprint = (authority: string, operation: string, input: unknown) =>
  NodeCrypto.createHash("sha256")
    .update(
      JSON.stringify([authority, operation, input], (_key: string, value: unknown) =>
        value !== null && typeof value === "object" && !Array.isArray(value)
          ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b)))
          : value,
      ),
    )
    .digest("hex");
