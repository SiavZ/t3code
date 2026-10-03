import * as Schema from "effect/Schema";
import { TrimmedNonEmptyString } from "./baseSchemas.ts";

const text = (max: number) => TrimmedNonEmptyString.check(Schema.isMaxLength(max));
export class CoordinationError extends Schema.TaggedError<CoordinationError>()(
  "CoordinationError",
  {
    code: Schema.Literals(["invalid", "forbidden", "conflict", "notFound", "busy", "exhausted"]),
    detail: text(2048),
  },
) {}
