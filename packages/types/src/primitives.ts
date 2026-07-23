import { z } from "zod";

export const CONTRACT_VERSION = "2026-07-19" as const;
const utf8Encoder = new TextEncoder();

/** Wire limits are bytes, never JavaScript UTF-16 code units. */
export function utf8ByteLength(value: string): number {
  return utf8Encoder.encode(value).byteLength;
}

export function hasUtf8ByteLengthBetween(value: string, minimum: number, maximum: number): boolean {
  const length = utf8ByteLength(value);
  return length >= minimum && length <= maximum;
}

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

export const JsonValueSchema: z.ZodType<JsonValue> = z.lazy(() =>
  z.union([
    z.null(),
    z.boolean(),
    z.number().finite(),
    z.string(),
    z.array(JsonValueSchema),
    z.record(z.string(), JsonValueSchema),
  ]),
);

export const IdPrefixSchema = z.enum([
  "org",
  "usr",
  "prj",
  "agt",
  "ver",
  "dep",
  "ses",
  "key",
  "evt",
  "dlq",
  "aud",
  "req",
  "tool",
  "dlq",
  "aud",
]);

export const ResourceIdSchema = z
  .string()
  .min(8)
  .max(80)
  .regex(/^[a-z]+_[A-Za-z0-9_-]+$/, "Invalid Calluwu resource ID");

export const TimestampSchema = z.iso.datetime({ offset: true });
export const Sha256Schema = z.string().regex(/^[a-f0-9]{64}$/);
export const SlugSchema = z
  .string()
  .min(2)
  .max(63)
  .regex(/^[a-z0-9]+(?:-[a-z0-9]+)*$/);

export function resourceId(prefix: z.infer<typeof IdPrefixSchema>, uuid: string): string {
  return `${prefix}_${uuid}`;
}
