import { z } from "zod";
import {
  hasUtf8ByteLengthBetween,
  JsonValueSchema,
  ResourceIdSchema,
  utf8ByteLength,
} from "./primitives.js";

export const MAX_REALTIME_IDENTIFIER_BYTES = 160;
export const MAX_REALTIME_TEXT_BYTES = 16_000;
export const MAX_REALTIME_SESSION_END_REASON_BYTES = 500;
export const MAX_REALTIME_CAPABILITIES = 32;
export const MAX_REALTIME_ERROR_MESSAGE_BYTES = 2_000;
export const MAX_REALTIME_ERROR_DETAILS_BYTES = 16_000;

function utf8String(minimum: number, maximum: number, label: string) {
  return z
    .string()
    .refine(
      (value) => hasUtf8ByteLengthBetween(value, minimum, maximum),
      `${label} must contain ${minimum} to ${maximum} UTF-8 bytes`,
    );
}

const RealtimeIdentifierSchema = utf8String(
  1,
  MAX_REALTIME_IDENTIFIER_BYTES,
  "Realtime identifier",
);
const RealtimeTextSchema = utf8String(1, MAX_REALTIME_TEXT_BYTES, "Realtime text");

const RealtimeBaseSchema = z
  .object({
    protocolVersion: z.literal(1),
    sessionId: ResourceIdSchema,
    messageId: RealtimeIdentifierSchema,
    runtimeGeneration: z.number().int().nonnegative(),
  })
  .strict();

export const RealtimeClientMessageSchema = z.discriminatedUnion("type", [
  RealtimeBaseSchema.extend({ type: z.literal("session.start") }),
  RealtimeBaseSchema.extend({
    type: z.literal("input.text"),
    text: RealtimeTextSchema,
  }),
  RealtimeBaseSchema.extend({ type: z.literal("input.commit") }),
  RealtimeBaseSchema.extend({
    type: z.literal("response.cancel"),
    responseId: RealtimeIdentifierSchema,
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("playout.ack"),
    responseId: RealtimeIdentifierSchema,
    playedThroughMs: z.number().finite().nonnegative(),
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("session.end"),
    reason: utf8String(0, MAX_REALTIME_SESSION_END_REASON_BYTES, "Session end reason"),
  }),
]);

export const RealtimeServerMessageSchema = z.discriminatedUnion("type", [
  RealtimeBaseSchema.extend({
    type: z.literal("session.ready"),
    capabilities: z.array(RealtimeIdentifierSchema).max(MAX_REALTIME_CAPABILITIES),
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("session.started"),
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("transcript.delta"),
    turnId: RealtimeIdentifierSchema,
    text: RealtimeTextSchema,
    isFinal: z.boolean(),
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("response.delta"),
    responseId: RealtimeIdentifierSchema,
    epoch: z.number().int().nonnegative(),
    text: RealtimeTextSchema,
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("response.completed"),
    responseId: RealtimeIdentifierSchema,
    epoch: z.number().int().nonnegative(),
    interrupted: z.boolean(),
  }),
  RealtimeBaseSchema.extend({
    type: z.literal("error"),
    code: RealtimeIdentifierSchema,
    message: utf8String(1, MAX_REALTIME_ERROR_MESSAGE_BYTES, "Realtime error message"),
    details: z
      .record(z.string(), JsonValueSchema)
      .refine(
        (value) => utf8ByteLength(JSON.stringify(value)) <= MAX_REALTIME_ERROR_DETAILS_BYTES,
        `Realtime error details exceed ${MAX_REALTIME_ERROR_DETAILS_BYTES} UTF-8 bytes`,
      )
      .optional(),
  }),
]);

export const REALTIME_AUDIO_MAGIC = "CWU1" as const;
export const MAX_REALTIME_AUDIO_HEADER_BYTES = 4_096;
export const MAX_REALTIME_AUDIO_PAYLOAD_BYTES = 65_536;

export const RealtimeAudioFrameHeaderSchema = RealtimeBaseSchema.extend({
  type: z.literal("audio.chunk"),
  responseId: RealtimeIdentifierSchema,
  epoch: z.number().int().nonnegative(),
  sequence: z.number().int().nonnegative(),
  encoding: z.literal("pcm16le"),
  sampleRateHz: z.number().int().min(8_000).max(48_000),
  channels: z.literal(1),
});

const audioMagic = new TextEncoder().encode(REALTIME_AUDIO_MAGIC);
const audioHeaderEncoder = new TextEncoder();
const audioHeaderDecoder = new TextDecoder("utf-8", { fatal: true });

export type RealtimeAudioFrameHeader = z.infer<typeof RealtimeAudioFrameHeaderSchema>;
export type RealtimeAudioFrame = {
  header: RealtimeAudioFrameHeader;
  audio: Uint8Array;
};

function validatePcmPayload(audio: Uint8Array): void {
  if (
    audio.byteLength === 0 ||
    audio.byteLength > MAX_REALTIME_AUDIO_PAYLOAD_BYTES ||
    audio.byteLength % 2 !== 0
  ) {
    throw new TypeError(
      `Realtime PCM16LE payload must contain an even 2-${MAX_REALTIME_AUDIO_PAYLOAD_BYTES} bytes`,
    );
  }
}

/** Encode a versioned Calluwu binary WebSocket audio frame. */
export function encodeRealtimeAudioFrame(
  headerInput: RealtimeAudioFrameHeader,
  audio: Uint8Array,
): Uint8Array {
  const header = RealtimeAudioFrameHeaderSchema.parse(headerInput);
  validatePcmPayload(audio);
  const headerBytes = audioHeaderEncoder.encode(JSON.stringify(header));
  if (headerBytes.byteLength > MAX_REALTIME_AUDIO_HEADER_BYTES) {
    throw new TypeError(`Realtime audio header exceeds ${MAX_REALTIME_AUDIO_HEADER_BYTES} bytes`);
  }

  const frame = new Uint8Array(8 + headerBytes.byteLength + audio.byteLength);
  frame.set(audioMagic, 0);
  new DataView(frame.buffer).setUint32(4, headerBytes.byteLength, false);
  frame.set(headerBytes, 8);
  frame.set(audio, 8 + headerBytes.byteLength);
  return frame;
}

/** Decode and validate an untrusted Calluwu binary WebSocket audio frame. */
export function decodeRealtimeAudioFrame(input: ArrayBuffer | Uint8Array): RealtimeAudioFrame {
  const frame = input instanceof Uint8Array ? input : new Uint8Array(input);
  if (frame.byteLength < 10 || !audioMagic.every((byte, index) => frame[index] === byte)) {
    throw new TypeError("Realtime audio frame has an invalid CWU1 prefix");
  }
  const headerLength = new DataView(frame.buffer, frame.byteOffset + 4, 4).getUint32(0, false);
  if (headerLength === 0 || headerLength > MAX_REALTIME_AUDIO_HEADER_BYTES) {
    throw new TypeError("Realtime audio frame header length is invalid");
  }
  const payloadOffset = 8 + headerLength;
  if (payloadOffset >= frame.byteLength) {
    throw new TypeError("Realtime audio frame is truncated");
  }

  let headerJson: unknown;
  try {
    headerJson = JSON.parse(audioHeaderDecoder.decode(frame.subarray(8, payloadOffset)));
  } catch {
    throw new TypeError("Realtime audio frame header is not valid UTF-8 JSON");
  }
  const header = RealtimeAudioFrameHeaderSchema.parse(headerJson);
  const audio = frame.subarray(payloadOffset);
  validatePcmPayload(audio);
  return { header, audio };
}

export type RealtimeClientMessage = z.infer<typeof RealtimeClientMessageSchema>;
export type RealtimeServerMessage = z.infer<typeof RealtimeServerMessageSchema>;
