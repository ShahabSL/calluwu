import { z } from "zod";
import { JsonValueSchema, ResourceIdSchema, TimestampSchema } from "./primitives.js";

export const EventTypeSchema = z.enum([
  "session.created",
  "session.provisioning",
  "session.ready",
  "session.started",
  "session.interrupted",
  "session.canceled",
  "session.completed",
  "session.failed",
  "speech.started",
  "speech.partial",
  "speech.final",
  "reasoning.started",
  "reasoning.delta",
  "reasoning.completed",
  "tool.started",
  "tool.completed",
  "tool.failed",
  "tts.started",
  "tts.first_audio",
  "tts.completed",
  "audio.overrun",
]);

export const EventEnvelopeSchema = z.object({
  id: ResourceIdSchema,
  type: EventTypeSchema,
  version: z.literal(1),
  organizationId: ResourceIdSchema,
  projectId: ResourceIdSchema,
  deploymentId: ResourceIdSchema,
  sessionId: ResourceIdSchema,
  sequence: z.number().int().nonnegative(),
  causationId: ResourceIdSchema.optional(),
  correlationId: z.string().min(1).max(160),
  occurredAt: TimestampSchema,
  source: z.enum(["control", "runtime", "provider", "tool"]),
  privacy: z.enum(["internal", "pii", "sensitive"]),
  payload: z.record(z.string(), JsonValueSchema),
});

export const PendingRuntimeEventSchema = EventEnvelopeSchema.omit({
  id: true,
  sequence: true,
  organizationId: true,
  projectId: true,
  deploymentId: true,
  sessionId: true,
}).extend({
  /** Monotonic, generation-local identity assigned by the single-writer runtime actor. */
  producerSequence: z.number().int().nonnegative(),
});

export type EventType = z.infer<typeof EventTypeSchema>;
export type EventEnvelope = z.infer<typeof EventEnvelopeSchema>;
export type PendingRuntimeEvent = z.infer<typeof PendingRuntimeEventSchema>;
