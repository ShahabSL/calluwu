import { z } from "zod";
import { AgentDefinitionSchema } from "./agent.js";
import { ApiKeyScopeSchema, DeploymentSchema, ProjectSchema, SessionSchema } from "./domain.js";
import { EventEnvelopeSchema } from "./events.js";
import { ResourceIdSchema, Sha256Schema, SlugSchema, TimestampSchema } from "./primitives.js";

const RealtimeSocketUrlSchema = z.url().superRefine((value, context) => {
  const url = new URL(value);
  const loopback =
    url.hostname === "localhost" || url.hostname === "127.0.0.1" || url.hostname === "[::1]";
  if (url.protocol !== "wss:" && !(url.protocol === "ws:" && loopback)) {
    context.addIssue({
      code: "custom",
      message: "Realtime URL must use WSS, except WS on a loopback host",
    });
  }
  if (url.username || url.password || url.search || url.hash) {
    context.addIssue({
      code: "custom",
      message: "Realtime URL cannot contain credentials, query parameters, or a fragment",
    });
  }
});

export const ApiErrorSchema = z.object({
  error: z.object({
    code: z.string().min(1).max(80),
    message: z.string().min(1).max(1_000),
    requestId: ResourceIdSchema,
    details: z.record(z.string(), z.unknown()).optional(),
  }),
});

export const CreateProjectRequestSchema = z.object({
  slug: SlugSchema,
  name: z.string().min(1).max(120),
});

export const CreateDeploymentRequestSchema = z.object({
  agentSlug: SlugSchema,
  agentName: z.string().min(1).max(120),
  environment: z.enum(["development", "staging", "production"]),
  definition: AgentDefinitionSchema,
  artifact: z.object({
    sha256: Sha256Schema,
    sizeBytes: z
      .number()
      .int()
      .nonnegative()
      .max(10 * 1024 * 1024),
    base64: z.string().min(1),
  }),
});

export const CreateSessionRequestSchema = z.object({
  deploymentId: ResourceIdSchema,
  metadata: z.record(z.string(), z.unknown()).default({}),
});

export const CreateApiKeyRequestSchema = z
  .object({
    name: z.string().trim().min(1).max(80),
    projectId: ResourceIdSchema.nullable().optional(),
    scopes: z
      .array(ApiKeyScopeSchema)
      .min(1)
      .max(ApiKeyScopeSchema.options.length)
      .refine((values) => new Set(values).size === values.length, "Scopes must be unique"),
    expiresAt: TimestampSchema,
  })
  .strict();

export const RotateApiKeyRequestSchema = z
  .object({
    expiresAt: TimestampSchema,
  })
  .strict();

export const ApiKeyMetadataSchema = z.object({
  id: ResourceIdSchema,
  organizationId: ResourceIdSchema,
  projectId: ResourceIdSchema.nullable(),
  name: z.string().min(1).max(80),
  scopes: z.array(ApiKeyScopeSchema).min(1),
  status: z.enum(["active", "expired", "revoked"]),
  createdAt: TimestampSchema,
  expiresAt: TimestampSchema.nullable(),
  lastUsedAt: TimestampSchema.nullable(),
  revokedAt: TimestampSchema.nullable(),
  rotatedFromKeyId: ResourceIdSchema.nullable(),
});

export const ProjectResponseSchema = z.object({ project: ProjectSchema });
export const ProjectListResponseSchema = z.object({ projects: z.array(ProjectSchema) });
export const DeploymentResponseSchema = z.object({ deployment: DeploymentSchema });
export const DeploymentListResponseSchema = z.object({ deployments: z.array(DeploymentSchema) });
export const SessionResponseSchema = z.object({
  session: SessionSchema,
  realtime: z.object({
    url: RealtimeSocketUrlSchema,
    token: z.string().min(16).max(4_096),
    expiresAt: TimestampSchema,
    sampleRateHz: z.number().int().min(8_000).max(48_000),
  }),
});
export const SessionListResponseSchema = z.object({ sessions: z.array(SessionSchema) });
export const SessionMutationResponseSchema = z.object({ session: SessionSchema });
export const EventListResponseSchema = z
  .object({
    events: z.array(EventEnvelopeSchema).max(25),
    nextAfter: z.number().int().nonnegative().nullable(),
    hasMore: z.boolean(),
  })
  .superRefine((page, context) => {
    const lastSequence = page.events.at(-1)?.sequence ?? null;
    if (page.nextAfter !== lastSequence) {
      context.addIssue({
        code: "custom",
        message: "nextAfter must identify the final event in the page",
        path: ["nextAfter"],
      });
    }
    if (page.hasMore && page.events.length === 0) {
      context.addIssue({
        code: "custom",
        message: "An empty event page cannot advertise more events",
        path: ["hasMore"],
      });
    }
  });
export const ApiKeyResponseSchema = z.object({ apiKey: ApiKeyMetadataSchema });
export const ApiKeyListResponseSchema = z.object({
  apiKeys: z.array(ApiKeyMetadataSchema).max(200),
});
const ApiKeyIssuedResponseSchema = ApiKeyResponseSchema.extend({
  credentialAlreadyIssued: z.literal(false),
  credential: z.string().min(32).max(1_024),
}).strict();
const ApiKeyCredentialReplayResponseSchema = ApiKeyResponseSchema.extend({
  credentialAlreadyIssued: z.literal(true),
}).strict();

export const ApiKeyCredentialResponseSchema = z.discriminatedUnion("credentialAlreadyIssued", [
  ApiKeyIssuedResponseSchema,
  ApiKeyCredentialReplayResponseSchema,
]);

const ApiKeyRotationMetadataSchema = z
  .object({
    previousKeyId: ResourceIdSchema,
    requiresPreviousKeyRevocation: z.literal(true),
  })
  .strict();

export const ApiKeyRotationResponseSchema = z.discriminatedUnion("credentialAlreadyIssued", [
  ApiKeyIssuedResponseSchema.extend({ rotation: ApiKeyRotationMetadataSchema }),
  ApiKeyCredentialReplayResponseSchema.extend({ rotation: ApiKeyRotationMetadataSchema }),
]);

export type ApiError = z.infer<typeof ApiErrorSchema>;
export type CreateDeploymentRequest = z.infer<typeof CreateDeploymentRequestSchema>;
export type CreateSessionRequest = z.infer<typeof CreateSessionRequestSchema>;
export type CreateApiKeyRequest = z.infer<typeof CreateApiKeyRequestSchema>;
export type RotateApiKeyRequest = z.infer<typeof RotateApiKeyRequestSchema>;
export type ApiKeyMetadata = z.infer<typeof ApiKeyMetadataSchema>;
export type ApiKeyCredentialResponse = z.infer<typeof ApiKeyCredentialResponseSchema>;
export type ApiKeyRotationResponse = z.infer<typeof ApiKeyRotationResponseSchema>;
export type EventListResponse = z.infer<typeof EventListResponseSchema>;
