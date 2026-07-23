import { z } from "zod";
import { AgentDefinitionSchema } from "./agent.js";
import { ResourceIdSchema, Sha256Schema, SlugSchema, TimestampSchema } from "./primitives.js";

export const OrganizationRoleSchema = z.enum(["owner", "admin", "developer", "viewer", "billing"]);
export const ApiKeyScopeSchema = z.enum([
  "organizations:read",
  "organizations:write",
  "api-keys:read",
  "api-keys:write",
  "projects:read",
  "projects:write",
  "agents:read",
  "agents:write",
  "deployments:read",
  "deployments:write",
  "sessions:read",
  "sessions:write",
  "media:read",
  "media:write",
  "telephony:read",
  "telephony:write",
  "integrations:read",
  "integrations:write",
  "billing:read",
  "billing:write",
]);

export const OrganizationSchema = z.object({
  id: ResourceIdSchema,
  slug: SlugSchema,
  name: z.string().min(1).max(120),
  createdAt: TimestampSchema,
});

export const ProjectSchema = z.object({
  id: ResourceIdSchema,
  organizationId: ResourceIdSchema,
  slug: SlugSchema,
  name: z.string().min(1).max(120),
  activeDeploymentId: ResourceIdSchema.nullable(),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema,
});

export const AgentSchema = z.object({
  id: ResourceIdSchema,
  organizationId: ResourceIdSchema,
  projectId: ResourceIdSchema,
  slug: SlugSchema,
  name: z.string().min(1).max(120),
  createdAt: TimestampSchema,
  updatedAt: TimestampSchema,
});

export const AgentVersionSchema = z.object({
  id: ResourceIdSchema,
  organizationId: ResourceIdSchema,
  projectId: ResourceIdSchema,
  agentId: ResourceIdSchema,
  number: z.number().int().positive(),
  artifactSha256: Sha256Schema,
  definition: AgentDefinitionSchema,
  createdAt: TimestampSchema,
});

export const DeploymentStatusSchema = z.enum(["creating", "ready", "failed", "archived"]);
export const DeploymentSchema = z.object({
  id: ResourceIdSchema,
  organizationId: ResourceIdSchema,
  projectId: ResourceIdSchema,
  agentId: ResourceIdSchema,
  versionId: ResourceIdSchema,
  status: DeploymentStatusSchema,
  environment: z.enum(["development", "staging", "production"]),
  createdAt: TimestampSchema,
  activatedAt: TimestampSchema.nullable(),
});

export const SessionStatusSchema = z.enum([
  "created",
  "provisioning",
  "ready",
  "active",
  "draining",
  "completed",
  "canceled",
  "failed",
]);
export const SessionSchema = z.object({
  id: ResourceIdSchema,
  organizationId: ResourceIdSchema,
  projectId: ResourceIdSchema,
  deploymentId: ResourceIdSchema,
  status: SessionStatusSchema,
  runtimeShardId: z.string().min(1).max(160).nullable(),
  runtimeGeneration: z.number().int().nonnegative(),
  createdAt: TimestampSchema,
  startedAt: TimestampSchema.nullable(),
  endedAt: TimestampSchema.nullable(),
});

export type Organization = z.infer<typeof OrganizationSchema>;
export type Project = z.infer<typeof ProjectSchema>;
export type Agent = z.infer<typeof AgentSchema>;
export type AgentVersion = z.infer<typeof AgentVersionSchema>;
export type Deployment = z.infer<typeof DeploymentSchema>;
export type Session = z.infer<typeof SessionSchema>;
export type ApiKeyScope = z.infer<typeof ApiKeyScopeSchema>;
