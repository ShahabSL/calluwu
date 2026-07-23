import { z } from "zod";
import {
  CONTRACT_VERSION,
  hasUtf8ByteLengthBetween,
  JsonValueSchema,
  Sha256Schema,
  SlugSchema,
} from "./primitives.js";

export const ToolSideEffectSchema = z.enum(["none", "idempotent", "commit_once"]);
export const ToolExecutionSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("builtin"), integration: SlugSchema }).strict(),
  z
    .object({
      kind: z.literal("https"),
      url: z.url({ protocol: /^https$/ }),
      secretRef: z
        .string()
        .min(1)
        .max(128)
        .refine((value) => hasUtf8ByteLengthBetween(value, 1, 128), "secretRef exceeds 128 bytes")
        .optional(),
    })
    .strict(),
  z.object({ kind: z.literal("local") }).strict(),
]);

export const ToolDefinitionSchema = z
  .object({
    name: z
      .string()
      .min(1)
      .max(64)
      .regex(/^[A-Za-z][A-Za-z0-9_-]*$/),
    description: z
      .string()
      .min(1)
      .max(2_000)
      .refine(
        (value) => hasUtf8ByteLengthBetween(value, 1, 2_000),
        "Tool description exceeds 2000 bytes",
      ),
    inputSchema: z.record(z.string(), JsonValueSchema),
    timeoutMs: z.number().int().min(100).max(30_000).default(10_000),
    sideEffect: ToolSideEffectSchema.default("none"),
    execution: ToolExecutionSchema,
  })
  .strict();

export const ProviderReferenceSchema = z
  .object({
    provider: SlugSchema,
    model: z
      .string()
      .min(1)
      .max(160)
      .refine(
        (value) => hasUtf8ByteLengthBetween(value, 1, 160),
        "Provider model exceeds 160 bytes",
      ),
    settings: z.record(z.string(), JsonValueSchema).default({}),
  })
  .strict();

export const ProviderCapabilitySchema = z.enum([
  "batch-stt",
  "streaming-stt",
  "streaming-reasoning",
  "streaming-tts",
  "tool-execution",
  "realtime-speech",
]);

export type ProviderCapability = z.infer<typeof ProviderCapabilitySchema>;

export const CLOUDFLARE_NOVA_3_LANGUAGES = [
  "en",
  "en-US",
  "en-AU",
  "en-GB",
  "en-IN",
  "en-NZ",
  "es",
  "es-419",
  "fr",
  "fr-CA",
  "de",
  "de-CH",
  "hi",
  "ru",
  "pt",
  "pt-BR",
  "pt-PT",
  "ja",
  "it",
  "nl",
  "multi",
] as const;

export const CLOUDFLARE_AURA_2_EN_SPEAKERS = [
  "amalthea",
  "andromeda",
  "apollo",
  "arcas",
  "aries",
  "asteria",
  "athena",
  "atlas",
  "aurora",
  "callista",
  "cora",
  "cordelia",
  "delia",
  "draco",
  "electra",
  "harmonia",
  "helena",
  "hera",
  "hermes",
  "hyperion",
  "iris",
  "janus",
  "juno",
  "jupiter",
  "luna",
  "mars",
  "minerva",
  "neptune",
  "odysseus",
  "ophelia",
  "orion",
  "orpheus",
  "pandora",
  "phoebe",
  "pluto",
  "saturn",
  "thalia",
  "theia",
  "vesta",
  "zeus",
] as const;

export const CLOUDFLARE_AURA_2_ES_SPEAKERS = [
  "sirio",
  "nestor",
  "carina",
  "celeste",
  "alvaro",
  "diana",
  "aquila",
  "selena",
  "estrella",
  "javier",
] as const;

export const CloudflareNova3SettingsSchema = z
  .object({
    detectLanguage: z.boolean().optional(),
    keyterm: z
      .string()
      .min(1)
      .max(2_000)
      .refine((value) => hasUtf8ByteLengthBetween(value, 1, 2_000), "keyterm exceeds 2000 bytes")
      .optional(),
    mipOptOut: z.boolean().optional(),
  })
  .strict();

export const CloudflareGptOss20bSettingsSchema = z
  .object({
    maxTokens: z.number().int().min(1).max(4_096).optional(),
    temperature: z.number().min(0).max(5).optional(),
    topP: z.number().min(0.001).max(1).optional(),
  })
  .strict();

export const CloudflareAura2EnSettingsSchema = z
  .object({ speaker: z.enum(CLOUDFLARE_AURA_2_EN_SPEAKERS).optional() })
  .strict();

export const CloudflareAura2EsSettingsSchema = z
  .object({ speaker: z.enum(CLOUDFLARE_AURA_2_ES_SPEAKERS).optional() })
  .strict();

export const AgentDefinitionSchema = z
  .object({
    name: SlugSchema,
    instructions: z
      .string()
      .min(1)
      .max(64_000)
      .refine(
        (value) => hasUtf8ByteLengthBetween(value, 1, 64_000),
        "Instructions exceed 64000 bytes",
      ),
    providers: z
      .object({
        speechToText: ProviderReferenceSchema,
        reasoning: ProviderReferenceSchema,
        textToSpeech: ProviderReferenceSchema,
      })
      .strict(),
    voice: z
      .object({
        id: z
          .string()
          .min(1)
          .max(160)
          .refine((value) => hasUtf8ByteLengthBetween(value, 1, 160), "Voice ID exceeds 160 bytes"),
        language: z
          .string()
          .min(2)
          .max(35)
          .refine(
            (value) => hasUtf8ByteLengthBetween(value, 2, 35),
            "Voice language exceeds 35 bytes",
          )
          .default("en-US"),
        sampleRateHz: z.number().int().min(8_000).max(48_000).default(16_000),
      })
      .strict(),
    tools: z.array(ToolDefinitionSchema).max(64).default([]),
    limits: z
      .object({
        maxSessionSeconds: z.number().int().min(10).max(14_400).default(3_600),
        maxConcurrentTools: z.number().int().min(1).max(16).default(4),
        maxHistoryMessages: z.number().int().min(2).max(1_000).default(100),
      })
      .strict()
      .default({
        maxSessionSeconds: 3_600,
        maxConcurrentTools: 4,
        maxHistoryMessages: 100,
      }),
    metadata: z.record(z.string(), JsonValueSchema).default({}),
  })
  .strict()
  .superRefine((definition, context) => {
    const names = new Set<string>();
    definition.tools.forEach((tool, index) => {
      if (names.has(tool.name)) {
        context.addIssue({
          code: "custom",
          message: `Tool name ${tool.name} is declared more than once`,
          path: ["tools", index, "name"],
        });
      }
      names.add(tool.name);
    });
  });

export type ToolDefinition = z.infer<typeof ToolDefinitionSchema>;
export type AgentDefinition = z.infer<typeof AgentDefinitionSchema>;

/** Baseline engine features required by a validated agent definition. */
export function requiredCapabilitiesForAgent(
  definition: Pick<AgentDefinition, "tools">,
): ProviderCapability[] {
  // Calluwu's current cascade starts STT only after input.commit, so an adapter that accepts a
  // complete utterance must not be advertised as end-to-end streaming STT.
  const capabilities: ProviderCapability[] = ["batch-stt", "streaming-reasoning", "streaming-tts"];
  if (definition.tools.length > 0) capabilities.push("tool-execution");
  return capabilities;
}

export const AgentManifestSchema = z
  .object({
    contractVersion: z.literal(CONTRACT_VERSION),
    definition: AgentDefinitionSchema,
    requiredCapabilities: z
      .array(ProviderCapabilitySchema)
      .max(128)
      .refine((values) => new Set(values).size === values.length, "Capabilities must be unique"),
    artifact: z
      .object({
        sha256: Sha256Schema,
        sizeBytes: z
          .number()
          .int()
          .nonnegative()
          .max(10 * 1024 * 1024),
        format: z.literal("javascript-esm"),
      })
      .strict(),
  })
  .strict()
  .superRefine((manifest, context) => {
    const available = new Set(manifest.requiredCapabilities);
    for (const required of requiredCapabilitiesForAgent(manifest.definition)) {
      if (!available.has(required)) {
        context.addIssue({
          code: "custom",
          message: `Manifest is missing required capability ${required}`,
          path: ["requiredCapabilities"],
        });
      }
    }
  });

export type AgentManifest = z.infer<typeof AgentManifestSchema>;
