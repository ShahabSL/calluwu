import { z } from "zod";
import { CLOUDFLARE_AURA_2_EN_SPEAKERS, CLOUDFLARE_AURA_2_ES_SPEAKERS } from "./agent.js";
import {
  hasUtf8ByteLengthBetween,
  ResourceIdSchema,
  SlugSchema,
  utf8ByteLength,
} from "./primitives.js";

export const EASY_INTAKE_MAX_FIELDS = 40;
export const EASY_INTAKE_MAX_SELECT_OPTIONS = 128;
export const EASY_INTAKE_MAX_DESTINATIONS = 8;
export const EASY_INTAKE_MAX_RECORD_BYTES = 48 * 1024;
export const EASY_INTAKE_DEFAULT_RETENTION_DAYS = 30;
export const EASY_INTAKE_MAX_RETENTION_DAYS = 365;
export const EASY_AGENT_MAX_AUTHORED_BYTES = 36 * 1024;

const boundedString = (minimumBytes: number, maximumBytes: number, message: string) =>
  z
    .string()
    .refine((value) => hasUtf8ByteLengthBetween(value, minimumBytes, maximumBytes), message);

export const IntakeFieldIdSchema = ResourceIdSchema.refine(
  (value) => value.startsWith("fld_"),
  "Expected a fld resource ID",
);

export const IntakeSchemaIdSchema = ResourceIdSchema.refine(
  (value) => value.startsWith("isc_"),
  "Expected an isc resource ID",
);

export const IntakeSchemaVersionIdSchema = ResourceIdSchema.refine(
  (value) => value.startsWith("isv_"),
  "Expected an isv resource ID",
);

export const IntakeRecordIdSchema = ResourceIdSchema.refine(
  (value) => value.startsWith("irc_"),
  "Expected an irc resource ID",
);

export const IntakeDestinationIdSchema = ResourceIdSchema.refine(
  (value) => value.startsWith("dst_"),
  "Expected a dst resource ID",
);

export const IntakeExportIdSchema = ResourceIdSchema.refine(
  (value) => value.startsWith("ixp_"),
  "Expected an ixp resource ID",
);

export const IntakeFieldKeySchema = z
  .string()
  .min(1)
  .max(63)
  .regex(/^[a-z][a-z0-9_]*$/, "Field keys must use lower-case snake_case")
  .refine((value) => !value.startsWith("calluwu_"), "The calluwu_ field prefix is reserved");

export const IntakePrivacySchema = z.enum(["normal", "personal", "sensitive"]);

const fieldCommonShape = {
  id: IntakeFieldIdSchema,
  key: IntakeFieldKeySchema,
  label: boundedString(1, 120, "Field labels must contain 1 to 120 UTF-8 bytes"),
  question: boundedString(1, 500, "Field questions must contain 1 to 500 UTF-8 bytes"),
  description: boundedString(
    1,
    1_000,
    "Field descriptions must contain 1 to 1000 UTF-8 bytes",
  ).optional(),
  required: z.boolean(),
  privacy: IntakePrivacySchema,
} as const;

const ShortTextFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("short_text"),
  minLength: z.number().int().min(0).max(512).default(0),
  maxLength: z.number().int().min(1).max(512).default(256),
});

const LongTextFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("long_text"),
  minLength: z.number().int().min(0).max(4_096).default(0),
  maxLength: z.number().int().min(1).max(4_096).default(2_000),
});

const EmailFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("email"),
});

const PhoneFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("phone_e164"),
});

const IntegerFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("integer"),
  minimum: z.number().int().safe().optional(),
  maximum: z.number().int().safe().optional(),
});

const NumberFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("number"),
  minimum: z.number().finite().optional(),
  maximum: z.number().finite().optional(),
});

const BooleanFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("boolean"),
});

export const IntakeSelectOptionSchema = z.strictObject({
  value: z
    .string()
    .min(1)
    .max(80)
    .regex(
      /^[A-Za-z0-9][A-Za-z0-9._:-]*$/,
      "Option values must use portable identifier characters",
    ),
  label: boundedString(1, 120, "Option labels must contain 1 to 120 UTF-8 bytes"),
});

const SingleSelectFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("single_select"),
  options: z
    .array(IntakeSelectOptionSchema)
    .min(1)
    .max(EASY_INTAKE_MAX_SELECT_OPTIONS)
    .superRefine((options, context) => {
      const values = new Set<string>();
      options.forEach((option, index) => {
        if (values.has(option.value)) {
          context.addIssue({
            code: "custom",
            message: `Option value ${option.value} is declared more than once`,
            path: [index, "value"],
          });
        }
        values.add(option.value);
      });
    }),
});

const DateFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("date"),
});

const DateTimeFieldSchema = z.strictObject({
  ...fieldCommonShape,
  type: z.literal("datetime"),
});

export const IntakeFieldSchema = z
  .discriminatedUnion("type", [
    ShortTextFieldSchema,
    LongTextFieldSchema,
    EmailFieldSchema,
    PhoneFieldSchema,
    IntegerFieldSchema,
    NumberFieldSchema,
    BooleanFieldSchema,
    SingleSelectFieldSchema,
    DateFieldSchema,
    DateTimeFieldSchema,
  ])
  .superRefine((field, context) => {
    if (
      (field.type === "short_text" || field.type === "long_text") &&
      field.minLength > field.maxLength
    ) {
      context.addIssue({
        code: "custom",
        message: "minLength cannot exceed maxLength",
        path: ["minLength"],
      });
    }
    if (
      (field.type === "integer" || field.type === "number") &&
      field.minimum !== undefined &&
      field.maximum !== undefined &&
      field.minimum > field.maximum
    ) {
      context.addIssue({
        code: "custom",
        message: "minimum cannot exceed maximum",
        path: ["minimum"],
      });
    }
  });

export const IntakeFieldsSchema = z
  .array(IntakeFieldSchema)
  .min(1)
  .max(EASY_INTAKE_MAX_FIELDS)
  .superRefine((fields, context) => {
    const ids = new Set<string>();
    const keys = new Set<string>();
    fields.forEach((field, index) => {
      if (ids.has(field.id)) {
        context.addIssue({
          code: "custom",
          message: `Field ID ${field.id} is declared more than once`,
          path: [index, "id"],
        });
      }
      if (keys.has(field.key)) {
        context.addIssue({
          code: "custom",
          message: `Field key ${field.key} is declared more than once`,
          path: [index, "key"],
        });
      }
      ids.add(field.id);
      keys.add(field.key);
    });
  });

const destinationIdsSchema = z
  .array(IntakeDestinationIdSchema)
  .max(EASY_INTAKE_MAX_DESTINATIONS)
  .refine((values) => new Set(values).size === values.length, "Destinations must be unique")
  .default([]);

export const IntakeSchemaDefinitionSchema = z.strictObject({
  name: boundedString(1, 120, "Schema names must contain 1 to 120 UTF-8 bytes"),
  retentionDays: z
    .number()
    .int()
    .min(1)
    .max(EASY_INTAKE_MAX_RETENTION_DAYS)
    .default(EASY_INTAKE_DEFAULT_RETENTION_DAYS),
  fields: IntakeFieldsSchema,
  destinationIds: destinationIdsSchema,
});

const EnglishVoiceSchema = z.strictObject({
  locale: z.literal("en"),
  language: z.enum(["en", "en-US", "en-AU", "en-GB", "en-IN", "en-NZ"]).default("en-US"),
  speaker: z.enum(CLOUDFLARE_AURA_2_EN_SPEAKERS).default("luna"),
  sampleRateHz: z
    .union([
      z.literal(8_000),
      z.literal(16_000),
      z.literal(24_000),
      z.literal(32_000),
      z.literal(48_000),
    ])
    .default(16_000),
});

const SpanishVoiceSchema = z.strictObject({
  locale: z.literal("es"),
  language: z.enum(["es", "es-419"]).default("es"),
  speaker: z.enum(CLOUDFLARE_AURA_2_ES_SPEAKERS).default("celeste"),
  sampleRateHz: z
    .union([
      z.literal(8_000),
      z.literal(16_000),
      z.literal(24_000),
      z.literal(32_000),
      z.literal(48_000),
    ])
    .default(16_000),
});

export const EasyAgentVoiceSchema = z.discriminatedUnion("locale", [
  EnglishVoiceSchema,
  SpanishVoiceSchema,
]);

export const EasyAgentSpecSchema = z
  .strictObject({
    agentSlug: SlugSchema,
    purpose: boundedString(1, 2_000, "Purpose must contain 1 to 2000 UTF-8 bytes"),
    greeting: boundedString(1, 500, "Greeting must contain 1 to 500 UTF-8 bytes"),
    closing: boundedString(1, 500, "Closing must contain 1 to 500 UTF-8 bytes"),
    knowledge: boundedString(0, 16_000, "Knowledge must not exceed 16000 UTF-8 bytes").default(""),
    additionalInstructions: boundedString(
      0,
      8_000,
      "Additional instructions must not exceed 8000 UTF-8 bytes",
    ).default(""),
    voice: EasyAgentVoiceSchema,
    maxSessionSeconds: z.number().int().min(60).max(3_600).default(900),
    intake: IntakeSchemaDefinitionSchema,
  })
  .superRefine((spec, context) => {
    const authoredStrings = [
      spec.purpose,
      spec.greeting,
      spec.closing,
      spec.knowledge,
      spec.additionalInstructions,
      spec.intake.name,
      ...spec.intake.fields.flatMap((field) => [
        field.label,
        field.question,
        field.description ?? "",
        ...(field.type === "single_select"
          ? field.options.flatMap((option) => [option.value, option.label])
          : []),
      ]),
    ];
    const authoredBytes = authoredStrings.reduce(
      (total, value) => total + utf8ByteLength(value),
      0,
    );
    if (authoredBytes > EASY_AGENT_MAX_AUTHORED_BYTES) {
      context.addIssue({
        code: "custom",
        message: `Easy agent authored content exceeds ${EASY_AGENT_MAX_AUTHORED_BYTES} UTF-8 bytes`,
      });
    }
  });

export const IntakeScalarValueSchema = z.union([z.string(), z.number().finite(), z.boolean()]);

export const IntakeRecordValuesSchema = z
  .record(IntakeFieldKeySchema, IntakeScalarValueSchema)
  .superRefine((values, context) => {
    if (Object.keys(values).length > EASY_INTAKE_MAX_FIELDS) {
      context.addIssue({
        code: "custom",
        message: `Records cannot contain more than ${EASY_INTAKE_MAX_FIELDS} fields`,
      });
    }
  });

export const IntakeRecordStatusSchema = z.enum([
  "collecting",
  "validated",
  "finalized",
  "incomplete",
  "purged",
]);

export type IntakePrivacy = z.infer<typeof IntakePrivacySchema>;
export type IntakeField = z.infer<typeof IntakeFieldSchema>;
export type IntakeSelectOption = z.infer<typeof IntakeSelectOptionSchema>;
export type IntakeSchemaDefinition = z.infer<typeof IntakeSchemaDefinitionSchema>;
export type EasyAgentVoice = z.infer<typeof EasyAgentVoiceSchema>;
export type EasyAgentSpec = z.infer<typeof EasyAgentSpecSchema>;
export type IntakeScalarValue = z.infer<typeof IntakeScalarValueSchema>;
export type IntakeRecordValues = z.infer<typeof IntakeRecordValuesSchema>;
export type IntakeRecordStatus = z.infer<typeof IntakeRecordStatusSchema>;
