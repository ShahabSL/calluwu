import { describe, expect, it } from "vitest";
import {
  EASY_AGENT_MAX_AUTHORED_BYTES,
  EASY_INTAKE_MAX_FIELDS,
  EasyAgentSpecSchema,
  IntakeFieldsSchema,
  IntakeRecordValuesSchema,
} from "../src/index.js";

function field(index: number) {
  return {
    id: `fld_contract-${index}`,
    key: `field_${index}`,
    label: `Field ${index}`,
    question: `What is field ${index}?`,
    type: "short_text" as const,
    required: index === 0,
    privacy: index === 0 ? ("personal" as const) : ("normal" as const),
  };
}

function easySpec() {
  return {
    agentSlug: "lead-qualifier",
    purpose: "Qualify an inbound sales lead.",
    greeting: "Hello, I can help with your request.",
    closing: "Thank you. We will follow up soon.",
    voice: { locale: "en" as const },
    intake: {
      name: "Sales lead",
      fields: [
        {
          ...field(0),
          key: "email",
          label: "Email",
          question: "What email should we use?",
          type: "email" as const,
        },
        {
          ...field(1),
          key: "company_size",
          label: "Company size",
          type: "single_select" as const,
          options: [
            { value: "small", label: "1–49" },
            { value: "medium", label: "50–499" },
          ],
        },
      ],
    },
  };
}

describe("Easy intake contracts", () => {
  it("applies secure defaults and rejects unknown configuration", () => {
    const parsed = EasyAgentSpecSchema.parse(easySpec());
    expect(parsed).toMatchObject({
      knowledge: "",
      additionalInstructions: "",
      maxSessionSeconds: 900,
      voice: { language: "en-US", speaker: "luna", sampleRateHz: 16_000 },
      intake: { retentionDays: 30, destinationIds: [] },
    });
    expect(
      EasyAgentSpecSchema.safeParse({ ...easySpec(), arbitraryCode: "export default true" })
        .success,
    ).toBe(false);
  });

  it("enforces stable unique field identities and the field-count ceiling", () => {
    expect(
      IntakeFieldsSchema.safeParse([field(0), { ...field(1), key: field(0).key }]).success,
    ).toBe(false);
    expect(IntakeFieldsSchema.safeParse([field(0), { ...field(1), id: field(0).id }]).success).toBe(
      false,
    );
    expect(
      IntakeFieldsSchema.safeParse(
        Array.from({ length: EASY_INTAKE_MAX_FIELDS + 1 }, (_, index) => field(index)),
      ).success,
    ).toBe(false);
  });

  it("rejects reserved keys, invalid ranges, and duplicate select values", () => {
    expect(IntakeFieldsSchema.safeParse([{ ...field(0), key: "calluwu_internal" }]).success).toBe(
      false,
    );
    expect(
      IntakeFieldsSchema.safeParse([{ ...field(0), minLength: 20, maxLength: 10 }]).success,
    ).toBe(false);
    expect(
      IntakeFieldsSchema.safeParse([
        {
          ...field(0),
          type: "single_select",
          options: [
            { value: "same", label: "First" },
            { value: "same", label: "Second" },
          ],
        },
      ]).success,
    ).toBe(false);
  });

  it("bounds aggregate authored content before compiler expansion", () => {
    const candidate = {
      ...easySpec(),
      intake: {
        ...easySpec().intake,
        fields: [
          {
            ...field(0),
            description: "x".repeat(EASY_AGENT_MAX_AUTHORED_BYTES),
          },
        ],
      },
    };
    expect(EasyAgentSpecSchema.safeParse(candidate).success).toBe(false);
  });

  it("accepts only bounded scalar records with public field keys", () => {
    expect(
      IntakeRecordValuesSchema.parse({
        email: "caller@example.com",
        company_size: "small",
        qualified: true,
        employees: 42,
      }),
    ).toEqual({
      email: "caller@example.com",
      company_size: "small",
      qualified: true,
      employees: 42,
    });
    expect(IntakeRecordValuesSchema.safeParse({ calluwu_secret: "no" }).success).toBe(false);
    expect(IntakeRecordValuesSchema.safeParse({ nested: { value: "no" } }).success).toBe(false);
    expect(IntakeRecordValuesSchema.safeParse({ invalid: Number.POSITIVE_INFINITY }).success).toBe(
      false,
    );
  });
});
