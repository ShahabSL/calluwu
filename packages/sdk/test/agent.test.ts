import { describe, expect, it } from "vitest";
import { Agent } from "../src/agent.js";
import { cloudflareVoice, scriptedVoice } from "../src/presets.js";
import { defineTool, httpTool } from "../src/tool.js";

describe("Agent", () => {
  it("supports the concise authoring experience", () => {
    const agent = new Agent({
      name: "customer-support",
      instructions: "Be helpful.",
      tools: [
        httpTool({
          name: "lookup_customer",
          description: "Find a customer",
          inputSchema: { type: "object" },
          timeoutMs: 1_000,
          sideEffect: "none",
          url: "https://tools.example.com/customer",
        }),
      ],
    });

    expect(agent.definition.name).toBe("customer-support");
    expect(agent.definition.providers.reasoning).toMatchObject({
      provider: "cloudflare",
      model: "@cf/openai/gpt-oss-20b",
    });
    expect(agent.definition.voice.id).toBe("luna");
    expect(agent.definition.voice.sampleRateHz).toBe(16_000);
    expect(agent.definition.tools[0]?.execution.kind).toBe("https");
  });

  it("offers exact installed Cloudflare and deterministic local presets", () => {
    const spanish = cloudflareVoice({ locale: "es", speaker: "celeste", language: "es-419" });
    expect(spanish.providers.textToSpeech).toEqual({
      provider: "cloudflare",
      model: "@cf/deepgram/aura-2-es",
      settings: { speaker: "celeste" },
    });
    expect(spanish.voice).toEqual({ id: "celeste", language: "es-419", sampleRateHz: 16_000 });
    expect(scriptedVoice().providers.reasoning).toEqual({
      provider: "scripted",
      model: "scripted-v1",
      settings: {},
    });
  });

  it("keeps local handlers out of the serializable manifest", () => {
    const tool = defineTool(
      {
        name: "echo",
        description: "Echo input",
        inputSchema: { type: "object" },
        timeoutMs: 1_000,
        sideEffect: "none",
      },
      async (input) => input,
    );
    const agent = new Agent({
      name: "echo-agent",
      instructions: "Echo.",
      ...scriptedVoice(),
      tools: [tool],
    });

    const serializedTool = agent.definition.tools[0];
    expect(serializedTool?.execution).toEqual({ kind: "local" });
    if (serializedTool === undefined) throw new Error("Expected one serialized tool definition");
    expect("handler" in serializedTool).toBe(false);
  });
});
