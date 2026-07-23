import type {
  AgentDefinition,
  CLOUDFLARE_AURA_2_EN_SPEAKERS,
  CLOUDFLARE_AURA_2_ES_SPEAKERS,
  CLOUDFLARE_NOVA_3_LANGUAGES,
} from "@calluwu/types";

export type VoicePreset = Readonly<Pick<AgentDefinition, "providers" | "voice">>;

type CloudflareLanguage = (typeof CLOUDFLARE_NOVA_3_LANGUAGES)[number];
type CloudflareSampleRate = 8_000 | 16_000 | 24_000 | 32_000 | 48_000;

interface CloudflareVoiceOptionsBase {
  /** Nova-3 transcription language. */
  language?: CloudflareLanguage;
  /** Linear16 input/output sample rate. */
  sampleRateHz?: CloudflareSampleRate;
  /** Detect the spoken language instead of pinning it. */
  detectLanguage?: boolean;
  /** Optional Nova-3 key terms. */
  keyterm?: string;
  /** Keep model-improvement opt-out enabled unless explicitly disabled. */
  mipOptOut?: boolean;
  /** Maximum reasoning output tokens for one turn. */
  maxTokens?: number;
  temperature?: number;
  topP?: number;
}

export type CloudflareVoiceOptions =
  | (CloudflareVoiceOptionsBase & {
      locale?: "en";
      speaker?: (typeof CLOUDFLARE_AURA_2_EN_SPEAKERS)[number];
    })
  | (CloudflareVoiceOptionsBase & {
      locale: "es";
      speaker?: (typeof CLOUDFLARE_AURA_2_ES_SPEAKERS)[number];
    });

/**
 * The installed Calluwu Cloud voice pipeline.
 *
 * Spreading this preset into an Agent keeps the provider/model contract explicit while avoiding
 * hand-written identifiers that can drift from the adapters installed by Calluwu Cloud.
 */
export function cloudflareVoice(options: CloudflareVoiceOptions = {}): VoicePreset {
  const locale = options.locale ?? "en";
  const speaker = options.speaker ?? (locale === "es" ? "celeste" : "luna");
  const speechToTextSettings = {
    detectLanguage: options.detectLanguage ?? false,
    mipOptOut: options.mipOptOut ?? true,
    ...(options.keyterm === undefined ? {} : { keyterm: options.keyterm }),
  };
  const reasoningSettings = {
    maxTokens: options.maxTokens ?? 512,
    temperature: options.temperature ?? 0.3,
    topP: options.topP ?? 0.9,
  };

  return {
    providers: {
      speechToText: {
        provider: "cloudflare",
        model: "@cf/deepgram/nova-3",
        settings: speechToTextSettings,
      },
      reasoning: {
        provider: "cloudflare",
        model: "@cf/openai/gpt-oss-20b",
        settings: reasoningSettings,
      },
      textToSpeech: {
        provider: "cloudflare",
        model: locale === "es" ? "@cf/deepgram/aura-2-es" : "@cf/deepgram/aura-2-en",
        settings: { speaker },
      },
    },
    voice: {
      id: speaker,
      language: options.language ?? (locale === "es" ? "es" : "en-US"),
      sampleRateHz: options.sampleRateHz ?? 16_000,
    },
  };
}

export interface ScriptedVoiceOptions {
  language?: string;
  sampleRateHz?: number;
}

/** Deterministic, credential-free provider used for local simulation and contract tests. */
export function scriptedVoice(options: ScriptedVoiceOptions = {}): VoicePreset {
  return {
    providers: {
      speechToText: { provider: "scripted", model: "scripted-v1", settings: {} },
      reasoning: { provider: "scripted", model: "scripted-v1", settings: {} },
      textToSpeech: { provider: "scripted", model: "scripted-v1", settings: {} },
    },
    voice: {
      id: "scripted-default",
      language: options.language ?? "en-US",
      sampleRateHz: options.sampleRateHz ?? 16_000,
    },
  };
}
