import { describe, expect, it } from "vitest";
import {
  AgentDefinitionSchema,
  AgentManifestSchema,
  ApiKeyCredentialResponseSchema,
  ApiKeyRotationResponseSchema,
  CONTRACT_VERSION,
  CreateApiKeyRequestSchema,
  decodeRealtimeAudioFrame,
  encodeRealtimeAudioFrame,
  RealtimeClientMessageSchema,
  resourceId,
  SessionResponseSchema,
} from "../src/index.js";
import unicodeBoundaries from "./fixtures/unicode-boundaries.json" with { type: "json" };

describe("shared contracts", () => {
  it("models API-key credentials as one-time discriminated responses", () => {
    const now = new Date().toISOString();
    const apiKey = {
      id: "key_contract-test",
      organizationId: "org_contract-test",
      projectId: null,
      name: "project-owner",
      scopes: ["api-keys:read", "api-keys:write"],
      status: "active",
      createdAt: now,
      expiresAt: new Date(Date.now() + 86_400_000).toISOString(),
      lastUsedAt: null,
      revokedAt: null,
      rotatedFromKeyId: null,
    };

    expect(
      ApiKeyCredentialResponseSchema.safeParse({
        apiKey,
        credentialAlreadyIssued: false,
        credential: "key_contract-test.this-is-a-long-random-secret",
      }).success,
    ).toBe(true);
    expect(
      ApiKeyCredentialResponseSchema.safeParse({
        apiKey,
        credentialAlreadyIssued: true,
        credential: "must-never-be-returned-on-replay",
      }).success,
    ).toBe(false);
    expect(
      ApiKeyRotationResponseSchema.safeParse({
        apiKey: { ...apiKey, id: "key_replacement", rotatedFromKeyId: apiKey.id },
        credentialAlreadyIssued: true,
        rotation: { previousKeyId: apiKey.id, requiresPreviousKeyRevocation: true },
      }).success,
    ).toBe(true);
    expect(
      CreateApiKeyRequestSchema.safeParse({
        name: "project-owner",
        scopes: ["api-keys:read"],
      }).success,
    ).toBe(false);
  });

  it("accepts the deterministic local agent", () => {
    const definition = AgentDefinitionSchema.parse({
      name: "customer-support",
      instructions: "Help the caller.",
      providers: {
        speechToText: { provider: "scripted", model: "scripted-v1" },
        reasoning: { provider: "scripted", model: "scripted-v1" },
        textToSpeech: { provider: "scripted", model: "scripted-v1" },
      },
      voice: { id: "neutral" },
    });

    expect(definition.name).toBe("customer-support");
    expect(CONTRACT_VERSION).toBe("2026-07-19");
  });

  it("rejects stale realtime messages without a generation fence", () => {
    const result = RealtimeClientMessageSchema.safeParse({
      type: "session.start",
      protocolVersion: 1,
      sessionId: resourceId("ses", crypto.randomUUID()),
      messageId: "message-1",
    });

    expect(result.success).toBe(false);
  });

  it("rejects unknown fields at manifest and realtime wire boundaries", () => {
    const unknown = unicodeBoundaries.unknownWireField;
    expect(
      RealtimeClientMessageSchema.safeParse({
        type: "session.start",
        protocolVersion: 1,
        sessionId: "ses_12345678",
        messageId: "message-1",
        runtimeGeneration: 1,
        [unknown.name]: unknown.value,
      }).success,
    ).toBe(false);
    expect(
      AgentManifestSchema.safeParse({
        contractVersion: CONTRACT_VERSION,
        definition: {
          name: "support-agent",
          instructions: "Help the caller.",
          providers: {
            speechToText: { provider: "scripted", model: "scripted-v1" },
            reasoning: { provider: "scripted", model: "scripted-v1" },
            textToSpeech: { provider: "scripted", model: "scripted-v1" },
          },
          voice: { id: "test" },
        },
        requiredCapabilities: ["batch-stt", "streaming-reasoning", "streaming-tts"],
        artifact: { sha256: "0".repeat(64), sizeBytes: 1, format: "javascript-esm" },
        [unknown.name]: unknown.value,
      }).success,
    ).toBe(false);
  });

  it("never accepts realtime credentials on an insecure non-loopback socket URL", () => {
    const session = {
      id: "ses_12345678",
      organizationId: "org_12345678",
      projectId: "prj_12345678",
      deploymentId: "dep_12345678",
      status: "ready" as const,
      runtimeShardId: "runtime-0",
      runtimeGeneration: 1,
      createdAt: "2026-07-19T12:00:00.000Z",
      startedAt: null,
      endedAt: null,
    };
    const realtime = {
      token: "ticket-value-long-enough",
      expiresAt: "2026-07-19T12:01:00.000Z",
      sampleRateHz: 16_000,
    };

    expect(
      SessionResponseSchema.safeParse({
        session,
        realtime: { ...realtime, url: "ws://127.0.0.1:8787/v1/sessions/ses_12345678/realtime" },
      }).success,
    ).toBe(true);
    expect(
      SessionResponseSchema.safeParse({
        session,
        realtime: { ...realtime, url: "ws://attacker.example/realtime" },
      }).success,
    ).toBe(false);
    expect(
      SessionResponseSchema.safeParse({
        session,
        realtime: { ...realtime, url: "wss://attacker.example/realtime?ticket=leak" },
      }).success,
    ).toBe(false);
  });

  it("enforces realtime wire limits in UTF-8 bytes", () => {
    const sessionId = resourceId("ses", crypto.randomUUID());
    const base = {
      type: "input.text" as const,
      protocolVersion: 1 as const,
      sessionId,
      runtimeGeneration: 1,
    };
    const messageId = unicodeBoundaries.realtimeMessageId;
    const inputText = unicodeBoundaries.realtimeInputText;
    const sessionEndReason = unicodeBoundaries.realtimeSessionEndReason;

    expect(
      RealtimeClientMessageSchema.safeParse({
        ...base,
        messageId: messageId.scalar.repeat(messageId.validRepeat),
        text: inputText.scalar.repeat(inputText.validRepeat),
      }).success,
    ).toBe(true);
    expect(
      RealtimeClientMessageSchema.safeParse({
        ...base,
        messageId: messageId.scalar.repeat(messageId.invalidRepeat),
        text: "hello",
      }).success,
    ).toBe(false);
    expect(
      RealtimeClientMessageSchema.safeParse({
        ...base,
        messageId: "message-1",
        text: inputText.scalar.repeat(inputText.invalidRepeat),
      }).success,
    ).toBe(false);
    expect(
      RealtimeClientMessageSchema.safeParse({
        ...base,
        type: "session.end",
        messageId: "message-2",
        reason: sessionEndReason.scalar.repeat(sessionEndReason.validRepeat),
      }).success,
    ).toBe(true);
    expect(
      RealtimeClientMessageSchema.safeParse({
        ...base,
        type: "session.end",
        messageId: "message-3",
        reason: sessionEndReason.scalar.repeat(sessionEndReason.invalidRepeat),
      }).success,
    ).toBe(false);
  });

  it("round-trips bounded binary PCM audio without base64", () => {
    const header = {
      type: "audio.chunk" as const,
      protocolVersion: 1 as const,
      sessionId: resourceId("ses", crypto.randomUUID()),
      messageId: "audio-1",
      runtimeGeneration: 3,
      responseId: "response-1",
      epoch: 2,
      sequence: 7,
      encoding: "pcm16le" as const,
      sampleRateHz: 16_000,
      channels: 1 as const,
    };
    const audio = new Uint8Array([0, 1, 2, 3]);

    const decoded = decodeRealtimeAudioFrame(encodeRealtimeAudioFrame(header, audio));

    expect(decoded.header).toEqual(header);
    expect([...decoded.audio]).toEqual([...audio]);
  });

  it("rejects malformed binary audio framing", () => {
    expect(() => decodeRealtimeAudioFrame(new Uint8Array([1, 2, 3, 4]))).toThrow(
      "invalid CWU1 prefix",
    );
    expect(() =>
      encodeRealtimeAudioFrame(
        {
          type: "audio.chunk",
          protocolVersion: 1,
          sessionId: "ses_12345678",
          messageId: "audio-1",
          runtimeGeneration: 1,
          responseId: "response-1",
          epoch: 1,
          sequence: 0,
          encoding: "pcm16le",
          sampleRateHz: 16_000,
          channels: 1,
        },
        new Uint8Array([1]),
      ),
    ).toThrow("PCM16LE");
  });

  it("does not permit manifests to omit engine capabilities", () => {
    const result = AgentManifestSchema.safeParse({
      contractVersion: CONTRACT_VERSION,
      definition: {
        name: "support-agent",
        instructions: "Help the caller.",
        providers: {
          speechToText: { provider: "scripted", model: "test" },
          reasoning: { provider: "scripted", model: "test" },
          textToSpeech: { provider: "scripted", model: "test" },
        },
        voice: { id: "test" },
      },
      requiredCapabilities: [],
      artifact: { sha256: "0".repeat(64), sizeBytes: 1, format: "javascript-esm" },
    });

    expect(result.success).toBe(false);
  });

  it("enforces manifest string limits in UTF-8 bytes", () => {
    const boundary = unicodeBoundaries.providerModel;
    const base = {
      name: "support-agent",
      instructions: "Help the caller.",
      providers: {
        speechToText: { provider: "scripted", model: boundary.scalar.repeat(boundary.validRepeat) },
        reasoning: { provider: "scripted", model: "scripted-v1" },
        textToSpeech: { provider: "scripted", model: "scripted-v1" },
      },
      voice: { id: "test" },
    };

    expect(AgentDefinitionSchema.safeParse(base).success).toBe(true);
    expect(
      AgentDefinitionSchema.safeParse({
        ...base,
        providers: {
          ...base.providers,
          speechToText: {
            provider: "scripted",
            model: boundary.scalar.repeat(boundary.invalidRepeat),
          },
        },
      }).success,
    ).toBe(false);
  });

  it("rejects duplicate tool names before runtime map construction", () => {
    const duplicate = {
      name: "lookup",
      description: "Lookup a record",
      inputSchema: { type: "object" },
      timeoutMs: 1_000,
      sideEffect: "none" as const,
      execution: { kind: "local" as const },
    };
    const result = AgentDefinitionSchema.safeParse({
      name: "support-agent",
      instructions: "Help the caller.",
      providers: {
        speechToText: { provider: "scripted", model: "scripted-v1" },
        reasoning: { provider: "scripted", model: "scripted-v1" },
        textToSpeech: { provider: "scripted", model: "scripted-v1" },
      },
      voice: { id: "test" },
      tools: [duplicate, duplicate],
    });

    expect(result.success).toBe(false);
  });
});
