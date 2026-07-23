import { resolve } from "node:path";
import { CONTRACT_VERSION } from "@calluwu/types";
import { describe, expect, it } from "vitest";
import { createManifest } from "../src/bundler.js";

describe("agent bundler", () => {
  it("creates a deterministic, validated manifest", async () => {
    const first = await createManifest(resolve("test/fixture-agent.ts"));
    const second = await createManifest(resolve("test/fixture-agent.ts"));

    expect(first.manifest.contractVersion).toBe(CONTRACT_VERSION);
    expect(first.manifest.definition.name).toBe("fixture-agent");
    expect(first.manifest.requiredCapabilities).toEqual([
      "batch-stt",
      "streaming-reasoning",
      "streaming-tts",
    ]);
    expect(first.bundle.sha256).toBe(second.bundle.sha256);
    expect(first.manifest.artifact.sha256).toBe(first.bundle.sha256);
  });
});
