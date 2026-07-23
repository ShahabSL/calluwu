import { access, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { AgentManifestSchema, CONTRACT_VERSION } from "@calluwu/types";
import { afterEach, describe, expect, it, vi } from "vitest";
import { runLocalRealtimeServer, runScriptedSimulation } from "../src/runtime.js";

const manifest = AgentManifestSchema.parse({
  contractVersion: CONTRACT_VERSION,
  definition: {
    name: "runtime-fixture",
    instructions: "Answer deterministically.",
    providers: {
      speechToText: { provider: "scripted", model: "scripted-v1" },
      reasoning: { provider: "scripted", model: "scripted-v1" },
      textToSpeech: { provider: "scripted", model: "scripted-v1" },
    },
    voice: { id: "scripted-default" },
  },
  requiredCapabilities: ["batch-stt", "streaming-reasoning", "streaming-tts"],
  artifact: { sha256: "0".repeat(64), sizeBytes: 1, format: "javascript-esm" },
});

afterEach(() => vi.unstubAllEnvs());

describe("runtime invocation", () => {
  it("passes a private temporary manifest to local realtime serve", async () => {
    const directory = await mkdtemp(join(tmpdir(), "calluwu-sdk-test-"));
    const executable = join(directory, "runtime-fixture");
    const capture = join(directory, "args.txt");
    await writeFile(
      executable,
      '#!/bin/sh\nprintf \'%s\\n\' "$@" > "$CALLUWU_RUNTIME_CAPTURE"\ntest -f "$7"\n',
      { encoding: "utf8", mode: 0o700 },
    );
    vi.stubEnv("CALLUWU_RUNTIME_BIN", executable);
    vi.stubEnv("CALLUWU_RUNTIME_CAPTURE", capture);

    try {
      await expect(runLocalRealtimeServer(manifest, { bind: "127.0.0.1:4321" })).resolves.toBe(0);
      const args = (await readFile(capture, "utf8")).trim().split("\n");
      expect(args.slice(0, 6)).toEqual([
        "serve",
        "--bind",
        "127.0.0.1:4321",
        "--max-sessions",
        "1",
        "--agent-manifest",
      ]);
      await expect(access(args[6] ?? "")).rejects.toThrow();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });

  it("passes scripted input and optional events to simulation", async () => {
    const directory = await mkdtemp(join(tmpdir(), "calluwu-sdk-test-"));
    const executable = join(directory, "runtime-fixture");
    const capture = join(directory, "args.txt");
    await writeFile(
      executable,
      '#!/bin/sh\nprintf \'%s\\n\' "$@" > "$CALLUWU_RUNTIME_CAPTURE"\ntest -f "$3"\n',
      { encoding: "utf8", mode: 0o700 },
    );
    vi.stubEnv("CALLUWU_RUNTIME_BIN", executable);
    vi.stubEnv("CALLUWU_RUNTIME_CAPTURE", capture);

    try {
      await expect(
        runScriptedSimulation(manifest, "hello", { eventsPath: join(directory, "events.ndjson") }),
      ).resolves.toBe(0);
      const args = (await readFile(capture, "utf8")).trim().split("\n");
      expect(args[0]).toBe("simulate");
      expect(args.slice(3)).toEqual([
        "--input",
        "hello",
        "--events",
        join(directory, "events.ndjson"),
      ]);
      await expect(access(args[2] ?? "")).rejects.toThrow();
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
});
