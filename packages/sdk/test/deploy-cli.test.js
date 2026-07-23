import { spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const packageDirectory = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const cliPath = resolve(packageDirectory, "dist/cli.js");
const sdkEntrypoint = resolve(packageDirectory, "src/index.ts");
const temporaryDirectories = [];

const cloudProviders = `
  providers: {
    speechToText: {
      provider: "cloudflare",
      model: "@cf/deepgram/nova-3",
      settings: { detectLanguage: false },
    },
    reasoning: {
      provider: "cloudflare",
      model: "@cf/openai/gpt-oss-20b",
      settings: { maxTokens: 256 },
    },
    textToSpeech: {
      provider: "cloudflare",
      model: "@cf/deepgram/aura-2-en",
      settings: { speaker: "luna" },
    },
  },
  voice: { id: "luna", language: "en-US", sampleRateHz: 16_000 },`;

async function writeAgent(source) {
  const directory = await mkdtemp(join(tmpdir(), "calluwu-deploy-cli-"));
  temporaryDirectories.push(directory);
  const agentPath = join(directory, "agent.ts");
  await writeFile(agentPath, source, "utf8");
  return agentPath;
}

function deployDryRun(agentPath) {
  return spawnSync(
    process.execPath,
    [cliPath, "deploy", agentPath, "--project", "prj_12345678", "--dry-run"],
    { cwd: packageDirectory, encoding: "utf8" },
  );
}

afterEach(async () => {
  await Promise.all(
    temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true })),
  );
});

describe("deploy CLI cloud tool admission", () => {
  it("allows HTTPS and registered builtin tools without applying the local artifact cap", async () => {
    const agentPath = await writeAgent(`
      import { Agent, builtinTool, httpTool } from ${JSON.stringify(sdkEntrypoint)};

      export default new Agent({
        name: "remote-tool-agent",
        instructions: ${JSON.stringify("Handle the call. ".repeat(3_500))},
        ${cloudProviders}
        tools: [
          httpTool({
            name: "lookup_customer",
            description: "Look up a customer",
            inputSchema: { type: "object" },
            timeoutMs: 1_000,
            sideEffect: "none",
            url: "https://tools.example.com/customer",
            secretRef: "int_customer_records",
          }),
          builtinTool({
            name: "slack_send_message",
            description: "Notify the support channel",
            inputSchema: { type: "object" },
            timeoutMs: 1_000,
            sideEffect: "idempotent",
            integration: "slack",
          }),
        ],
      });
    `);

    const result = deployDryRun(agentPath);

    expect(result.status, result.stderr).toBe(0);
    const output = JSON.parse(result.stdout);
    expect(output.dryRun).toBe(true);
    expect(output.manifest.artifact.sizeBytes).toBeGreaterThan(64 * 1024);
    expect(output.manifest.definition.tools.map((tool) => tool.execution.kind)).toEqual([
      "https",
      "builtin",
    ]);
  });

  it("rejects tools on the deterministic scripted provider path", async () => {
    const agentPath = await writeAgent(`
      import { Agent, httpTool, scriptedVoice } from ${JSON.stringify(sdkEntrypoint)};

      export default new Agent({
        name: "scripted-tool-agent",
        instructions: "Exercise deterministic runtime behavior.",
        ...scriptedVoice(),
        tools: [
          httpTool({
            name: "lookup_customer",
            description: "Look up a customer",
            inputSchema: { type: "object" },
            timeoutMs: 1_000,
            sideEffect: "none",
            url: "https://tools.example.com/customer",
          }),
        ],
      });
    `);

    const result = deployDryRun(agentPath);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("The scripted cloud provider path does not execute tools");
  });

  it("rejects local cloud handlers while the production isolation boundary is unavailable", async () => {
    const agentPath = await writeAgent(`
      import { Agent, defineTool } from ${JSON.stringify(sdkEntrypoint)};

      export default new Agent({
        name: "local-tool-agent",
        instructions: ${JSON.stringify("Handle the call. ".repeat(3_500))},
        ${cloudProviders}
        tools: [
          defineTool(
            {
              name: "lookup_customer",
              description: "Look up a customer",
              inputSchema: { type: "object" },
              timeoutMs: 1_000,
              sideEffect: "none",
            },
            async (input) => input,
          ),
        ],
      });
    `);

    const result = deployDryRun(agentPath);

    expect(result.status).toBe(1);
    expect(result.stderr).toContain("Local cloud tools are disabled");
  });
});
