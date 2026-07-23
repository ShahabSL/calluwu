#!/usr/bin/env node
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { resolve } from "node:path";
import { stdout } from "node:process";
import { type ApiKeyScope, ApiKeyScopeSchema } from "@calluwu/types";
import { Command } from "commander";
import { createManifest } from "./bundler.js";
import { CalluwuApiError, CalluwuClient } from "./client.js";
import { runLocalRealtimeServer, runScriptedSimulation } from "./runtime.js";

const program = new Command();

function apiClient(options: { apiUrl?: string }): CalluwuClient {
  const apiUrl = options.apiUrl ?? process.env.CALLUWU_API_URL;
  const apiKey = process.env.CALLUWU_API_KEY;
  if (!apiUrl || !apiKey) {
    throw new Error(
      "Set CALLUWU_API_URL and CALLUWU_API_KEY (API keys are not accepted on command lines)",
    );
  }
  return new CalluwuClient({ apiUrl, apiKey });
}

function printJson(value: unknown): void {
  stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function parseApiKeyScopes(value: string): ApiKeyScope[] {
  return ApiKeyScopeSchema.array()
    .min(1)
    .parse(value.split(",").map((scope) => scope.trim()));
}

function intentKey(prefix: string, value: unknown): string {
  return `${prefix}-${createHash("sha256").update(JSON.stringify(value)).digest("hex")}`;
}

program
  .name("calluwu")
  .description("Build, run, and deploy realtime voice agents")
  .version("0.1.0")
  .showSuggestionAfterError();

program
  .command("init")
  .description("Create a minimal Calluwu agent")
  .argument("[directory]", "target directory", ".")
  .action(async (directory: string) => {
    const target = resolve(directory);
    await mkdir(target, { recursive: true });
    const agentPath = resolve(target, "agent.ts");
    const packagePath = resolve(target, "package.json");
    await writeFile(
      agentPath,
      `import { Agent, cloudflareVoice } from "@calluwu/sdk";\n\nexport default new Agent({\n  name: "customer-support",\n  instructions: "You are a concise, helpful customer support agent.",\n  ...cloudflareVoice(),\n});\n`,
      { encoding: "utf8", flag: "wx" },
    );
    await writeFile(
      packagePath,
      `${JSON.stringify({ private: true, type: "module", dependencies: { "@calluwu/sdk": "^0.1.0" } }, null, 2)}\n`,
      { encoding: "utf8", flag: "wx" },
    );
    stdout.write(`Created ${agentPath}\n`);
  });

program
  .command("validate")
  .description("Bundle and validate an agent definition")
  .argument("<agent>", "agent TypeScript entrypoint")
  .action(async (entrypoint: string) => {
    const { manifest } = await createManifest(resolve(entrypoint));
    printJson({ valid: true, manifest });
  });

program
  .command("build")
  .description("Build a content-addressed deployment bundle")
  .argument("<agent>", "agent TypeScript entrypoint")
  .option("-o, --out <directory>", "output directory", "dist/calluwu")
  .action(async (entrypoint: string, options: { out: string }) => {
    const { bundle, manifest } = await createManifest(resolve(entrypoint));
    const out = resolve(options.out);
    await mkdir(out, { recursive: true });
    await Promise.all([
      writeFile(resolve(out, "agent.mjs"), bundle.source, "utf8"),
      writeFile(resolve(out, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, "utf8"),
    ]);
    printJson({ output: out, sha256: bundle.sha256, sizeBytes: bundle.sizeBytes });
  });

program
  .command("run")
  .description("Host a local realtime audio endpoint or run one deterministic text turn")
  .argument("<agent>", "agent TypeScript entrypoint")
  .option("--input <text>", "scripted caller utterance")
  .option("--events <path>", "write the semantic event log")
  .option("--bind <address>", "local realtime listen address", "127.0.0.1:0")
  .action(
    async (entrypoint: string, options: { input?: string; events?: string; bind: string }) => {
      const { agent, manifest } = await createManifest(resolve(entrypoint));
      if (agent.definition.tools.length > 0) {
        throw new Error(
          "Local tool adapters are not enabled in this foundation; validation never fabricates tool success",
        );
      }
      if (options.events && options.input === undefined) {
        throw new Error("--events is available only with --input simulation mode");
      }
      const exitCode =
        options.input === undefined
          ? await runLocalRealtimeServer(manifest, { bind: options.bind })
          : await runScriptedSimulation(manifest, options.input, {
              ...(options.events ? { eventsPath: resolve(options.events) } : {}),
            });
      if (exitCode !== 0) process.exitCode = exitCode;
    },
  );

program
  .command("deploy")
  .description("Create an immutable cloud deployment")
  .argument("<agent>", "agent TypeScript entrypoint")
  .requiredOption("--project <id>", "project ID")
  .option("--environment <name>", "development, staging, or production", "development")
  .option("--api-url <url>", "Calluwu API URL")
  .option("--activate", "activate the immutable deployment after upload", false)
  .option("--dry-run", "validate and build without uploading", false)
  .action(
    async (
      entrypoint: string,
      options: {
        project: string;
        environment: "development" | "staging" | "production";
        apiUrl?: string;
        activate: boolean;
        dryRun: boolean;
      },
    ) => {
      const { agent, bundle, manifest } = await createManifest(resolve(entrypoint));
      const tools = agent.definition.tools;
      const usesScriptedProviderPath = Object.values(agent.definition.providers).every(
        (provider) =>
          provider.provider === "scripted" &&
          provider.model === "scripted-v1" &&
          Object.keys(provider.settings).length === 0,
      );
      if (usesScriptedProviderPath && tools.length > 0) {
        throw new Error("The scripted cloud provider path does not execute tools");
      }
      if (usesScriptedProviderPath) {
        throw new Error(
          "The scripted provider is local-only; use cloudflareVoice() before deploying to Calluwu Cloud",
        );
      }
      if (tools.some((tool) => tool.execution.kind === "local")) {
        throw new Error(
          "Local cloud tools are disabled until Calluwu can enforce an isolated control plane",
        );
      }
      if (options.dryRun) {
        printJson({ dryRun: true, activate: options.activate, manifest });
        return;
      }
      const client = apiClient(options);
      const deployment = await client.createDeployment(
        options.project,
        {
          agentSlug: agent.definition.name,
          agentName: agent.definition.name,
          environment: options.environment,
          definition: agent.definition,
          artifact: {
            sha256: bundle.sha256,
            sizeBytes: bundle.sizeBytes,
            base64: Buffer.from(bundle.source).toString("base64"),
          },
        },
        `deploy-${bundle.sha256}-${options.environment}`,
      );
      const result = options.activate
        ? await client.activateDeployment(
            options.project,
            deployment.id,
            `activate-${deployment.id}`,
          )
        : deployment;
      printJson({ deployment: result, activated: options.activate });
    },
  );

const projects = program.command("projects").description("Inspect projects");
projects
  .command("list")
  .option("--api-url <url>")
  .action(async (options: { apiUrl?: string }) => {
    printJson({ projects: await apiClient(options).listProjects() });
  });
projects
  .command("create")
  .requiredOption("--slug <slug>", "stable project slug")
  .requiredOption("--name <name>", "operator-visible project name")
  .option("--idempotency-key <key>", "stable project creation intent key")
  .option("--api-url <url>")
  .action(
    async (options: { slug: string; name: string; idempotencyKey?: string; apiUrl?: string }) => {
      const input = { slug: options.slug, name: options.name };
      printJson({
        project: await apiClient(options).createProject(
          input,
          options.idempotencyKey ?? intentKey("project-create", input),
        ),
      });
    },
  );

const deployments = program.command("deployments").description("Inspect deployments");
deployments
  .command("list")
  .requiredOption("--project <id>")
  .option("--api-url <url>")
  .action(async (options: { project: string; apiUrl?: string }) => {
    printJson({ deployments: await apiClient(options).listDeployments(options.project) });
  });
deployments
  .command("activate")
  .argument("<deployment>", "deployment ID")
  .requiredOption("--project <id>", "project ID")
  .option("--idempotency-key <key>", "stable activation intent key")
  .option("--api-url <url>")
  .action(
    async (
      deployment: string,
      options: { project: string; idempotencyKey?: string; apiUrl?: string },
    ) => {
      printJson({
        deployment: await apiClient(options).activateDeployment(
          options.project,
          deployment,
          options.idempotencyKey ?? `activate-${deployment}`,
        ),
      });
    },
  );

const sessions = program.command("sessions").description("Inspect sessions");
sessions
  .command("list")
  .requiredOption("--project <id>")
  .option("--api-url <url>")
  .action(async (options: { project: string; apiUrl?: string }) => {
    printJson({ sessions: await apiClient(options).listSessions(options.project) });
  });
sessions
  .command("events")
  .argument("<session>")
  .option("--api-url <url>")
  .action(async (session: string, options: { apiUrl?: string }) => {
    printJson(await apiClient(options).listSessionEvents(session));
  });
sessions
  .command("cancel")
  .argument("<session>", "session ID")
  .option("--api-url <url>")
  .action(async (session: string, options: { apiUrl?: string }) => {
    printJson({ session: await apiClient(options).cancelSession(session) });
  });

const apiKeys = program.command("api-keys").description("Manage scoped API keys");
apiKeys
  .command("list")
  .option("--limit <count>", "maximum keys", "100")
  .option("--api-url <url>")
  .action(async (options: { limit: string; apiUrl?: string }) => {
    printJson({ apiKeys: await apiClient(options).listApiKeys(Number(options.limit)) });
  });
apiKeys
  .command("get")
  .argument("<key>", "API key ID")
  .option("--api-url <url>")
  .action(async (key: string, options: { apiUrl?: string }) => {
    printJson({ apiKey: await apiClient(options).getApiKey(key) });
  });
apiKeys
  .command("create")
  .requiredOption("--name <name>", "operator-visible key name")
  .requiredOption("--scopes <scopes>", "comma-separated least-privilege scopes")
  .option("--project <id>", "restrict the key to one project")
  .requiredOption(
    "--expires-at <timestamp>",
    "RFC 3339 expiration between five minutes and 90 days in the future",
  )
  .option("--idempotency-key <key>", "stable key creation intent key")
  .option("--api-url <url>")
  .action(
    async (options: {
      name: string;
      scopes: string;
      project?: string;
      expiresAt: string;
      idempotencyKey?: string;
      apiUrl?: string;
    }) => {
      const input = {
        name: options.name,
        scopes: parseApiKeyScopes(options.scopes),
        ...(options.project ? { projectId: options.project } : {}),
        expiresAt: options.expiresAt,
      };
      printJson(
        await apiClient(options).createApiKey(
          input,
          options.idempotencyKey ?? intentKey("api-key-create", input),
        ),
      );
    },
  );
apiKeys
  .command("rotate")
  .argument("<key>", "API key ID")
  .requiredOption(
    "--expires-at <timestamp>",
    "replacement expiration between five minutes and 90 days in the future",
  )
  .option("--idempotency-key <key>", "stable key rotation intent key")
  .option("--api-url <url>")
  .action(
    async (
      key: string,
      options: { expiresAt: string; idempotencyKey?: string; apiUrl?: string },
    ) => {
      const input = { expiresAt: options.expiresAt };
      printJson(
        await apiClient(options).rotateApiKey(
          key,
          input,
          options.idempotencyKey ?? intentKey("api-key-rotate", { key, ...input }),
        ),
      );
    },
  );
apiKeys
  .command("revoke")
  .argument("<key>", "API key ID")
  .option("--api-url <url>")
  .action(async (key: string, options: { apiUrl?: string }) => {
    await apiClient(options).revokeApiKey(key);
    printJson({ revoked: true, keyId: key });
  });

program
  .command("whoami")
  .description("Verify API connectivity and credentials")
  .option("--api-url <url>")
  .action(async (options: { apiUrl?: string }) => {
    const client = apiClient(options);
    const projects = await client.listProjects();
    printJson({ authenticated: true, visibleProjects: projects.length });
  });

program.parseAsync().catch((error: unknown) => {
  if (error instanceof CalluwuApiError) {
    process.stderr.write(
      `${JSON.stringify({ error: error.code, message: error.message, requestId: error.requestId })}\n`,
    );
  } else if (error instanceof Error) {
    process.stderr.write(`${error.name}: ${error.message}\n`);
  } else {
    process.stderr.write("Unknown error\n");
  }
  process.exitCode = 1;
});
