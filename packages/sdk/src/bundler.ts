import { createHash } from "node:crypto";
import { pathToFileURL } from "node:url";
import {
  type AgentManifest,
  AgentManifestSchema,
  CONTRACT_VERSION,
  requiredCapabilitiesForAgent,
} from "@calluwu/types";
import { build } from "esbuild";
import { type Agent, isAgent } from "./agent.js";

export type AgentBundle = {
  source: string;
  sha256: string;
  sizeBytes: number;
};

export async function bundleAgent(entrypoint: string): Promise<AgentBundle> {
  const result = await build({
    absWorkingDir: process.cwd(),
    bundle: true,
    entryPoints: [entrypoint],
    format: "esm",
    legalComments: "none",
    logLevel: "silent",
    minify: false,
    platform: "node",
    sourcemap: false,
    target: "node22",
    treeShaking: true,
    write: false,
  });
  const output = result.outputFiles[0];
  if (output === undefined) {
    throw new Error("Agent bundling produced no output");
  }

  const source = output.text;
  return {
    source,
    sha256: createHash("sha256").update(source).digest("hex"),
    sizeBytes: Buffer.byteLength(source),
  };
}

export async function loadAgent(entrypoint: string, bundle?: AgentBundle): Promise<Agent> {
  const built = bundle ?? (await bundleAgent(entrypoint));
  const moduleUrl = `data:text/javascript;base64,${Buffer.from(built.source).toString("base64")}`;
  const module = (await import(moduleUrl)) as { default?: unknown };
  if (!isAgent(module.default)) {
    throw new TypeError(
      `${pathToFileURL(entrypoint).href} must default-export an Agent from @calluwu/sdk`,
    );
  }
  return module.default;
}

export async function createManifest(entrypoint: string): Promise<{
  agent: Agent;
  bundle: AgentBundle;
  manifest: AgentManifest;
}> {
  const bundle = await bundleAgent(entrypoint);
  const agent = await loadAgent(entrypoint, bundle);
  const manifest = AgentManifestSchema.parse({
    contractVersion: CONTRACT_VERSION,
    definition: agent.definition,
    requiredCapabilities: requiredCapabilitiesForAgent(agent.definition),
    artifact: {
      sha256: bundle.sha256,
      sizeBytes: bundle.sizeBytes,
      format: "javascript-esm",
    },
  });
  return { agent, bundle, manifest };
}
