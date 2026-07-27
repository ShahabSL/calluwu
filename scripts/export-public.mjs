#!/usr/bin/env node

import { createHash, randomUUID } from "node:crypto";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readdir,
  readFile,
  realpath,
  rename,
  rm,
  rmdir,
  stat,
} from "node:fs/promises";
import { homedir } from "node:os";
import { basename, dirname, isAbsolute, join, parse, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCRIPT_PATH = fileURLToPath(import.meta.url);
const DEFAULT_SOURCE_ROOT = resolve(dirname(SCRIPT_PATH), "..");
const MANIFEST_PATH = ".public-export-manifest.json";
const MAX_PUBLIC_FILE_BYTES = 10 * 1024 * 1024;

function pathList(value) {
  return value.trim().split(/\s+/u);
}

const COMPONENT_FILES = pathList(`
  examples/customer-support/agent.ts
  examples/customer-support/package.json
  examples/customer-support/tsconfig.json
  packages/sdk/package.json
  packages/sdk/scripts/postbuild.mjs
  packages/sdk/src/agent.ts
  packages/sdk/src/bundler.ts
  packages/sdk/src/cli.ts
  packages/sdk/src/client.ts
  packages/sdk/src/index.ts
  packages/sdk/src/presets.ts
  packages/sdk/src/runtime.ts
  packages/sdk/src/tool.ts
  packages/sdk/test/agent.test.ts
  packages/sdk/test/bundler.test.ts
  packages/sdk/test/client.test.ts
  packages/sdk/test/deploy-cli.test.js
  packages/sdk/test/fixture-agent.ts
  packages/sdk/test/replay-handshake.test.js
  packages/sdk/test/runtime.test.ts
  packages/sdk/tsconfig.build.json
  packages/sdk/tsconfig.json
  packages/types/package.json
  packages/types/src/agent.ts
  packages/types/src/api.ts
  packages/types/src/domain.ts
  packages/types/src/events.ts
  packages/types/src/index.ts
  packages/types/src/intake.ts
  packages/types/src/primitives.ts
  packages/types/src/realtime.ts
  packages/types/test/contracts.test.ts
  packages/types/test/fixtures/unicode-boundaries.json
  packages/types/test/intake.test.ts
  packages/types/tsconfig.build.json
  packages/types/tsconfig.json
  packages/webrtc-client/README.md
  packages/webrtc-client/package.json
  packages/webrtc-client/src/client.ts
  packages/webrtc-client/src/index.ts
  packages/webrtc-client/src/types.ts
  packages/webrtc-client/src/validation.ts
  packages/webrtc-client/test/client.spec.ts
  packages/webrtc-client/test/fakes.ts
  packages/webrtc-client/tsconfig.build.json
  packages/webrtc-client/tsconfig.json
  runtime/calluwu-core/Cargo.toml
  runtime/calluwu-core/Dockerfile
  runtime/calluwu-core/README.md
  runtime/calluwu-core/benches/latency.rs
  runtime/calluwu-core/src/domain.rs
  runtime/calluwu-core/src/error.rs
  runtime/calluwu-core/src/event.rs
  runtime/calluwu-core/src/lib.rs
  runtime/calluwu-core/src/main.rs
  runtime/calluwu-core/src/manifest.rs
  runtime/calluwu-core/src/probe.rs
  runtime/calluwu-core/src/protocol.rs
  runtime/calluwu-core/src/provider.rs
  runtime/calluwu-core/src/provider/gateway.rs
  runtime/calluwu-core/src/server.rs
  runtime/calluwu-core/src/session.rs
  runtime/calluwu-core/src/supervisor.rs
  runtime/calluwu-core/src/tool.rs
  runtime/calluwu-core/tests/cli.rs
  runtime/calluwu-core/tests/realtime_websocket.rs
`);

const ROOT_FILES = pathList(`
  .dockerignore
  .editorconfig
  CODE_OF_CONDUCT.md
  Cargo.lock
  Cargo.toml
  SUPPORT.md
  TRADEMARKS.md
  biome.json
  deny.toml
  dependency-policy.json
  rust-toolchain.toml
  tsconfig.base.json
`);

const TOOLING_FILES = pathList(`
  scripts/export-public.mjs
  scripts/export-public.test.mjs
  scripts/publish-public-packages.mjs
  scripts/publish-public-packages.test.mjs
  scripts/replay-handshake.mjs
  scripts/verify-node-licenses.mjs
  scripts/verify-node-licenses.test.mjs
  scripts/verify-release-version.mjs
  scripts/verify-release-version.test.mjs
  scripts/verify-rust-licenses.mjs
  scripts/verify-rust-licenses.test.mjs
`);

const TEMPLATE_DESTINATIONS = pathList(`
  .github/CODEOWNERS
  .github/PULL_REQUEST_TEMPLATE.md
  .github/dependabot.yml
  .github/workflows/ci.yml
  .github/workflows/publish.yml
  .gitignore
  .gitleaks.toml
  CONTRIBUTING.md
  GOVERNANCE.md
  LICENSE
  NOTICE
  README.md
  SECURITY.md
  docs/README.md
  docs/cloud-boundary.md
  docs/release-integrity.md
  docs/supply-chain.md
  package.json
  pnpm-lock.yaml
  pnpm-workspace.yaml
`);

const COMPONENT_ROOTS = [
  "examples/customer-support",
  "packages/sdk",
  "packages/types",
  "packages/webrtc-client",
  "runtime/calluwu-core",
];

const IGNORED_SOURCE_DIRECTORY_NAMES = new Set([
  ".git",
  ".wrangler",
  "coverage",
  "dist",
  "node_modules",
  "target",
]);

const FORBIDDEN_OUTPUT_SEGMENTS = new Set([
  ".calluwu",
  ".git",
  ".playwright-cli",
  ".playwright-mcp",
  ".pnpm-store",
  ".wrangler",
  "coverage",
  "dist",
  "node_modules",
  "target",
]);

const FORBIDDEN_OUTPUT_PREFIXES = [
  "apps/",
  "brand/",
  "infra/",
  "packages/brand/",
  "packages/cloud-contracts/",
  "scripts/public-root/",
];

const SECRET_CONTENT_PATTERNS = [
  { name: "private key", pattern: /-----BEGIN (?:[A-Z0-9 ]+ )?PRIVATE KEY-----/u },
  { name: "GitHub token", pattern: /\bgh[pousr]_[A-Za-z0-9]{32,}\b/u },
  { name: "AWS access key", pattern: /\bAKIA[A-Z0-9]{16}\b/u },
  { name: "Stripe live secret", pattern: /\bsk_live_[A-Za-z0-9]{16,}\b/u },
  { name: "Slack token", pattern: /\bxox[baprs]-[A-Za-z0-9-]{20,}\b/u },
  {
    name: "credential file",
    pattern: /(?:^|[/\\])\.calluwu[/\\]credentials\.json\b/u,
  },
];

const PRIVILEGED_SURFACE_PATTERNS = [
  {
    name: "hosted service initialization surface",
    pattern: new RegExp(["boot", "strap"].join(""), "iu"),
  },
  {
    name: "service-owner identity surface",
    pattern: new RegExp(["found", "er"].join(""), "iu"),
  },
  {
    name: "projection recovery surface",
    pattern: new RegExp(["dead", "[-_ ]?", "letters?"].join(""), "iu"),
  },
  {
    name: "billing administration surface",
    pattern: new RegExp(["billing", "[-_ ]", "admin"].join(""), "iu"),
  },
  {
    name: "provider credential surface",
    pattern: new RegExp(["provider", "[-_ ]", "secrets?"].join(""), "iu"),
  },
  {
    name: "founder bootstrap client",
    pattern: new RegExp(["CalluwuClient", "\\.", "bootstrap"].join(""), "u"),
  },
  {
    name: "founder bootstrap CLI",
    pattern: new RegExp(["\\.command\\([\"']", "bootstrap", "[\"']\\)"].join(""), "u"),
  },
  {
    name: "founder bootstrap route",
    pattern: new RegExp(["/v1/", "bootstrap"].join(""), "u"),
  },
  {
    name: "founder bootstrap credential",
    pattern: new RegExp(["CALLUWU_", "BOOTSTRAP_TOKEN"].join(""), "u"),
  },
  {
    name: "projection recovery client",
    pattern: new RegExp(["(?:list|replay)", "ProjectionDeadLetter"].join(""), "u"),
  },
  {
    name: "projection recovery route",
    pattern: new RegExp(["/v1/", "projection-dead-letters"].join(""), "u"),
  },
  {
    name: "projection recovery CLI",
    pattern: new RegExp(["\\.command\\([\"']", "dead-letters", "[\"']\\)"].join(""), "u"),
  },
  {
    name: "private bootstrap schema",
    pattern: new RegExp(["Bootstrap", "(?:Request|Response)", "Schema"].join(""), "u"),
  },
  {
    name: "private projection schema",
    pattern: new RegExp(["Projection", "DeadLetter"].join(""), "u"),
  },
];

function normalizeRelativePath(value) {
  const normalized = value.split(sep).join("/");
  if (
    normalized.length === 0 ||
    normalized.startsWith("/") ||
    normalized === ".." ||
    normalized.startsWith("../") ||
    normalized.includes("/../") ||
    normalized.includes("\0")
  ) {
    throw new Error(`Unsafe relative path in public inventory: ${JSON.stringify(value)}`);
  }
  return normalized;
}

function templateEntry(destination) {
  return {
    destination,
    sourceCandidates: [`scripts/public-root/${destination}`, destination],
  };
}

export const PUBLIC_EXPORT_ENTRIES = Object.freeze(
  [
    ...ROOT_FILES.map((destination) => ({ destination, sourceCandidates: [destination] })),
    ...TOOLING_FILES.map((destination) => ({ destination, sourceCandidates: [destination] })),
    ...COMPONENT_FILES.map((destination) => ({ destination, sourceCandidates: [destination] })),
    ...TEMPLATE_DESTINATIONS.map(templateEntry),
  ]
    .map((entry) => ({
      destination: normalizeRelativePath(entry.destination),
      sourceCandidates: entry.sourceCandidates.map(normalizeRelativePath),
    }))
    .sort((left, right) => left.destination.localeCompare(right.destination)),
);

export const PUBLIC_DESTINATIONS = Object.freeze(
  PUBLIC_EXPORT_ENTRIES.map((entry) => entry.destination),
);

function assertSafeOutputPath(path) {
  const normalized = normalizeRelativePath(path);
  const segments = normalized.split("/");
  if (segments.some((segment) => FORBIDDEN_OUTPUT_SEGMENTS.has(segment))) {
    throw new Error(`Forbidden build, secret, or repository path in public output: ${normalized}`);
  }
  if (FORBIDDEN_OUTPUT_PREFIXES.some((prefix) => normalized.startsWith(prefix))) {
    throw new Error(`Private workspace path in public output: ${normalized}`);
  }
  if (
    segments.some(
      (segment) =>
        segment === ".env" ||
        segment.startsWith(".env.") ||
        segment === ".dev.vars" ||
        segment.startsWith(".dev.vars."),
    )
  ) {
    throw new Error(`Environment file in public output: ${normalized}`);
  }
}

function assertInventoryDefinition() {
  const seenDestinations = new Set();
  for (const entry of PUBLIC_EXPORT_ENTRIES) {
    if (seenDestinations.has(entry.destination)) {
      throw new Error(`Duplicate public destination: ${entry.destination}`);
    }
    seenDestinations.add(entry.destination);
    assertSafeOutputPath(entry.destination);
  }
}

async function listTreeFiles(root, options = {}) {
  const results = [];
  async function visit(directory, prefix) {
    const entries = await readdir(directory, { withFileTypes: true });
    entries.sort((left, right) => left.name.localeCompare(right.name));
    for (const entry of entries) {
      const relativePath = prefix.length === 0 ? entry.name : `${prefix}/${entry.name}`;
      const absolutePath = join(directory, entry.name);
      if (entry.isDirectory()) {
        if (options.ignoreBuildDirectories && IGNORED_SOURCE_DIRECTORY_NAMES.has(entry.name)) {
          continue;
        }
        await visit(absolutePath, relativePath);
      } else if (entry.isFile()) {
        results.push(relativePath);
      } else {
        throw new Error(`Symlink or special file is not allowed: ${relativePath}`);
      }
    }
  }
  await visit(root, "");
  return results;
}

export async function validatePublicSourceInventory(sourceRoot = DEFAULT_SOURCE_ROOT) {
  const allowed = new Set(COMPONENT_FILES);
  const unexpected = [];
  for (const componentRoot of COMPONENT_ROOTS) {
    const files = await listTreeFiles(resolve(sourceRoot, componentRoot), {
      ignoreBuildDirectories: true,
    });
    for (const file of files) {
      const sourcePath = `${componentRoot}/${file}`;
      if (!allowed.has(sourcePath)) unexpected.push(sourcePath);
    }
  }

  const templateRoot = resolve(sourceRoot, "scripts/public-root");
  try {
    const templateStat = await lstat(templateRoot);
    if (!templateStat.isDirectory()) {
      throw new Error("scripts/public-root must be a directory");
    }
    const actualTemplates = (await listTreeFiles(templateRoot)).sort();
    const expectedTemplates = [...TEMPLATE_DESTINATIONS].sort();
    if (JSON.stringify(actualTemplates) !== JSON.stringify(expectedTemplates)) {
      const actualSet = new Set(actualTemplates);
      const expectedSet = new Set(expectedTemplates);
      for (const path of actualTemplates) {
        if (!expectedSet.has(path)) unexpected.push(`scripts/public-root/${path}`);
      }
      for (const path of expectedTemplates) {
        if (!actualSet.has(path)) unexpected.push(`scripts/public-root/${path} (missing)`);
      }
    }
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) {
      throw error;
    }
  }

  if (unexpected.length > 0) {
    throw new Error(
      `Unexpected files in public component trees or public templates; review and update the exact inventory:\n${unexpected
        .sort()
        .map((path) => `- ${path}`)
        .join("\n")}`,
    );
  }
}

async function firstExistingSource(sourceRoot, candidates) {
  for (const candidate of candidates) {
    const absolutePath = resolve(sourceRoot, candidate);
    try {
      const sourceStat = await lstat(absolutePath);
      if (!sourceStat.isFile()) {
        throw new Error(`Public source must be a regular file: ${candidate}`);
      }
      if (sourceStat.size > MAX_PUBLIC_FILE_BYTES) {
        throw new Error(
          `Public source exceeds the ${MAX_PUBLIC_FILE_BYTES.toString()} byte limit: ${candidate}`,
        );
      }
      return absolutePath;
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        continue;
      }
      throw error;
    }
  }
  throw new Error(`Missing public source; checked: ${candidates.join(", ")}`);
}

function isText(bytes) {
  return !bytes.subarray(0, Math.min(bytes.length, 8_192)).includes(0);
}

function scanSecrets(path, bytes) {
  if (!isText(bytes)) return;
  const text = bytes.toString("utf8");
  for (const check of SECRET_CONTENT_PATTERNS) {
    if (check.pattern.test(text)) {
      throw new Error(`Potential ${check.name} in public output: ${path}`);
    }
  }
}

function scanPrivilegedSurfaces(path, bytes) {
  const shouldScan =
    path === "README.md" ||
    path.startsWith("docs/") ||
    path.startsWith("packages/sdk/") ||
    path.startsWith("packages/types/");
  if (!shouldScan || !isText(bytes)) return;
  const text = bytes.toString("utf8");
  for (const check of PRIVILEGED_SURFACE_PATTERNS) {
    if (check.pattern.test(text)) {
      throw new Error(`Private ${check.name} re-entered public output: ${path}`);
    }
  }
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

async function writePublicFile(stagingRoot, destination, bytes) {
  const target = resolve(stagingRoot, destination);
  await mkdir(dirname(target), { recursive: true, mode: 0o755 });
  const mode = destination.endsWith(".mjs") ? 0o755 : 0o644;
  const handle = await open(target, "wx", mode);
  try {
    await handle.writeFile(bytes);
  } finally {
    await handle.close();
  }
  await chmod(target, mode);
  return mode;
}

function isPathInside(parent, child) {
  const difference = relative(parent, child);
  return difference !== "" && difference !== ".." && !difference.startsWith(`..${sep}`);
}

async function prepareDestination(sourceRoot, destination) {
  if (!isAbsolute(destination)) {
    throw new Error("Public export destination must be an absolute path");
  }
  const source = await realpath(sourceRoot);
  const target = resolve(destination);
  if (
    target === source ||
    isPathInside(source, target) ||
    target === parse(target).root ||
    target === resolve(homedir())
  ) {
    throw new Error(`Unsafe public export destination: ${target}`);
  }

  await mkdir(dirname(target), { recursive: true });
  const canonicalParent = await realpath(dirname(target));
  const canonicalTarget = join(canonicalParent, basename(target));
  try {
    const destinationStat = await lstat(canonicalTarget);
    if (!destinationStat.isDirectory()) {
      throw new Error("Public export destination exists and is not a directory");
    }
    if ((await readdir(canonicalTarget)).length > 0) {
      throw new Error("Public export destination must be new or empty");
    }
  } catch (error) {
    if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) {
      throw error;
    }
  }
  return { source, target: canonicalTarget };
}

export async function validatePublicOutput(outputRoot) {
  const actual = (await listTreeFiles(outputRoot)).sort();
  const expected = [...PUBLIC_DESTINATIONS, MANIFEST_PATH].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    const expectedSet = new Set(expected);
    const actualSet = new Set(actual);
    const unexpected = actual.filter((path) => !expectedSet.has(path));
    const missing = expected.filter((path) => !actualSet.has(path));
    throw new Error(
      [
        "Public output did not match the exact destination inventory.",
        ...unexpected.map((path) => `Unexpected: ${path}`),
        ...missing.map((path) => `Missing: ${path}`),
      ].join("\n"),
    );
  }
  for (const path of actual) assertSafeOutputPath(path);

  const parsed = JSON.parse(await readFile(resolve(outputRoot, MANIFEST_PATH), "utf8"));
  if (
    parsed === null ||
    typeof parsed !== "object" ||
    parsed.schemaVersion !== 1 ||
    !Array.isArray(parsed.files)
  ) {
    throw new Error("Public export manifest is invalid");
  }
  const manifestPaths = parsed.files.map((entry) => entry?.path).sort();
  if (JSON.stringify(manifestPaths) !== JSON.stringify([...PUBLIC_DESTINATIONS].sort())) {
    throw new Error("Public export manifest path inventory is invalid");
  }
  for (const entry of parsed.files) {
    if (
      entry === null ||
      typeof entry !== "object" ||
      typeof entry.path !== "string" ||
      typeof entry.sha256 !== "string" ||
      !/^(?:0644|0755)$/u.test(entry.mode)
    ) {
      throw new Error("Public export manifest entry is invalid");
    }
    assertSafeOutputPath(entry.path);
    const target = resolve(outputRoot, entry.path);
    const bytes = await readFile(target);
    if (sha256(bytes) !== entry.sha256) {
      throw new Error(`Public export manifest checksum mismatch: ${entry.path}`);
    }
    const actualMode = ((await stat(target)).mode & 0o777).toString(8).padStart(4, "0");
    if (actualMode !== entry.mode) {
      throw new Error(`Public export manifest mode mismatch: ${entry.path}`);
    }
    scanSecrets(entry.path, bytes);
    scanPrivilegedSurfaces(entry.path, bytes);
  }
}

export async function exportPublicRepository({
  sourceRoot = DEFAULT_SOURCE_ROOT,
  destination,
} = {}) {
  if (typeof destination !== "string" || destination.length === 0) {
    throw new Error("A public export destination is required");
  }
  assertInventoryDefinition();
  const prepared = await prepareDestination(sourceRoot, destination);
  await validatePublicSourceInventory(prepared.source);

  const stagingRoot = join(
    dirname(prepared.target),
    `.calluwu-public-export-${process.pid.toString()}-${randomUUID()}`,
  );
  await mkdir(stagingRoot, { mode: 0o700 });
  try {
    const manifestEntries = [];
    for (const entry of PUBLIC_EXPORT_ENTRIES) {
      const source = await firstExistingSource(prepared.source, entry.sourceCandidates);
      const bytes = await readFile(source);
      scanSecrets(entry.destination, bytes);
      scanPrivilegedSurfaces(entry.destination, bytes);
      const mode = await writePublicFile(stagingRoot, entry.destination, bytes);
      manifestEntries.push({
        path: entry.destination,
        sha256: sha256(bytes),
        mode: mode === 0o755 ? "0755" : "0644",
      });
    }
    const manifest = `${JSON.stringify({ schemaVersion: 1, files: manifestEntries }, null, 2)}\n`;
    await writePublicFile(stagingRoot, MANIFEST_PATH, Buffer.from(manifest));
    await validatePublicOutput(stagingRoot);

    try {
      const targetStat = await stat(prepared.target);
      if (targetStat.isDirectory()) await rmdir(prepared.target);
    } catch (error) {
      if (!(error && typeof error === "object" && "code" in error && error.code === "ENOENT")) {
        throw error;
      }
    }
    await rename(stagingRoot, prepared.target);
    return {
      destination: prepared.target,
      files: manifestEntries.length,
      manifest: resolve(prepared.target, MANIFEST_PATH),
    };
  } catch (error) {
    await rm(stagingRoot, { recursive: true, force: true });
    throw error;
  }
}

function isMainModule() {
  const entrypoint = process.argv[1];
  return entrypoint !== undefined && import.meta.url === pathToFileURL(resolve(entrypoint)).href;
}

if (isMainModule()) {
  exportPublicRepository({ destination: process.argv[2] })
    .then((result) => {
      process.stdout.write(
        `${JSON.stringify(
          {
            status: "exported",
            destination: result.destination,
            files: result.files,
            manifest: result.manifest,
          },
          null,
          2,
        )}\n`,
      );
    })
    .catch((error) => {
      const message = error instanceof Error ? error.message : "Unknown public export failure";
      process.stderr.write(`Public export failed: ${message}\n`);
      process.exitCode = 1;
    });
}
