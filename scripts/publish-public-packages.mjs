#!/usr/bin/env node

import { execFile as execFileCallback } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

const execFile = promisify(execFileCallback);
const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const registryOrigin = "https://registry.npmjs.org";
const packages = Object.freeze([
  { name: "@calluwu/types", tarballPrefix: "calluwu-types" },
  { name: "@calluwu/webrtc-client", tarballPrefix: "calluwu-webrtc-client" },
  { name: "@calluwu/sdk", tarballPrefix: "calluwu-sdk" },
]);

export function tarballIntegrity(contents) {
  return `sha512-${createHash("sha512").update(contents).digest("base64")}`;
}

export async function readPublishedIntegrity(name, version, fetchImpl = fetch) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10_000);
  try {
    const response = await fetchImpl(
      `${registryOrigin}/${encodeURIComponent(name)}/${encodeURIComponent(version)}`,
      {
        headers: { accept: "application/json" },
        signal: controller.signal,
      },
    );
    if (response.status === 404) return null;
    if (!response.ok) {
      throw new Error(`npm registry returned HTTP ${response.status} for ${name}@${version}`);
    }
    const metadata = await response.json();
    const integrity = metadata?.dist?.integrity;
    if (typeof integrity !== "string" || !integrity.startsWith("sha512-")) {
      throw new Error(`npm registry did not return sha512 integrity for ${name}@${version}`);
    }
    return integrity;
  } finally {
    clearTimeout(timeout);
  }
}

async function publishTarball(path, runCommand = execFile) {
  await runCommand("npm", ["publish", path, "--access", "public", "--provenance"], {
    cwd: repositoryRoot,
    maxBuffer: 1024 * 1024,
  });
}

export async function publishPublicPackages(version, options = {}) {
  if (
    typeof version !== "string" ||
    !/^(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?$/u.test(version)
  ) {
    throw new Error("version must be a canonical semantic version without a v prefix");
  }

  const fetchImpl = options.fetchImpl ?? fetch;
  const runCommand = options.runCommand ?? execFile;
  const releaseDirectory = resolve(
    options.releaseDirectory ?? resolve(repositoryRoot, "release-artifacts"),
  );
  const results = [];

  for (const entry of packages) {
    const path = resolve(releaseDirectory, `${entry.tarballPrefix}-${version}.tgz`);
    if (dirname(path) !== releaseDirectory) {
      throw new Error(`unsafe release artifact path for ${entry.name}`);
    }
    const localIntegrity = tarballIntegrity(await readFile(path));
    const publishedIntegrity = await readPublishedIntegrity(entry.name, version, fetchImpl);

    if (publishedIntegrity !== null) {
      if (publishedIntegrity !== localIntegrity) {
        throw new Error(`${entry.name}@${version} already exists with different immutable content`);
      }
      results.push({ name: entry.name, status: "verified-existing", integrity: localIntegrity });
      continue;
    }

    try {
      await publishTarball(path, runCommand);
    } catch (error) {
      // npm can lose the response after accepting an immutable version. Verify once before
      // failing, so a job resumes without republishing or silently accepting mismatched bytes.
      const acceptedIntegrity = await readPublishedIntegrity(entry.name, version, fetchImpl);
      if (acceptedIntegrity !== localIntegrity) throw error;
    }

    const acceptedIntegrity = await readPublishedIntegrity(entry.name, version, fetchImpl);
    if (acceptedIntegrity !== localIntegrity) {
      throw new Error(`npm did not expose the exact published bytes for ${entry.name}@${version}`);
    }
    results.push({ name: entry.name, status: "published", integrity: localIntegrity });
  }

  return results;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  publishPublicPackages(process.argv[2]).then(
    (results) => {
      for (const result of results) {
        console.log(`${result.status}: ${result.name} (${result.integrity})`);
      }
    },
    (error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    },
  );
}
