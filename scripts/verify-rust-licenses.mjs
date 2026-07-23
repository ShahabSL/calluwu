#!/usr/bin/env node

import { spawnSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCRIPT_DIRECTORY = dirname(fileURLToPath(import.meta.url));
const WORKSPACE_ROOT = resolve(SCRIPT_DIRECTORY, "..");

function fail(message) {
  throw new Error(`Rust production dependency license policy failed: ${message}`);
}

function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function sortedUniqueStrings(values, label) {
  if (
    !Array.isArray(values) ||
    values.length === 0 ||
    values.some((value) => typeof value !== "string" || value.length === 0)
  ) {
    fail(`${label} must be a non-empty string array`);
  }
  const sorted = [...new Set(values)].sort(compareStrings);
  if (JSON.stringify(values) !== JSON.stringify(sorted)) {
    fail(`${label} must be sorted and contain no duplicates`);
  }
  return sorted;
}

export function parseCargoDenyVersion(output) {
  const match = /^cargo-deny (\d+\.\d+\.\d+)(?:\s|$)/u.exec(output.trim());
  if (match === null)
    fail(`could not parse cargo-deny version from ${JSON.stringify(output.trim())}`);
  return match[1];
}

export function parseDenyLicenseAllowlist(contents) {
  const sectionHeader = /^\[licenses\]\s*$/mu.exec(contents);
  if (sectionHeader === null) fail("deny.toml is missing [licenses]");
  const sectionTail = contents.slice(sectionHeader.index + sectionHeader[0].length);
  const nextSection = /^\[[^\]]+\]\s*$/mu.exec(sectionTail);
  const section = nextSection === null ? sectionTail : sectionTail.slice(0, nextSection.index);
  const allow = /^\s*allow\s*=\s*\[([\s\S]*?)^\s*\]\s*$/mu.exec(section);
  if (allow === null) fail("deny.toml is missing licenses.allow");
  const values = [...allow[1].matchAll(/"([^"\\]+)"/gu)].map((match) => match[1]);
  return sortedUniqueStrings(values, "deny.toml licenses.allow");
}

export function verifyRustPolicyConfiguration(policy, denyContents) {
  if (
    policy === null ||
    typeof policy !== "object" ||
    policy.schemaVersion !== 1 ||
    policy.rust === null ||
    typeof policy.rust !== "object" ||
    policy.rust.tool !== "cargo-deny" ||
    !/^\d+\.\d+\.\d+$/u.test(policy.rust.version) ||
    policy.rust.config !== "deny.toml"
  ) {
    fail("dependency-policy.json must pin cargo-deny and deny.toml");
  }
  const policyLicenses = sortedUniqueStrings(policy.rust.allowedLicenses, "rust.allowedLicenses");
  sortedUniqueStrings(policy.rust.allowedExceptions, "rust.allowedExceptions");
  const denyLicenses = parseDenyLicenseAllowlist(denyContents);
  if (JSON.stringify(policyLicenses) !== JSON.stringify(denyLicenses)) {
    fail("dependency-policy.json and deny.toml license allowlists differ");
  }
  return policy.rust;
}

export function verifyInstalledRustLicenses() {
  const policy = JSON.parse(
    readFileSync(resolve(WORKSPACE_ROOT, "dependency-policy.json"), "utf8"),
  );
  const denyContents = readFileSync(resolve(WORKSPACE_ROOT, "deny.toml"), "utf8");
  const rustPolicy = verifyRustPolicyConfiguration(policy, denyContents);

  const versionResult = spawnSync("cargo", ["deny", "--version"], {
    cwd: WORKSPACE_ROOT,
    encoding: "utf8",
  });
  if (versionResult.status !== 0) {
    fail(
      `cargo-deny ${rustPolicy.version} is required; install it with cargo install --locked --version ${rustPolicy.version} cargo-deny`,
    );
  }
  const actualVersion = parseCargoDenyVersion(versionResult.stdout);
  if (actualVersion !== rustPolicy.version) {
    fail(`expected cargo-deny ${rustPolicy.version}, found ${actualVersion}`);
  }

  const checkResult = spawnSync("cargo", ["deny", "check", "licenses"], {
    cwd: WORKSPACE_ROOT,
    stdio: "inherit",
  });
  if (checkResult.status !== 0) {
    fail("cargo deny check licenses rejected the production/build dependency graph");
  }
}

function isMainModule() {
  const entrypoint = process.argv[1];
  return entrypoint !== undefined && import.meta.url === pathToFileURL(resolve(entrypoint)).href;
}

if (isMainModule()) {
  try {
    verifyInstalledRustLicenses();
    process.stdout.write("Rust production dependency licenses accepted by cargo-deny 0.19.4.\n");
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.message : "Unknown failure"}\n`);
    process.exitCode = 1;
  }
}
