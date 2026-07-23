#!/usr/bin/env node

import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);
const SCRIPT_DIRECTORY = dirname(fileURLToPath(import.meta.url));
const WORKSPACE_ROOT = resolve(SCRIPT_DIRECTORY, "..");
const POLICY_PATH = resolve(WORKSPACE_ROOT, "dependency-policy.json");
const PACKAGE_PATH = resolve(WORKSPACE_ROOT, "package.json");
const MAX_COMMAND_OUTPUT_BYTES = 32 * 1024 * 1024;

function fail(message) {
  throw new Error(`Node production dependency license policy failed: ${message}`);
}

function compareStrings(left, right) {
  return left < right ? -1 : left > right ? 1 : 0;
}

function assertSortedUniqueStrings(values, label) {
  if (
    !Array.isArray(values) ||
    values.length === 0 ||
    values.some((value) => typeof value !== "string" || value.length === 0)
  ) {
    fail(`${label} must be a non-empty array of non-empty strings`);
  }
  const sorted = [...new Set(values)].sort(compareStrings);
  if (JSON.stringify(values) !== JSON.stringify(sorted)) {
    fail(`${label} must be sorted and contain no duplicates`);
  }
}

export function validateDependencyPolicy(policy) {
  if (
    policy === null ||
    typeof policy !== "object" ||
    policy.schemaVersion !== 1 ||
    policy.node === null ||
    typeof policy.node !== "object"
  ) {
    fail("dependency-policy.json has an unsupported schema");
  }
  const node = policy.node;
  if (node.tool !== "pnpm" || !/^\d+\.\d+\.\d+$/u.test(node.version)) {
    fail("the Node license scanner must be an exact pnpm version");
  }
  if (
    !Array.isArray(node.command) ||
    JSON.stringify(node.command) !== JSON.stringify(["licenses", "list", "--prod", "--json"])
  ) {
    fail("the pnpm scanner command must inspect production dependencies as JSON");
  }
  assertSortedUniqueStrings(node.allowedLicenses, "node.allowedLicenses");
  assertSortedUniqueStrings(node.allowedExceptions, "node.allowedExceptions");
  const fallback = node.fallback;
  if (
    fallback === null ||
    typeof fallback !== "object" ||
    fallback.trigger !== "ERR_PNPM_MISSING_PACKAGE_INDEX_FILE" ||
    JSON.stringify(fallback.installedCommand) !==
      JSON.stringify(["list", "--prod", "--recursive", "--depth", "Infinity", "--json"]) ||
    JSON.stringify(fallback.lockfileCommand) !==
      JSON.stringify([
        "list",
        "--prod",
        "--recursive",
        "--depth",
        "Infinity",
        "--json",
        "--lockfile-only",
      ]) ||
    !Array.isArray(fallback.licenseOverrides)
  ) {
    fail("the pnpm missing-index fallback contract is invalid");
  }
  const overrideIdentities = new Set();
  for (const override of fallback.licenseOverrides) {
    if (
      override === null ||
      typeof override !== "object" ||
      !/^\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/u.test(override.version) ||
      typeof override.license !== "string" ||
      typeof override.reason !== "string" ||
      override.reason.length < 40
    ) {
      fail("each fallback license override needs an exact version, license, and review reason");
    }
    assertSortedUniqueStrings(override.packages, "node.fallback.licenseOverrides[].packages");
    inspectSpdxExpression(override.license, node);
    for (const name of override.packages) {
      const identity = `${name}@${override.version}`;
      if (overrideIdentities.has(identity)) {
        fail(`duplicate fallback license override for ${identity}`);
      }
      overrideIdentities.add(identity);
    }
  }
  return node;
}

function tokenizeSpdxExpression(expression) {
  const tokens = [];
  const pattern = /\s*(\(|\)|AND|OR|WITH|[A-Za-z0-9][A-Za-z0-9.+-]*)/guy;
  let offset = 0;
  while (offset < expression.length) {
    pattern.lastIndex = offset;
    const match = pattern.exec(expression);
    if (match === null) fail(`invalid SPDX expression ${JSON.stringify(expression)}`);
    tokens.push(match[1]);
    offset = pattern.lastIndex;
  }
  return tokens;
}

function inspectSpdxExpression(expression, policy) {
  if (typeof expression !== "string" || expression.length === 0) {
    fail("a dependency has a missing license expression");
  }
  const tokens = tokenizeSpdxExpression(expression);
  const allowedLicenses = new Set(policy.allowedLicenses);
  const allowedExceptions = new Set(policy.allowedExceptions);
  let cursor = 0;

  function peek() {
    return tokens[cursor];
  }

  function take(expected) {
    if (peek() !== expected) {
      fail(
        `invalid SPDX expression ${JSON.stringify(expression)}: expected ${expected}, found ${
          peek() ?? "end of expression"
        }`,
      );
    }
    cursor += 1;
  }

  function parsePrimary() {
    if (peek() === "(") {
      cursor += 1;
      parseOr();
      take(")");
      return;
    }
    const identifier = peek();
    if (
      identifier === undefined ||
      identifier === ")" ||
      identifier === "AND" ||
      identifier === "OR" ||
      identifier === "WITH"
    ) {
      fail(`invalid SPDX expression ${JSON.stringify(expression)}`);
    }
    if (!allowedLicenses.has(identifier)) {
      fail(
        `license ${JSON.stringify(identifier)} in ${JSON.stringify(
          expression,
        )} is not on the permissive allowlist`,
      );
    }
    cursor += 1;
  }

  function parseWith() {
    parsePrimary();
    if (peek() !== "WITH") return;
    cursor += 1;
    const exception = peek();
    if (
      exception === undefined ||
      exception === ")" ||
      exception === "AND" ||
      exception === "OR" ||
      exception === "WITH"
    ) {
      fail(`invalid SPDX exception in ${JSON.stringify(expression)}`);
    }
    if (!allowedExceptions.has(exception)) {
      fail(
        `license exception ${JSON.stringify(exception)} in ${JSON.stringify(
          expression,
        )} is not on the allowlist`,
      );
    }
    cursor += 1;
  }

  function parseAnd() {
    parseWith();
    while (peek() === "AND") {
      cursor += 1;
      parseWith();
    }
  }

  function parseOr() {
    parseAnd();
    while (peek() === "OR") {
      cursor += 1;
      parseAnd();
    }
  }

  parseOr();
  if (cursor !== tokens.length) {
    fail(`invalid SPDX expression ${JSON.stringify(expression)} near ${JSON.stringify(peek())}`);
  }
}

export function verifyPnpmLicenseReport(report, policy) {
  if (
    report === null ||
    typeof report !== "object" ||
    Array.isArray(report) ||
    Object.keys(report).length === 0
  ) {
    fail("pnpm returned an empty or malformed JSON report");
  }

  const seenPackages = new Set();
  const acceptedExpressions = [];
  for (const expression of Object.keys(report).sort(compareStrings)) {
    inspectSpdxExpression(expression, policy);
    const packages = report[expression];
    if (!Array.isArray(packages) || packages.length === 0) {
      fail(`license group ${JSON.stringify(expression)} has no packages`);
    }
    acceptedExpressions.push(expression);

    for (const dependency of packages) {
      if (
        dependency === null ||
        typeof dependency !== "object" ||
        typeof dependency.name !== "string" ||
        dependency.name.length === 0 ||
        !Array.isArray(dependency.versions) ||
        dependency.versions.length === 0 ||
        !Array.isArray(dependency.paths) ||
        dependency.paths.length !== dependency.versions.length
      ) {
        fail(`license group ${JSON.stringify(expression)} contains malformed package metadata`);
      }
      for (const version of dependency.versions) {
        if (typeof version !== "string" || version.length === 0) {
          fail(`package ${JSON.stringify(dependency.name)} has a missing version`);
        }
        const identity = `${dependency.name}@${version}`;
        if (seenPackages.has(identity)) {
          fail(`package ${identity} appears more than once in the pnpm report`);
        }
        seenPackages.add(identity);
      }
    }
  }

  return {
    packages: seenPackages.size,
    expressions: acceptedExpressions,
  };
}

function collectDependencyGraph(report, label) {
  if (!Array.isArray(report) || report.length === 0) {
    fail(`${label} is empty or malformed`);
  }
  const external = new Map();

  function visitDependencies(dependencies) {
    if (dependencies === undefined) return;
    if (dependencies === null || typeof dependencies !== "object" || Array.isArray(dependencies)) {
      fail(`${label} contains a malformed dependencies object`);
    }
    for (const [edgeName, node] of Object.entries(dependencies)) {
      if (
        node === null ||
        typeof node !== "object" ||
        typeof node.version !== "string" ||
        node.version.length === 0
      ) {
        fail(`${label} contains malformed metadata for ${edgeName}`);
      }
      const name = typeof node.from === "string" && node.from.length > 0 ? node.from : edgeName;
      if (node.version.startsWith("link:") || node.version.startsWith("workspace:")) {
        visitDependencies(node.dependencies);
        continue;
      }
      if (
        typeof node.resolved !== "string" ||
        !node.resolved.startsWith("https://registry.npmjs.org/") ||
        typeof node.path !== "string" ||
        node.path.length === 0
      ) {
        fail(`${label} contains an unsupported external source for ${name}@${node.version}`);
      }

      const identity = `${name}@${node.version}`;
      const existing = external.get(identity) ?? {
        identity,
        name,
        version: node.version,
        paths: new Set(),
      };
      existing.paths.add(node.path);
      external.set(identity, existing);
      visitDependencies(node.dependencies);
    }
  }

  for (const project of report) {
    if (
      project === null ||
      typeof project !== "object" ||
      typeof project.path !== "string" ||
      project.path.length === 0
    ) {
      fail(`${label} contains a malformed workspace project`);
    }
    visitDependencies(project.dependencies);
  }
  if (external.size === 0) fail(`${label} contains no external production dependencies`);
  return external;
}

function fallbackOverrides(policy) {
  const overrides = new Map();
  for (const override of policy.fallback.licenseOverrides) {
    for (const name of override.packages) {
      overrides.set(`${name}@${override.version}`, override.license);
    }
  }
  return overrides;
}

function assertFallbackOverrideInventory(locked, policy) {
  const overrides = fallbackOverrides(policy);
  const staleOverrides = [...overrides.keys()]
    .filter((identity) => !locked.has(identity))
    .sort(compareStrings);
  if (staleOverrides.length > 0) {
    fail(`fallback license overrides are stale: ${staleOverrides.join(", ")}`);
  }
  return overrides;
}

async function readInstalledManifest(dependency) {
  for (const path of [...dependency.paths].sort(compareStrings)) {
    try {
      return {
        path,
        manifest: JSON.parse(await readFile(resolve(path, "package.json"), "utf8")),
      };
    } catch (error) {
      if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") {
        continue;
      }
      const reason = error instanceof Error ? error.message : "unknown read failure";
      fail(`could not read installed manifest for ${dependency.identity}: ${reason}`);
    }
  }
  return null;
}

export async function buildManifestFallbackReport({
  lockfileReport,
  installedReport,
  policy,
  manifestReader = readInstalledManifest,
}) {
  const locked = collectDependencyGraph(lockfileReport, "pnpm lockfile production graph");
  const installed = collectDependencyGraph(installedReport, "pnpm installed production graph");
  const lockedIdentities = [...locked.keys()].sort(compareStrings);
  const installedIdentities = [...installed.keys()].sort(compareStrings);
  if (JSON.stringify(lockedIdentities) !== JSON.stringify(installedIdentities)) {
    const lockedSet = new Set(lockedIdentities);
    const installedSet = new Set(installedIdentities);
    const missing = lockedIdentities.filter((identity) => !installedSet.has(identity));
    const extra = installedIdentities.filter((identity) => !lockedSet.has(identity));
    fail(
      `the installed production graph does not exactly match the frozen lock graph; missing=[${missing.join(
        ", ",
      )}], extra=[${extra.join(", ")}]`,
    );
  }

  const overrides = assertFallbackOverrideInventory(locked, policy);

  const report = {};
  for (const identity of lockedIdentities) {
    const dependency = installed.get(identity);
    const installedManifest = await manifestReader(dependency);
    const override = overrides.get(identity);
    let license;
    let evidencePath = null;
    if (installedManifest === null) {
      if (override === undefined) {
        fail(`no installed package manifest or exact reviewed override exists for ${identity}`);
      }
      license = override;
    } else {
      const { manifest, path } = installedManifest;
      if (
        manifest === null ||
        typeof manifest !== "object" ||
        manifest.name !== dependency.name ||
        manifest.version !== dependency.version ||
        typeof manifest.license !== "string" ||
        manifest.license.length === 0
      ) {
        fail(`installed package manifest identity or license mismatch for ${identity}`);
      }
      license = manifest.license;
      evidencePath = path;
      if (override !== undefined && override !== license) {
        fail(
          `installed license ${JSON.stringify(license)} does not match the reviewed override ${JSON.stringify(
            override,
          )} for ${identity}`,
        );
      }
    }
    report[license] ??= [];
    report[license].push({
      name: dependency.name,
      versions: [dependency.version],
      paths: [evidencePath],
    });
  }
  for (const packages of Object.values(report)) {
    packages.sort((left, right) =>
      compareStrings(`${left.name}@${left.versions[0]}`, `${right.name}@${right.versions[0]}`),
    );
  }
  return report;
}

async function readJson(path, label) {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    const reason = error instanceof Error ? error.message : "unknown read failure";
    fail(`could not read ${label}: ${reason}`);
  }
}

function pnpmErrorCode(error) {
  if (!(error && typeof error === "object")) return undefined;
  for (const stream of ["stdout", "stderr"]) {
    if (!(stream in error) || typeof error[stream] !== "string") continue;
    try {
      const parsed = JSON.parse(error[stream]);
      if (typeof parsed?.error?.code === "string") return parsed.error.code;
    } catch {
      // Non-JSON pnpm diagnostics are included in the fail-closed error below.
    }
  }
  return undefined;
}

async function runPnpm(arguments_, { acceptedErrorCode } = {}) {
  try {
    return await execFileAsync("pnpm", arguments_, {
      cwd: WORKSPACE_ROOT,
      encoding: "utf8",
      maxBuffer: MAX_COMMAND_OUTPUT_BYTES,
      env: { ...process.env, NO_COLOR: "1" },
    });
  } catch (error) {
    if (acceptedErrorCode !== undefined && pnpmErrorCode(error) === acceptedErrorCode) {
      return { stdout: "", stderr: "", acceptedErrorCode };
    }
    const stderr =
      error && typeof error === "object" && "stderr" in error && typeof error.stderr === "string"
        ? error.stderr.trim()
        : "";
    const stdout =
      error && typeof error === "object" && "stdout" in error && typeof error.stdout === "string"
        ? error.stdout.trim()
        : "";
    const detail =
      stderr || stdout || (error instanceof Error ? error.message : "unknown pnpm failure");
    fail(
      `pnpm ${arguments_.join(
        " ",
      )} did not complete; no dependency was skipped. Run a frozen install (or a forced frozen install if the pnpm store index is incomplete) and retry. ${detail}`,
    );
  }
}

export async function verifyInstalledNodeLicenses() {
  const dependencyPolicy = await readJson(POLICY_PATH, "dependency-policy.json");
  const nodePolicy = validateDependencyPolicy(dependencyPolicy);
  const packageManifest = await readJson(PACKAGE_PATH, "package.json");
  if (packageManifest.packageManager !== `pnpm@${nodePolicy.version}`) {
    fail(
      `packageManager must be exactly pnpm@${nodePolicy.version}, found ${JSON.stringify(
        packageManifest.packageManager,
      )}`,
    );
  }

  const versionResult = await runPnpm(["--version"]);
  const actualVersion = versionResult.stdout.trim();
  if (actualVersion !== nodePolicy.version) {
    fail(`expected pnpm ${nodePolicy.version}, found ${actualVersion || "no version"}`);
  }

  const licenseResult = await runPnpm(nodePolicy.command, {
    acceptedErrorCode: nodePolicy.fallback.trigger,
  });
  const lockfileResult = await runPnpm(nodePolicy.fallback.lockfileCommand);
  let lockfileReport;
  try {
    lockfileReport = JSON.parse(lockfileResult.stdout);
  } catch {
    fail("pnpm lockfile graph command did not return valid JSON");
  }
  const locked = collectDependencyGraph(lockfileReport, "pnpm lockfile production graph");
  assertFallbackOverrideInventory(locked, nodePolicy);

  let report;
  let source = "pnpm licenses";
  if (licenseResult.acceptedErrorCode === nodePolicy.fallback.trigger) {
    const installedResult = await runPnpm(nodePolicy.fallback.installedCommand);
    let installedReport;
    try {
      installedReport = JSON.parse(installedResult.stdout);
    } catch {
      fail("pnpm installed fallback graph command did not return valid JSON");
    }
    report = await buildManifestFallbackReport({
      lockfileReport,
      installedReport,
      policy: nodePolicy,
    });
    source = "verified pnpm lock/install manifest fallback";
  } else {
    try {
      report = JSON.parse(licenseResult.stdout);
    } catch {
      fail("pnpm did not return valid JSON; an empty dependency graph is not accepted");
    }
  }
  return { ...verifyPnpmLicenseReport(report, nodePolicy), source };
}

function isMainModule() {
  const entrypoint = process.argv[1];
  return entrypoint !== undefined && import.meta.url === pathToFileURL(resolve(entrypoint)).href;
}

if (isMainModule()) {
  verifyInstalledNodeLicenses()
    .then((result) => {
      process.stdout.write(
        `Node production dependency licenses accepted via ${result.source}: ${result.packages.toString()} package versions across ${result.expressions.length.toString()} reviewed expression(s).\n`,
      );
    })
    .catch((error) => {
      process.stderr.write(`${error instanceof Error ? error.message : "Unknown failure"}\n`);
      process.exitCode = 1;
    });
}
