import assert from "node:assert/strict";
import { mkdir, readdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { after, describe, it } from "node:test";
import {
  exportPublicRepository,
  PUBLIC_DESTINATIONS,
  validatePublicOutput,
  validatePublicSourceInventory,
} from "./export-public.mjs";

const temporaryPaths = [];

function temporaryPath(name) {
  const path = join(
    tmpdir(),
    `calluwu-public-export-test-${process.pid.toString()}-${crypto.randomUUID()}-${name}`,
  );
  temporaryPaths.push(path);
  return path;
}

async function fileDigestMap(directory) {
  const manifest = JSON.parse(
    await readFile(join(directory, ".public-export-manifest.json"), "utf8"),
  );
  return manifest.files.map(({ path, sha256, mode }) => ({ path, sha256, mode }));
}

async function workflowFiles(root) {
  const directory = join(root, ".github/workflows");
  const entries = (await readdir(directory)).filter((path) => path.endsWith(".yml")).sort();
  return Promise.all(
    entries.map(async (path) => ({
      path,
      contents: await readFile(join(directory, path), "utf8"),
    })),
  );
}

function assertFullShaActionPins(path, contents) {
  const actions = [...contents.matchAll(/^\s*(?:-\s+)?uses:\s+[^@\s]+@([^\s#]+)/gmu)];
  assert.ok(actions.length > 0, `${path} should use at least one reviewed action`);
  for (const action of actions) {
    assert.match(action[1], /^[0-9a-f]{40}$/u, `${path} has an unpinned action: ${action[0]}`);
  }
}

after(async () => {
  await Promise.all(temporaryPaths.map((path) => rm(path, { recursive: true, force: true })));
});

describe("public repository exporter", () => {
  it("has a reviewed source inventory and deterministic content", async () => {
    await validatePublicSourceInventory();
    const first = temporaryPath("first");
    const second = temporaryPath("second");

    await exportPublicRepository({ destination: first });
    await exportPublicRepository({ destination: second });

    assert.deepEqual(await fileDigestMap(first), await fileDigestMap(second));
    await validatePublicOutput(first);
    assert.equal(PUBLIC_DESTINATIONS.includes("packages/cloud-contracts/package.json"), false);
    assert.equal(
      PUBLIC_DESTINATIONS.some((path) => path.startsWith("apps/")),
      false,
    );
    assert.equal(
      PUBLIC_DESTINATIONS.some((path) => path.includes("public-root")),
      false,
    );
  });

  it("refuses a non-empty destination instead of overwriting it", async () => {
    const destination = temporaryPath("non-empty");
    await writeFile(destination, "not a directory", "utf8");

    await assert.rejects(exportPublicRepository({ destination }), /exists and is not a directory/u);
    assert.equal(await readFile(destination, "utf8"), "not a directory");
  });

  it("fails closed on an unreviewed component file or secret-like content", async () => {
    const source = temporaryPath("tainted-source");
    await exportPublicRepository({ destination: source });

    const unexpectedFile = join(source, "packages/sdk/src/unreviewed.ts");
    await writeFile(unexpectedFile, "export const unreviewed = true;\n", "utf8");
    await assert.rejects(
      validatePublicSourceInventory(source),
      /Unexpected files in public component trees/u,
    );
    await rm(unexpectedFile);

    const secret = ["AKIA", "A".repeat(16)].join("");
    await writeFile(join(source, "README.md"), `${secret}\n`, "utf8");
    await assert.rejects(
      exportPublicRepository({
        sourceRoot: source,
        destination: temporaryPath("tainted-output"),
      }),
      /Potential AWS access key in public output/u,
    );
  });

  it("fails closed on public template inventory drift", async () => {
    const source = temporaryPath("template-drift");
    await exportPublicRepository({ destination: source });
    const unexpected = join(source, "scripts/public-root/.github/workflows/unreviewed.yml");
    await mkdir(join(source, "scripts/public-root/.github/workflows"), { recursive: true });
    await writeFile(unexpected, "name: unreviewed\n", "utf8");

    await assert.rejects(
      validatePublicSourceInventory(source),
      /public component trees or public templates/u,
    );
  });

  it("ships complete, pinned license and SBOM release gates", async () => {
    const destination = temporaryPath("supply-chain");
    await exportPublicRepository({ destination });

    for (const required of [
      "deny.toml",
      "dependency-policy.json",
      "docs/supply-chain.md",
      "scripts/publish-public-packages.mjs",
      "scripts/publish-public-packages.test.mjs",
      "scripts/verify-node-licenses.mjs",
      "scripts/verify-node-licenses.test.mjs",
      "scripts/verify-rust-licenses.mjs",
      "scripts/verify-rust-licenses.test.mjs",
    ]) {
      assert.equal(PUBLIC_DESTINATIONS.includes(required), true, required);
    }

    const packageManifest = JSON.parse(await readFile(join(destination, "package.json"), "utf8"));
    assert.equal(packageManifest.packageManager, "pnpm@11.9.0");
    assert.equal(packageManifest.scripts["licenses:node"], "node scripts/verify-node-licenses.mjs");
    assert.equal(packageManifest.scripts["licenses:rust"], "node scripts/verify-rust-licenses.mjs");
    assert.match(packageManifest.scripts.check, /pnpm run licenses/u);

    const publicWorkflows = await workflowFiles(destination);
    for (const workflow of publicWorkflows) {
      assertFullShaActionPins(`public ${workflow.path}`, workflow.contents);
      assert.match(workflow.contents, /version: v0\.72\.0/u);
      assert.match(workflow.contents, /format: cyclonedx/u);
      assert.match(workflow.contents, /actions\/upload-artifact@[0-9a-f]{40}/u);
      assert.doesNotMatch(workflow.contents, /node-version:\s*24/u);
      assert.doesNotMatch(workflow.contents, /ignore-unfixed/u);
    }
    const publicCi = publicWorkflows.find(({ path }) => path === "ci.yml")?.contents ?? "";
    assert.match(publicCi, /pnpm licenses:node/u);
    assert.match(publicCi, /cargo install cargo-deny --locked --version 0\.19\.4/u);
    assert.match(publicCi, /cargo deny check licenses/u);
    assert.match(
      publicCi,
      /docker build --platform linux\/amd64 -f runtime\/calluwu-core\/Dockerfile/u,
    );
    const runtimeImageGate =
      publicCi.match(
        /- name: Scan runtime image[\s\S]*?- name: Prepare runtime SBOM artifact directory/u,
      )?.[0] ?? "";
    assert.notEqual(runtimeImageGate, "");
    assert.doesNotMatch(runtimeImageGate, /ignore-unfixed/u);
    const publicPublish =
      publicWorkflows.find(({ path }) => path === "publish.yml")?.contents ?? "";
    assert.match(publicPublish, /pnpm audit --prod/u);
    assert.match(publicPublish, /cargo audit --deny warnings/u);
    assert.match(publicPublish, /severity: HIGH,CRITICAL/u);
    assert.match(publicPublish, /exit-code: "1"/u);
    assert.match(publicPublish, /publish-public-packages\.mjs/u);

    const checkoutWorkflows = await workflowFiles(resolve("."));
    for (const workflow of checkoutWorkflows) {
      assertFullShaActionPins(`checkout ${workflow.path}`, workflow.contents);
      assert.doesNotMatch(workflow.contents, /ignore-unfixed/u);
    }
  });

  it("ships no founder bootstrap or projection-recovery SDK surface", async () => {
    const destination = temporaryPath("boundary");
    await exportPublicRepository({ destination });

    const sdkClient = await readFile(join(destination, "packages/sdk/src/client.ts"), "utf8");
    const sdkCli = await readFile(join(destination, "packages/sdk/src/cli.ts"), "utf8");
    const publicTypes = await readFile(join(destination, "packages/types/src/api.ts"), "utf8");
    const forbiddenTerms = [
      ["CalluwuClient", ".", "bootstrap"].join(""),
      ["/v1/", "bootstrap"].join(""),
      ["projection", "-dead-letters"].join(""),
      ["Projection", "DeadLetter"].join(""),
      ["CALLUWU_", "BOOTSTRAP_TOKEN"].join(""),
    ];
    for (const term of forbiddenTerms) {
      assert.equal(sdkClient.includes(term), false, term);
      assert.equal(sdkCli.includes(term), false, term);
      assert.equal(publicTypes.includes(term), false, term);
    }
  });
});
