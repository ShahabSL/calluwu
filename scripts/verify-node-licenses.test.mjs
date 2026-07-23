import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  buildManifestFallbackReport,
  validateDependencyPolicy,
  verifyPnpmLicenseReport,
} from "./verify-node-licenses.mjs";

const policy = validateDependencyPolicy({
  schemaVersion: 1,
  node: {
    tool: "pnpm",
    version: "11.9.0",
    command: ["licenses", "list", "--prod", "--json"],
    fallback: {
      trigger: "ERR_PNPM_MISSING_PACKAGE_INDEX_FILE",
      installedCommand: ["list", "--prod", "--recursive", "--depth", "Infinity", "--json"],
      lockfileCommand: [
        "list",
        "--prod",
        "--recursive",
        "--depth",
        "Infinity",
        "--json",
        "--lockfile-only",
      ],
      licenseOverrides: [
        {
          version: "1.0.0",
          license: "MIT",
          reason:
            "The target-specific package is locked but intentionally absent on other host platforms.",
          packages: ["target-binary"],
        },
      ],
    },
    allowedLicenses: ["Apache-2.0", "BSD-3-Clause", "MIT"],
    allowedExceptions: ["LLVM-exception"],
  },
});

function packageEntry(name = "dependency", versions = ["1.0.0"]) {
  return {
    name,
    versions,
    paths: versions.map((version) => `/store/${name}/${version}`),
  };
}

describe("Node production dependency license policy", () => {
  it("accepts only well-formed expressions whose every term is explicitly allowed", () => {
    const result = verifyPnpmLicenseReport(
      {
        "(Apache-2.0 OR MIT) AND BSD-3-Clause": [packageEntry("dual")],
        "Apache-2.0 WITH LLVM-exception": [packageEntry("exception")],
        MIT: [packageEntry("mit", ["1.0.0", "2.0.0"])],
      },
      policy,
    );

    assert.deepEqual(result, {
      packages: 4,
      expressions: [
        "(Apache-2.0 OR MIT) AND BSD-3-Clause",
        "Apache-2.0 WITH LLVM-exception",
        "MIT",
      ],
    });
  });

  for (const expression of [
    "GPL-3.0-only",
    "LGPL-2.1-or-later",
    "MPL-2.0",
    "MIT OR GPL-3.0-only",
    "UNKNOWN",
    "SEE LICENSE IN LICENSE",
    "",
  ]) {
    it(`fails closed for ${JSON.stringify(expression)}`, () => {
      assert.throws(
        () => verifyPnpmLicenseReport({ [expression]: [packageEntry()] }, policy),
        /license policy failed/u,
      );
    });
  }

  it("rejects empty, malformed, or duplicate package evidence", () => {
    assert.throws(() => verifyPnpmLicenseReport({}, policy), /empty or malformed/u);
    assert.throws(
      () =>
        verifyPnpmLicenseReport(
          {
            MIT: [{ name: "bad", versions: ["1.0.0"], paths: [] }],
          },
          policy,
        ),
      /malformed package metadata/u,
    );
    assert.throws(
      () =>
        verifyPnpmLicenseReport(
          {
            "Apache-2.0": [packageEntry("duplicate")],
            MIT: [packageEntry("duplicate")],
          },
          policy,
        ),
      /appears more than once/u,
    );
  });

  it("rejects an unpinned or drifted scanner policy", () => {
    assert.throws(
      () =>
        validateDependencyPolicy({
          schemaVersion: 1,
          node: {
            tool: "pnpm",
            version: "latest",
            command: ["licenses", "list", "--prod", "--json"],
            fallback: policy.fallback,
            allowedLicenses: ["MIT"],
            allowedExceptions: ["LLVM-exception"],
          },
        }),
      /exact pnpm version/u,
    );
    assert.throws(
      () =>
        validateDependencyPolicy({
          schemaVersion: 1,
          node: {
            tool: "pnpm",
            version: "11.9.0",
            command: ["licenses", "list", "--json"],
            fallback: policy.fallback,
            allowedLicenses: ["MIT"],
            allowedExceptions: ["LLVM-exception"],
          },
        }),
      /must inspect production dependencies/u,
    );
  });

  it("builds fallback evidence only from identical lock/install graphs and exact manifests", async () => {
    const graph = [
      {
        path: "/workspace/app",
        dependencies: {
          library: {
            from: "library",
            version: "2.0.0",
            resolved: "https://registry.npmjs.org/library/-/library-2.0.0.tgz",
            path: "/store/library",
          },
          "target-binary": {
            from: "target-binary",
            version: "1.0.0",
            resolved: "https://registry.npmjs.org/target-binary/-/target-binary-1.0.0.tgz",
            path: "/store/target-binary",
          },
          workspace: {
            from: "workspace",
            version: "link:../workspace",
            path: "/workspace/package",
            dependencies: {
              transitive: {
                from: "transitive",
                version: "3.0.0",
                resolved: "https://registry.npmjs.org/transitive/-/transitive-3.0.0.tgz",
                path: "/store/transitive",
              },
            },
          },
        },
      },
    ];
    const manifests = new Map([
      [
        "library@2.0.0",
        {
          path: "/store/library",
          manifest: { name: "library", version: "2.0.0", license: "Apache-2.0" },
        },
      ],
      [
        "transitive@3.0.0",
        {
          path: "/store/transitive",
          manifest: { name: "transitive", version: "3.0.0", license: "BSD-3-Clause" },
        },
      ],
    ]);

    const report = await buildManifestFallbackReport({
      lockfileReport: graph,
      installedReport: graph,
      policy,
      manifestReader: async (dependency) => manifests.get(dependency.identity) ?? null,
    });
    assert.deepEqual(verifyPnpmLicenseReport(report, policy), {
      packages: 3,
      expressions: ["Apache-2.0", "BSD-3-Clause", "MIT"],
    });
  });

  it("fails the fallback on graph drift, missing evidence, or stale overrides", async () => {
    const locked = [
      {
        path: "/workspace/app",
        dependencies: {
          dependency: {
            from: "dependency",
            version: "1.0.0",
            resolved: "https://registry.npmjs.org/dependency/-/dependency-1.0.0.tgz",
            path: "/store/dependency",
          },
        },
      },
    ];
    await assert.rejects(
      buildManifestFallbackReport({
        lockfileReport: locked,
        installedReport: [{ path: "/workspace/app" }],
        policy,
        manifestReader: async () => null,
      }),
      /installed production graph/u,
    );

    const noOverrides = {
      ...policy,
      fallback: { ...policy.fallback, licenseOverrides: [] },
    };
    await assert.rejects(
      buildManifestFallbackReport({
        lockfileReport: locked,
        installedReport: locked,
        policy: noOverrides,
        manifestReader: async () => null,
      }),
      /no installed package manifest or exact reviewed override/u,
    );
    await assert.rejects(
      buildManifestFallbackReport({
        lockfileReport: locked,
        installedReport: locked,
        policy,
        manifestReader: async () => ({
          path: "/store/dependency",
          manifest: { name: "dependency", version: "1.0.0", license: "MIT" },
        }),
      }),
      /overrides are stale/u,
    );
  });
});
