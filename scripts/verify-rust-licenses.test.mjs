import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
  parseCargoDenyVersion,
  parseDenyLicenseAllowlist,
  verifyRustPolicyConfiguration,
} from "./verify-rust-licenses.mjs";

const deny = `[licenses]
allow = [
  "Apache-2.0",
  "MIT",
]

[licenses.private]
ignore = false
`;

describe("Rust dependency license policy configuration", () => {
  it("parses the exact cargo-deny version", () => {
    assert.equal(parseCargoDenyVersion("cargo-deny 0.19.4\n"), "0.19.4");
    assert.throws(() => parseCargoDenyVersion("cargo-deny latest"), /could not parse/u);
  });

  it("requires a sorted unique deny.toml allowlist", () => {
    assert.deepEqual(parseDenyLicenseAllowlist(deny), ["Apache-2.0", "MIT"]);
    assert.throws(
      () =>
        parseDenyLicenseAllowlist(`[licenses]
allow = [
  "MIT",
  "Apache-2.0",
]
`),
      /must be sorted/u,
    );
  });

  it("rejects drift between the reviewed policy and cargo-deny", () => {
    const base = {
      schemaVersion: 1,
      rust: {
        tool: "cargo-deny",
        version: "0.19.4",
        config: "deny.toml",
        allowedLicenses: ["Apache-2.0", "MIT"],
        allowedExceptions: ["LLVM-exception"],
      },
    };
    assert.equal(verifyRustPolicyConfiguration(base, deny).version, "0.19.4");
    assert.throws(
      () =>
        verifyRustPolicyConfiguration(
          {
            ...base,
            rust: { ...base.rust, allowedLicenses: ["Apache-2.0"] },
          },
          deny,
        ),
      /allowlists differ/u,
    );
  });
});
