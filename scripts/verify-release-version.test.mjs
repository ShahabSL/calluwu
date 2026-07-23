import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { verifyReleaseVersion } from "./verify-release-version.mjs";

describe("public package release version", () => {
  it("accepts only the exact shared package version", async () => {
    const verified = await verifyReleaseVersion("v0.1.0");
    assert.equal(verified.version, "0.1.0");
    await assert.rejects(verifyReleaseVersion("0.1.0"), /canonical/u);
    await assert.rejects(verifyReleaseVersion("v0.1.1"), /version must equal/u);
  });
});
