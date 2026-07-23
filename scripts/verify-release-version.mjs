#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packagePaths = [
  "packages/types/package.json",
  "packages/webrtc-client/package.json",
  "packages/sdk/package.json",
];

export async function verifyReleaseVersion(tag) {
  if (
    typeof tag !== "string" ||
    !/^v(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?$/u.test(tag)
  ) {
    throw new Error("release tag must be a canonical v-prefixed semantic version");
  }

  const packages = await Promise.all(
    packagePaths.map(async (path) => {
      const value = JSON.parse(await readFile(resolve(repositoryRoot, path), "utf8"));
      return { path, value };
    }),
  );
  const expectedVersion = tag.slice(1);
  for (const { path, value } of packages) {
    if (value.version !== expectedVersion) {
      throw new Error(`${path} version must equal release tag ${tag}`);
    }
    if (value.private === true || value.license !== "Apache-2.0") {
      throw new Error(`${path} is not configured as a public Apache-2.0 package`);
    }
    if (
      value.repository?.url !== "git+https://github.com/ShahabSL/calluwu.git" ||
      value.publishConfig?.access !== "public"
    ) {
      throw new Error(`${path} does not declare the reviewed public repository and access policy`);
    }
  }
  return { tag, version: expectedVersion, packages: packagePaths };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  verifyReleaseVersion(process.argv[2]).then(
    ({ version }) => console.log(`verified public package release ${version}`),
    (error) => {
      console.error(error instanceof Error ? error.message : String(error));
      process.exitCode = 1;
    },
  );
}
