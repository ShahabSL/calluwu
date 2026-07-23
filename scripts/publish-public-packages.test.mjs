import assert from "node:assert/strict";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import test from "node:test";

import { publishPublicPackages, tarballIntegrity } from "./publish-public-packages.mjs";

const packageFiles = [
  "calluwu-types-0.1.0.tgz",
  "calluwu-webrtc-client-0.1.0.tgz",
  "calluwu-sdk-0.1.0.tgz",
];

async function releaseDirectory() {
  const directory = await mkdtemp(join(tmpdir(), "calluwu-release-"));
  await Promise.all(
    packageFiles.map((file, index) => writeFile(join(directory, file), `tarball-${index}`)),
  );
  return directory;
}

function registryResponse(status, integrity) {
  return {
    ok: status >= 200 && status < 300,
    status,
    async json() {
      return { dist: { integrity } };
    },
  };
}

test("skips only exact immutable versions and publishes missing packages in order", async () => {
  const directory = await releaseDirectory();
  const integrities = packageFiles.map((_, index) =>
    tarballIntegrity(Buffer.from(`tarball-${index}`)),
  );
  const published = new Map([["%40calluwu%2Ftypes", integrities[0]]]);
  const commands = [];
  const fetchImpl = async (url) => {
    const encodedName = new URL(url).pathname.split("/")[1];
    const integrity = published.get(encodedName);
    return integrity ? registryResponse(200, integrity) : registryResponse(404);
  };
  const runCommand = async (_command, args) => {
    commands.push(basename(args[1]));
    const encodedName = commands.length === 1 ? "%40calluwu%2Fwebrtc-client" : "%40calluwu%2Fsdk";
    const integrity = commands.length === 1 ? integrities[1] : integrities[2];
    published.set(encodedName, integrity);
  };

  const results = await publishPublicPackages("0.1.0", {
    releaseDirectory: directory,
    fetchImpl,
    runCommand,
  });

  assert.deepEqual(commands, ["calluwu-webrtc-client-0.1.0.tgz", "calluwu-sdk-0.1.0.tgz"]);
  assert.deepEqual(
    results.map(({ name, status }) => ({ name, status })),
    [
      { name: "@calluwu/types", status: "verified-existing" },
      { name: "@calluwu/webrtc-client", status: "published" },
      { name: "@calluwu/sdk", status: "published" },
    ],
  );
});

test("fails closed when an immutable version has different bytes", async () => {
  await assert.rejects(
    publishPublicPackages("0.1.0", {
      releaseDirectory: await releaseDirectory(),
      fetchImpl: async () => registryResponse(200, "sha512-different"),
      runCommand: async () => assert.fail("publish must not run"),
    }),
    /different immutable content/u,
  );
});

test("accepts a lost publish response only when npm exposes identical bytes", async () => {
  const directory = await releaseDirectory();
  const integrities = packageFiles.map((_, index) =>
    tarballIntegrity(Buffer.from(`tarball-${index}`)),
  );
  const published = new Map();
  let commandIndex = 0;
  const fetchImpl = async (url) => {
    const encodedName = new URL(url).pathname.split("/")[1];
    const integrity = published.get(encodedName);
    return integrity ? registryResponse(200, integrity) : registryResponse(404);
  };
  const runCommand = async () => {
    const names = ["%40calluwu%2Ftypes", "%40calluwu%2Fwebrtc-client", "%40calluwu%2Fsdk"];
    published.set(names[commandIndex], integrities[commandIndex]);
    commandIndex += 1;
    if (commandIndex === 1) throw new Error("connection reset after upload");
  };

  const results = await publishPublicPackages("0.1.0", {
    releaseDirectory: directory,
    fetchImpl,
    runCommand,
  });
  assert.equal(results.length, 3);
  assert.equal(results[0].status, "published");
});

test("rejects non-canonical versions before filesystem or network access", async () => {
  await assert.rejects(publishPublicPackages("v0.1.0"), /canonical semantic version/u);
});
