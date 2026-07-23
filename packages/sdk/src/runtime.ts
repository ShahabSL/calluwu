import { spawn } from "node:child_process";
import { access, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { AgentManifest } from "@calluwu/types";

type RuntimeInvocation = { command: string; args: string[]; cwd?: string };

async function exists(path: string): Promise<boolean> {
  return access(path).then(
    () => true,
    () => false,
  );
}

async function findWorkspace(start: string): Promise<string | undefined> {
  let current = resolve(start);
  for (;;) {
    if (await exists(join(current, "runtime", "calluwu-core", "Cargo.toml"))) {
      return current;
    }
    const parent = dirname(current);
    if (parent === current) return undefined;
    current = parent;
  }
}

async function resolveRuntime(): Promise<RuntimeInvocation> {
  if (process.env.CALLUWU_RUNTIME_BIN) {
    return { command: process.env.CALLUWU_RUNTIME_BIN, args: [] };
  }
  const workspace = await findWorkspace(process.cwd());
  if (workspace) {
    const binary = join(workspace, "target", "debug", "calluwu-runtime");
    if (await exists(binary)) return { command: binary, args: [], cwd: workspace };
    return {
      command: "cargo",
      args: ["run", "--quiet", "-p", "calluwu-core", "--bin", "calluwu-runtime", "--"],
      cwd: workspace,
    };
  }

  const paths = (process.env.PATH ?? "").split(delimiter);
  for (const path of paths) {
    const binary = join(path, "calluwu-runtime");
    if (await exists(binary)) return { command: binary, args: [] };
  }
  throw new Error(
    `Unable to find calluwu-runtime from ${fileURLToPath(import.meta.url)}; set CALLUWU_RUNTIME_BIN`,
  );
}

export async function runScriptedSimulation(
  manifest: AgentManifest,
  input: string,
  options: { eventsPath?: string } = {},
): Promise<number> {
  const directory = await mkdtemp(join(tmpdir(), "calluwu-runtime-"));
  const manifestPath = join(directory, "manifest.json");
  await writeFile(manifestPath, JSON.stringify(manifest), { encoding: "utf8", mode: 0o600 });
  const runtime = await resolveRuntime();
  const args = [...runtime.args, "simulate", "--agent-manifest", manifestPath, "--input", input];
  if (options.eventsPath) args.push("--events", options.eventsPath);

  try {
    return await new Promise<number>((resolveExit, reject) => {
      const child = spawn(runtime.command, args, {
        cwd: runtime.cwd,
        stdio: "inherit",
        env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "calluwu=info" },
      });
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        if (signal) reject(new Error(`calluwu-runtime exited from signal ${signal}`));
        else resolveExit(code ?? 1);
      });
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}

export async function runLocalRealtimeServer(
  manifest: AgentManifest,
  options: { bind?: string } = {},
): Promise<number> {
  const directory = await mkdtemp(join(tmpdir(), "calluwu-runtime-"));
  const manifestPath = join(directory, "manifest.json");
  await writeFile(manifestPath, JSON.stringify(manifest), { encoding: "utf8", mode: 0o600 });
  const runtime = await resolveRuntime();
  const args = [
    ...runtime.args,
    "serve",
    "--bind",
    options.bind ?? "127.0.0.1:0",
    "--max-sessions",
    "1",
    "--agent-manifest",
    manifestPath,
  ];

  try {
    return await new Promise<number>((resolveExit, reject) => {
      const child = spawn(runtime.command, args, {
        cwd: runtime.cwd,
        stdio: "inherit",
        env: { ...process.env, RUST_LOG: process.env.RUST_LOG ?? "calluwu=info" },
      });
      child.once("error", reject);
      child.once("exit", (code, signal) => {
        if (signal && signal !== "SIGINT" && signal !== "SIGTERM") {
          reject(new Error(`calluwu-runtime exited from signal ${signal}`));
        } else {
          resolveExit(code ?? (signal ? 0 : 1));
        }
      });
    });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
}
