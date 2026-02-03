#!/usr/bin/env node
import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const crateDir = join(repoRoot, "packages", "engine-wasm");
const targetDir = join(repoRoot, "target", "wasm32-unknown-unknown", "debug");
const wasmPath = join(targetDir, "lix_engine_wasm.wasm");
const outDir = join(crateDir, "dist", "wasm");

function run(cmd, args, opts = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(cmd, args, { stdio: "inherit", ...opts });
    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`${cmd} exited with code ${code ?? 1}`));
    });
  });
}

async function main() {
  await run("cargo", ["build", "-p", "lix_engine_wasm", "--target", "wasm32-unknown-unknown"]);

  await run("wasm-bindgen", [
    wasmPath,
    "--target",
    "web",
    "--out-dir",
    outDir,
  ]);

  await run("node", [join(crateDir, "scripts", "embed-engine-wasm.js")]);

  const wrapper = `import init, * as mod from "./wasm/lix_engine_wasm.js";\nexport * from "./wasm/lix_engine_wasm.js";\nexport { wasmBinary } from "./engine-wasm-binary.js";\nexport default init;\n`;
  await mkdir(join(crateDir, "dist"), { recursive: true });
  await writeFile(join(crateDir, "dist", "index.js"), wrapper);
}

main().catch((err) => {
  console.error("[engine-wasm] build failed:\n", err);
  process.exit(1);
});
