import { spawn } from "node:child_process";
import { mkdirSync, writeFileSync, existsSync, cpSync } from "node:fs";
import { availableParallelism } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export function sdkBuildPlan(runtime, root, env = process.env, cpus = availableParallelism()) {
  if (!["native", "browser"].includes(runtime)) throw new Error(`Invalid SDK runtime: ${runtime}`);
  const phases = [
    ...(runtime === "native" ? [
      ["native", "build-native.js", { LIX_OFFLINE_MIGRATION: "0" }],
      ["migration-native", "build-native.js", { LIX_OFFLINE_MIGRATION: "1" }],
    ] : []),
    ["wasm", "build-wasm.js", { LIX_OFFLINE_MIGRATION: "0", LIX_WASM_PROFILE: runtime === "native" ? "dev" : "release" }],
    ["migration-wasm", "build-wasm.js", { LIX_OFFLINE_MIGRATION: "1", LIX_WASM_PROFILE: runtime === "native" ? "dev" : "release" }],
    ["plugins", "build-bundled-plugins.js", { LIX_OFFLINE_MIGRATION: "0" }],
  ];
  // Isolate target directories: sharing one would serialize Cargo on its lock
  // and allow regular/migration cdylibs to overwrite each other before copying.
  // Keep the sum of Cargo job limits within this single runner's CPU budget.
  const jobs = Math.max(1, Math.floor(cpus / phases.length));
  return phases.map(([name, script, overrides]) => ({
    name, script: join(root, "packages/js-sdk/scripts", script),
    env: { ...env, ...overrides, CARGO_BUILD_JOBS: String(jobs),
      CARGO_TARGET_DIR: join(env.RUNNER_TEMP || join(root, "target"), "ci-sdk", runtime, name) },
  }));
}

export async function executeBuildPlan(plan, run) {
  const results = await Promise.allSettled(plan.map(async phase => {
    const start = performance.now();
    try {
      await run(phase);
      return { name: phase.name, seconds: (performance.now() - start) / 1000, jobs: Number(phase.env.CARGO_BUILD_JOBS) };
    } catch (error) {
      throw new Error(`${phase.name}: ${error.message}`);
    }
  }));
  const failed = results.filter(result => result.status === "rejected");
  if (failed.length) throw new AggregateError(failed.map(result => result.reason), "SDK build phases failed");
  return results.map(result => result.value);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const runtime = process.argv[2];
  const root = process.cwd();
  const plan = sdkBuildPlan(runtime, root);
  const report = resolve("ci-sdk-timings", runtime);
  mkdirSync(report, { recursive: true });
  const run = phase => new Promise((resolve, reject) => {
    console.log(`Starting ${phase.name} (${phase.env.CARGO_BUILD_JOBS} Cargo jobs)`);
    const start = performance.now();
    const child = spawn(process.execPath, [phase.script], { cwd: root, env: phase.env, stdio: "inherit" });
    child.on("error", reject);
    child.on("exit", (code, signal) => {
      const seconds = (performance.now() - start) / 1000;
      console.log(`Finished ${phase.name}: ${seconds.toFixed(1)}s, exit=${code}, signal=${signal}`);
      writeFileSync(join(report, `${phase.name}.json`), JSON.stringify({ name: phase.name, seconds, code, signal, jobs: Number(phase.env.CARGO_BUILD_JOBS) }, null, 2));
      const timings = join(phase.env.CARGO_TARGET_DIR, "cargo-timings");
      if (existsSync(timings)) cpSync(timings, join(report, phase.name), { recursive: true });
      if (code === 0) resolve(); else reject(new Error(`exited ${code ?? signal}`));
    });
  });
  await executeBuildPlan(plan, run);
}
