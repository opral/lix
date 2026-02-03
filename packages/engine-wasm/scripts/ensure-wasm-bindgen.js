#!/usr/bin/env node
import { spawnSync } from "node:child_process";

const check = spawnSync("wasm-bindgen", ["--version"], { stdio: "ignore" });
if (check.status === 0) {
  process.exit(0);
}

const install = spawnSync("cargo", ["install", "wasm-bindgen-cli"], {
  stdio: "inherit",
});

if (install.status !== 0) {
  process.exit(install.status ?? 1);
}
