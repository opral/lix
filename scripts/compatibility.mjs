#!/usr/bin/env node
// Public source-checkout contract. Runs before SDK/native/Wasm builds and has
// no package dependencies. Protocol values are owned by the Rust engine.
import { readFileSync, realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const root = new URL("../", import.meta.url);

function constant(file, name) {
  const source = readFileSync(new URL(file, root), "utf8");
  const matches = [...source.matchAll(new RegExp(
    `^\\s*pub(?:\\(crate\\))?\\s+const\\s+${name}\\s*:\\s*u32\\s*=\\s*([0-9_]+)\\s*;`, "gm",
  ))];
  if (matches.length !== 1) {
    throw new Error(`Expected one canonical ${name} literal in ${file}`);
  }
  const value = Number(matches[0][1].replaceAll("_", ""));
  if (!Number.isSafeInteger(value) || value < 0 || value > 0xffff_ffff) {
    throw new Error(`Invalid canonical ${name} in ${file}`);
  }
  return value;
}

/** Read this Lix checkout's compatibility contract without loading a binding. */
export function getCompatibility() {
  return Object.freeze({
    serverProtocolVersion: constant("packages/lix/src/lib.rs", "SERVER_PROTOCOL_VERSION"),
    syncProtocolVersion: constant("packages/lix/src/sync/mod.rs", "SYNC_PROTOCOL_VERSION"),
    storageFormatVersion: constant("packages/lix/src/init.rs", "CURRENT_FORMAT_VERSION"),
  });
}

if (process.argv[1] && realpathSync(resolve(process.argv[1])) === realpathSync(fileURLToPath(import.meta.url))) {
  process.stdout.write(`${JSON.stringify(getCompatibility())}\n`);
}
