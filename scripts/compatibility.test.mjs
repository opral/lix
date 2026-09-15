import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, cpSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { getCompatibility } from "./compatibility.mjs";

test("checkout CLI is independent of cwd and matches the import contract", () => {
  const script = fileURLToPath(new URL("./compatibility.mjs", import.meta.url));
  assert.deepEqual(JSON.parse(execFileSync(process.execPath, [script], { cwd: tmpdir(), encoding: "utf8" })), getCompatibility());
  assert.ok(Object.isFrozen(getCompatibility()));
});

test("SDK metadata derives from changed engine constants and imports without bindings", async () => {
  const root = mkdtempSync(join(tmpdir(), "lix-compatibility-"));
  try {
    for (const path of ["scripts", "packages/lix/src/sync", "packages/js-sdk/scripts"]) {
      mkdirSync(join(root, path), { recursive: true });
    }
    cpSync(new URL("./compatibility.mjs", import.meta.url), join(root, "scripts/compatibility.mjs"));
    cpSync(new URL("../packages/js-sdk/scripts/build-compatibility.js", import.meta.url), join(root, "packages/js-sdk/scripts/build-compatibility.js"));
    writeFileSync(join(root, "package.json"), '{"type":"module"}');
    writeFileSync(join(root, "packages/lix/src/lib.rs"), "pub const SERVER_PROTOCOL_VERSION: u32 = 123;\n");
    writeFileSync(join(root, "packages/lix/src/sync/mod.rs"), "pub(crate) const SYNC_PROTOCOL_VERSION: u32 = 4_567;\n");
    writeFileSync(join(root, "packages/lix/src/init.rs"), "pub(crate) const CURRENT_FORMAT_VERSION: u32 = 891;\n");
    execFileSync(process.execPath, [join(root, "packages/js-sdk/scripts/build-compatibility.js")], { cwd: tmpdir() });
    const { compatibility } = await import(pathToFileURL(join(root, "packages/js-sdk/dist/compatibility.js")));
    assert.deepEqual(compatibility, { serverProtocolVersion: 123, syncProtocolVersion: 4567, storageFormatVersion: 891 });
    assert.ok(Object.isFrozen(compatibility));
    assert.match(readFileSync(join(root, "packages/js-sdk/dist/compatibility.d.ts"), "utf8"), /serverProtocolVersion: number/);
    // Expressions or moved definitions must fail closed instead of emitting
    // stale or guessed protocol versions.
    writeFileSync(join(root, "packages/lix/src/init.rs"), "pub(crate) const CURRENT_FORMAT_VERSION: u32 = other::VERSION;\n");
    assert.throws(() => execFileSync(process.execPath, [join(root, "scripts/compatibility.mjs")], { stdio: "pipe" }), /Command failed/);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
