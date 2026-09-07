import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";
import test from "node:test";

for (const target of [undefined, "aarch64-unknown-linux-gnu"]) {
  test(`native build selects the ${target ?? "host"} artifact`, { skip: process.platform === "win32" }, () => {
    const root = mkdtempSync(join(tmpdir(), "lix-native-build-"));
    try {
      mkdirSync(join(root, "scripts"));
      copyFileSync(new URL("../packages/js-sdk/scripts/build-native.js", import.meta.url), join(root, "scripts/build-native.mjs"));
      const cargo = join(root, "cargo");
      writeFileSync(cargo, `#!${process.execPath}
const fs = require('node:fs');
const path = require('node:path');
const root = __dirname;
const args = process.argv.slice(2);
if (args[0] === 'metadata') {
  console.log(JSON.stringify({target_directory: path.join(root, 'target')}));
} else {
  fs.writeFileSync(path.join(root, 'args.json'), JSON.stringify(args));
  const target = args.includes('--target') ? args[args.indexOf('--target') + 1] : '';
  const dir = path.join(root, 'target', target, 'release');
  fs.mkdirSync(dir, {recursive: true});
  const artifact = target || process.platform === 'linux' ? 'liblix_js_sdk.so' : 'liblix_js_sdk.dylib';
  fs.writeFileSync(path.join(dir, artifact), target || 'host');
}
`);
      chmodSync(cargo, 0o755);
      // A stale host binary must never be packaged as ARM64.
      mkdirSync(join(root, "target/release"), { recursive: true });
      writeFileSync(join(root, "target/release/liblix_js_sdk.so"), "stale host");
      const env = { ...process.env, PATH: `${root}${delimiter}${process.env.PATH}` };
      delete env.LIX_NATIVE_TARGET;
      delete env.LIX_NATIVE_PROFILE;
      if (target) env.LIX_NATIVE_TARGET = target;
      const result = spawnSync(process.execPath, [join(root, "scripts/build-native.mjs")], { env, encoding: "utf8" });
      assert.equal(result.status, 0, result.stderr);
      assert.equal(readFileSync(join(root, "lix_js_sdk.node"), "utf8"), target ?? "host");
      const args = JSON.parse(readFileSync(join(root, "args.json"), "utf8"));
      assert.equal(args.includes("--target"), Boolean(target));
      assert.equal(args[args.indexOf("--profile") + 1], "release");
    } finally {
      rmSync(root, { recursive: true, force: true });
    }
  });
}
