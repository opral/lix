import assert from "node:assert/strict";
import { mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createRequire } from "node:module";
import { test } from "node:test";
import { archivePath, componentApi, packagePlugin, repositoryRoot } from "./plugin-archive.mjs";

const require = createRequire(new URL("../packages/js-sdk/package.json", import.meta.url));
const { unzipSync } = require("fflate");
const fixture = join(repositoryRoot, "packages/lix/tests/fixtures/plugin-api/v2/plugin_csv.lixplugin");
const legacyFixture = join(repositoryRoot, "packages/lix/tests/fixtures/plugin-api/legacy-v2/plugin_csv.lixplugin");
const wasm = unzipSync(await readFile(fixture))["plugin.wasm"];

async function temporaryPlugin(fn) {
  const dir = await mkdtemp(join(tmpdir(), "lix-package-test-"));
  try {
    await mkdir(join(dir, "schema"));
    await writeFile(join(dir, "schema/row.json"), '{"key":"example"}');
    const manifest = { key: "plugin_test", entry: "plugin.wasm", schemas: ["schema/row.json"] };
    const save = () => writeFile(join(dir, "manifest.json"), JSON.stringify(manifest));
    await save();
    await fn(dir, manifest, save);
  } finally { await rm(dir, { recursive: true, force: true }); }
}

test("archive names reject traversal, absolute paths and ambiguous separators", () => {
  for (const path of ["../outside", "/outside", "a/../b", "a//b", "./a", "a\\b", "C:foo", "", null]) {
    assert.throws(() => archivePath(path), /Invalid archive path/);
  }
  assert.equal(archivePath("schema/row.json"), "schema/row.json");
});

test("API metadata is extracted from actual canonical and legacy compiled Components", async () => {
  assert.deepEqual(await componentApi(wasm), { apiMajor: 2, apiIdentity: "lix:plugin-v2" });
  const legacy = unzipSync(await readFile(legacyFixture))["plugin.wasm"];
  assert.deepEqual(await componentApi(legacy), { apiMajor: 2, apiIdentity: "lix:plugin@2.0.0" });
  await assert.rejects(componentApi(new Uint8Array([0, 1, 2])));
});

test("manifest-driven package has exact entries, repeatable bytes and checksum", async () => {
  await temporaryPlugin(async (dir) => {
    const previousTz = process.env.TZ;
    let first, second;
    try {
      process.env.TZ = "UTC";
      first = await packagePlugin(dir, "plugin_test", wasm);
      process.env.TZ = "America/Los_Angeles";
      second = await packagePlugin(dir, "plugin_test", wasm);
    } finally {
      if (previousTz === undefined) delete process.env.TZ;
      else process.env.TZ = previousTz;
    }
    assert.deepEqual(first.archive, second.archive);
    assert.equal(first.sha256, second.sha256);
    assert.match(first.sha256, /^[a-f0-9]{64}$/);
    const entries = unzipSync(first.archive);
    assert.deepEqual(Object.keys(entries), ["manifest.json", "plugin.wasm", "schema/row.json"]);
    assert.deepEqual(entries["plugin.wasm"], wasm);
    assert.equal(new TextDecoder().decode(entries["schema/row.json"]), '{"key":"example"}');
  });
});

test("packager rejects duplicate entries and mismatched keys", async () => {
  await temporaryPlugin(async (dir, manifest, save) => {
    await assert.rejects(packagePlugin(dir, "plugin_other", wasm), /Manifest key/);
    manifest.schemas.push("plugin.wasm");
    await save();
    await assert.rejects(packagePlugin(dir, "plugin_test", wasm), /Duplicate/);
  });
});

test("manifest sources cannot escape through symlinks", async () => {
  await temporaryPlugin(async (dir, manifest, save) => {
    await symlink(join(repositoryRoot, "Cargo.toml"), join(dir, "outside.json"));
    manifest.schemas = ["outside.json"];
    await save();
    await assert.rejects(packagePlugin(dir, "plugin_test", wasm), /escapes plugin directory/);
  });
});
