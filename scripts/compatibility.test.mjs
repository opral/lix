import { test } from "node:test";
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync, cpSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { getCompatibility } from "./compatibility.mjs";
import { validateProtocolVersionContract } from "./validate-server-protocol-docs.mjs";

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

test("OpenAPI and protocol prose stay aligned with Rust-owned wire versions", () => {
  const openApi = readFileSync(new URL("../packages/lix/server-protocol.openapi.yaml", import.meta.url), "utf8");
  const serverDoc = readFileSync(new URL("../docs/server-protocol.md", import.meta.url), "utf8");
  const admissionDoc = readFileSync(new URL("../docs/architecture/http-admission.md", import.meta.url), "utf8");
  assert.equal(validateProtocolVersionContract(openApi, serverDoc, admissionDoc, getCompatibility()), true);
});

test("protocol contract validation rejects missing or stale header and epoch versions", () => {
  const openApi = readFileSync(new URL("../packages/lix/server-protocol.openapi.yaml", import.meta.url), "utf8");
  const serverDoc = readFileSync(new URL("../docs/server-protocol.md", import.meta.url), "utf8");
  const admissionDoc = readFileSync(new URL("../docs/architecture/http-admission.md", import.meta.url), "utf8");
  const compatibility = getCompatibility();
  const staleVersion = (current) => current > 0 ? current - 1 : current + 1;
  const staleServerVersion = staleVersion(compatibility.serverProtocolVersion);
  const staleSyncVersion = staleVersion(compatibility.syncProtocolVersion);

  assert.throws(
    () => validateProtocolVersionContract(
      openApi.replace(
        `const: ${compatibility.serverProtocolVersion} }`,
        `const: ${staleServerVersion} }`,
      ),
      serverDoc,
      admissionDoc,
      compatibility,
    ),
    new RegExp(`ServerProtocolVersion const is ${staleServerVersion}; canonical version is ${compatibility.serverProtocolVersion}`),
  );
  assert.throws(
    () => validateProtocolVersionContract(
      openApi.replace(
        `const: ${compatibility.syncProtocolVersion} }`,
        `const: ${staleSyncVersion} }`,
      ),
      serverDoc,
      admissionDoc,
      compatibility,
    ),
    new RegExp(`SyncProtocolVersion const is ${staleSyncVersion}; canonical version is ${compatibility.syncProtocolVersion}`),
  );
  assert.throws(
    () => validateProtocolVersionContract(openApi.replace("    SyncProtocolVersion:\n", "    RemovedSyncProtocolVersion:\n"), serverDoc, admissionDoc, compatibility),
    /has no SyncProtocolVersion parameter/,
  );
  assert.throws(
    () => validateProtocolVersionContract(openApi.replace(
      "      required: true\n      description: Exact SQL protocol version",
      "      required: false\n      description: Exact SQL protocol version",
    ), serverDoc, admissionDoc, compatibility),
    /ServerProtocolVersion must be the required lix-server-protocol-version header/,
  );
  assert.throws(
    () => validateProtocolVersionContract(
      openApi,
      serverDoc.replace(
        `lix-sync-protocol-version: ${compatibility.syncProtocolVersion}`,
        `lix-sync-protocol-version: ${staleSyncVersion}`,
      ),
      admissionDoc,
      compatibility,
    ),
    new RegExp(`docs/server-protocol\\.md documents lix-sync-protocol-version version\\(s\\) ${staleSyncVersion}; canonical version is ${compatibility.syncProtocolVersion}`),
  );
  assert.throws(
    () => validateProtocolVersionContract(
      openApi,
      serverDoc.replace(
        `lix-server-protocol-version: ${compatibility.serverProtocolVersion}`,
        `lix-server-protocol-version: ${staleServerVersion}`,
      ),
      admissionDoc,
      compatibility,
    ),
    new RegExp(`docs/server-protocol\\.md documents lix-server-protocol-version version\\(s\\) ${staleServerVersion}; canonical version is ${compatibility.serverProtocolVersion}`),
  );
  assert.throws(
    () => validateProtocolVersionContract(
      openApi,
      serverDoc,
      admissionDoc.replace(
        `protocolEpoch: ${compatibility.syncProtocolVersion}`,
        `protocolEpoch: ${staleSyncVersion}`,
      ),
      compatibility,
    ),
    new RegExp(`docs/architecture/http-admission\\.md documents protocolEpoch value\\(s\\) ${staleSyncVersion}; canonical version is ${compatibility.syncProtocolVersion}`),
  );
});
