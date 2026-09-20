#!/usr/bin/env node
// Test the published package layout, without the detached migration addon.
import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
const sdk = join(dirname(fileURLToPath(import.meta.url)), "..");
const temporary = mkdtempSync(join(tmpdir(), "lix-packed-native-open-"));
const run = (command, args, cwd = sdk) => execFileSync(command, args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "inherit"] });
const pack = (directory) => join(temporary, JSON.parse(run("npm", ["pack", "--json", "--pack-destination", temporary], directory))[0].filename);
try {
  const nativeDirectory = run(process.execPath, ["scripts/prepare-native-package.js", `--out=${join(temporary, "native")}`]).trim();
  const app = join(temporary, "app");
  mkdirSync(app);
  writeFileSync(join(app, "package.json"), '{"private":true,"type":"module"}\n');
  run("npm", ["install", "--ignore-scripts", "--no-audit", "--no-fund", "--omit=optional", pack(sdk), pack(nativeDirectory), pack(join(sdk, "../storage-filesystem"))], app);
  const repository = join(app, "repository");
  mkdirSync(repository);
  run("tar", ["-xzf", join(sdk, "test-fixtures/filesystem-v0.16.0/repository.tar.gz"), "-C", repository]);
  writeFileSync(join(app, "verify.mjs"), `
import assert from 'node:assert/strict';
import {openLix} from '@lix-js/sdk';
import {FilesystemStorage} from '@lix-js/storage-filesystem';
const events = [];
const storage = () => new FilesystemStorage({path: './repository'});
const lix = await openLix({storage: storage(), onProgress: event => events.push(event)});
try {
  assert.equal(lix.openReport.initialized, false);
  assert.equal(lix.openReport.migrations.length, 1);
  assert.equal(lix.openReport.migrations[0].scope, 'local');
  assert.equal(lix.openReport.migrations[0].fromFormat, 78);
  assert.equal(lix.openReport.migrations[0].toFormat, lix.openReport.format);
  assert.ok(events.some(event => event.phase === 'migrating' && event.scope === 'local'));
  const working = await lix.execute("SELECT content FROM lix_file WHERE path = '/notes.md'");
  assert.equal(new TextDecoder().decode(working.rows[0].content), '# Notes\\n\\nWorking paragraph.\\n');
  const checkpoints = await lix.execute('SELECT commit_id AS id FROM lix_log() WHERE is_checkpoint');
  assert.equal(checkpoints.rows.length, 1);
  const nodes = await lix.execute('SELECT kind FROM markdown_node ORDER BY kind');
  assert.deepEqual(nodes.rows.map(row => row.kind), ['document', 'heading', 'paragraph']);
  const historicalNodes = await lix.execute("SELECT kind FROM lix_as_of('markdown_node', $1) ORDER BY kind", [checkpoints.rows[0].id]);
  assert.deepEqual(historicalNodes.rows.map(row => row.kind), ['document', 'heading', 'paragraph']);
  const historical = await lix.execute("SELECT content FROM lix_as_of('lix_file', $1) WHERE path = '/notes.md'", [checkpoints.rows[0].id]);
  assert.equal(new TextDecoder().decode(historical.rows[0].content), '# Notes\\n\\nCheckpoint paragraph.\\n');
} finally { await lix.close(); }
const reopened = await openLix({storage: storage()});
try { assert.deepEqual(reopened.openReport.migrations, []); }
finally { await reopened.close(); }
console.log('Released 0.16.0 filesystem upgrade through installed native package passed');
`);
  const result = run(process.execPath, ["verify.mjs"], app);
  assert.match(result, /passed/);
  process.stdout.write(result);
} finally { rmSync(temporary, { recursive: true, force: true }); }
