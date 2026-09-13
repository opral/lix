import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { publishPlugin, selectPluginReleases } from "./publish-plugin.mjs";

function fixture(t, { existing = false, draft = false, wrongTag = false } = {}) {
  const root = mkdtempSync(join(tmpdir(), "plugin-release-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  mkdirSync(join(root, "plugins/json"), { recursive: true });
  writeFileSync(join(root, "plugins/json/CHANGELOG.md"), "# JSON\n\n## 0.16.2\n\nFix parsing.\n\n## 0.16.1\nOld.\n");
  const bytes = Buffer.from("compiled archive");
  const digest = createHash("sha256").update(bytes).digest("hex");
  writeFileSync(join(root, "plugin_json.lixplugin"), bytes);
  writeFileSync(join(root, "SHA256SUMS"), `${digest}  plugin_json.lixplugin\n`);
  writeFileSync(join(root, "release-metadata.json"), JSON.stringify({ key: "plugin_json", version: "0.16.2", fileName: "plugin_json.lixplugin", apiMajor: 2, apiIdentity: "lix:plugin-v2", sha256: digest }));
  const sha = "a".repeat(40);
  const state = { tag: existing ? (wrongTag ? "b".repeat(40) : sha) : null, release: existing ? { id: 1, draft } : null, assets: [], calls: [] };
  const missing = () => { throw Object.assign(new Error("Not found"), { status: 404 }); };
  const repos = {
    getCommit: async () => state.tag ? { data: { sha: state.tag } } : missing(),
    getReleaseByTag: async () => state.release ? { data: state.release } : missing(),
    createRelease: async args => { state.calls.push(["create", args]); state.release = { id: 1, draft: args.draft }; return { data: state.release }; },
    listReleaseAssets: async () => ({ data: state.assets }),
    getReleaseAsset: async args => ({ data: state.assets.find(asset => asset.id === args.asset_id).bytes }),
    uploadReleaseAsset: async args => { state.calls.push(["upload", args.name]); state.assets.push({ id: state.assets.length + 1, name: args.name, bytes: args.data }); },
    updateRelease: async args => { state.calls.push(["publish", args]); state.release.draft = args.draft; },
  };
  const github = { rest: { repos, git: { getRef: async () => state.tag ? { data: { object: { sha: state.tag } } } : missing(), createRef: async args => { state.calls.push(["tag", args]); state.tag = args.sha; } } }, paginate: async fn => (await fn()).data };
  return { state, bytes, root, run: () => publishPlugin({ github, owner: "opral", repo: "lix", target: "plugin_json", version: "0.16.2", sha, directory: root, root }), github };
}

test("publishes only after uploaded assets verify and never makes plugin latest", async t => {
  const { run, state } = fixture(t);
  assert.equal(await run(), "plugin_json/v0.16.2");
  assert.deepEqual(state.calls.map(call => call[0]), ["tag", "create", "upload", "upload", "publish"]);
  const create = state.calls.find(call => call[0] === "create")[1];
  assert.equal(create.draft, true);
  assert.equal(create.make_latest, "false");
  assert.match(create.body, /Plugin API: v2/);
  assert.doesNotMatch(create.body, /Old\./);
  assert.equal(state.calls.at(-1)[1].make_latest, "false");
  state.calls.length = 0;
  await run();
  assert.deepEqual(state.calls, []);
});

test("resumes partial draft without replacing an existing archive", async t => {
  const { run, state, bytes } = fixture(t, { existing: true, draft: true });
  state.assets.push({ id: 1, name: "plugin_json.lixplugin", bytes });
  await run();
  assert.deepEqual(state.calls.map(call => call.slice(0, 2)), [["upload", "SHA256SUMS"], ["publish", state.calls.at(-1)[1]]]);
});

test("rejects tag mismatch before uploading", async t => {
  const { run, state } = fixture(t, { existing: true, wrongTag: true });
  await assert.rejects(run, /different source commit/);
  assert.deepEqual(state.calls, []);
});

test("published incomplete release is not modified", async t => {
  const { run, state } = fixture(t, { existing: true });
  await assert.rejects(run, /Published release is missing/);
  assert.deepEqual(state.calls, []);
});

test("changed existing asset is never clobbered", async t => {
  const { run, state } = fixture(t, { existing: true, draft: true });
  state.assets.push({ id: 1, name: "plugin_json.lixplugin", bytes: Buffer.from("wrong") });
  await assert.rejects(run, /refusing to overwrite/);
  assert.deepEqual(state.calls, []);
});

test("failed downloaded checksum leaves release a draft", async t => {
  const { run, state, github } = fixture(t);
  github.rest.repos.getReleaseAsset = async () => ({ data: Buffer.from("corrupt") });
  await assert.rejects(run, /failed checksum verification/);
  assert.equal(state.release.draft, true);
  assert.ok(!state.calls.some(call => call[0] === "publish"));
});

test("server failures are not treated as missing releases", async t => {
  const { run, state, github } = fixture(t);
  github.rest.git.getRef = async () => { throw Object.assign(new Error("Unavailable"), { status: 503 }); };
  await assert.rejects(run, /Unavailable/);
  assert.deepEqual(state.calls, []);
});

test("version selection ignores inheritance migration and isolates plugin bumps", t => {
  const root = mkdtempSync(join(tmpdir(), "plugin-select-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const git = (...args) => execFileSync("git", args, { cwd: root, encoding: "utf8" }).trim();
  git("init", "--quiet"); git("config", "user.name", "Test"); git("config", "user.email", "test@example.com");
  writeFileSync(join(root, "Cargo.toml"), '[workspace.package]\nversion = "0.16.1"\n');
  for (const key of ["json", "csv", "markdown", "text", "excalidraw"]) {
    mkdirSync(join(root, "plugins", key), { recursive: true });
    writeFileSync(join(root, "plugins", key, "Cargo.toml"), '[package]\nversion.workspace = true\n');
  }
  git("add", "."); git("commit", "-qm", "before"); const before = git("rev-parse", "HEAD");
  for (const key of ["json", "csv", "markdown", "text", "excalidraw"]) writeFileSync(join(root, "plugins", key, "Cargo.toml"), '[package]\nversion = "0.16.1"\n');
  git("add", "."); git("commit", "-qm", "decouple"); const decoupled = git("rev-parse", "HEAD"); git("update-ref", "refs/remotes/origin/main", decoupled);
  assert.deepEqual(selectPluginReleases(root, { before, sha: decoupled }), []);
  writeFileSync(join(root, "plugins/json/Cargo.toml"), '[package]\nversion = "0.16.2"\n');
  git("add", "."); git("commit", "-qm", "json release"); const sha = git("rev-parse", "HEAD"); git("update-ref", "refs/remotes/origin/main", sha);
  assert.deepEqual(selectPluginReleases(root, { before: decoupled, sha }), [{ target: "plugin_json", version: "0.16.2", sha, tag: "plugin_json/v0.16.2" }]);
  assert.throws(() => selectPluginReleases(root, { sha: "main", target: "plugin_json" }), /full commit SHA/);
  assert.equal(selectPluginReleases(root, { sha, target: "plugin_json" }).length, 1);
  assert.throws(() => selectPluginReleases(root, { sha: decoupled, target: "plugin_json" }), /commit that increased/);
  git("commit", "--allow-empty", "-qm", "unrelated later change");
  const later = git("rev-parse", "HEAD"); git("update-ref", "refs/remotes/origin/main", later);
  assert.throws(() => selectPluginReleases(root, { sha: later, target: "plugin_json" }), /commit that increased/);
});


test("missing changelog fails before creating a tag", async t => {
  const { run, state, root } = fixture(t);
  writeFileSync(join(root, "plugins/json/CHANGELOG.md"), "# Changelog\n");
  await assert.rejects(run, /Missing changelog/);
  assert.deepEqual(state.calls, []);
});

test("unexpected assets added during upload prevent publication", async t => {
  const { run, state, github } = fixture(t);
  const upload = github.rest.repos.uploadReleaseAsset;
  github.rest.repos.uploadReleaseAsset = async args => {
    await upload(args);
    if (args.name === "SHA256SUMS") state.assets.push({ id: 3, name: "extra", bytes: Buffer.from("extra") });
  };
  await assert.rejects(run, /unexpected or duplicate/);
  assert.equal(state.release.draft, true);
});
