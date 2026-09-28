import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promoteServerImage, validateServerImage, validateServerManifest } from "./ci-promote-server-image.mjs";

const sourceRevision = "1".repeat(40);
const revision = "2".repeat(40);
const manifest = { schemaVersion: 1, kind: "lix-server-image", target: "linux-x64",
  sourceRevision, image: `lix-server-ci:${sourceRevision}` };
const source = { Os: "linux", Architecture: "amd64", Config: {
  Env: ["TEST_VALUE=preserved", `LIX_SOURCE_REVISION=${sourceRevision}`],
  Labels: { "org.opencontainers.image.revision": sourceRevision } }, RootFS: { Layers: ["sha256:tested"] } };
const promotedLabels = { "org.opencontainers.image.revision": revision,
  "io.opral.lix.ci.source-revision": sourceRevision, "io.opral.lix.ci.source-run": "42" };

test("rejects mismatched manifest or OCI provenance before promotion", () => {
  validateServerImage(manifest, source, sourceRevision);
  for (const patch of [{ schemaVersion: 2 }, { kind: "other" }, { target: "linux-arm64" },
    { sourceRevision: revision }, { image: "another-image" }]) {
    assert.throws(() => validateServerManifest({ ...manifest, ...patch }, sourceRevision));
  }
  for (const patch of [{ Architecture: "arm64" }, { Os: "windows" },
    { Config: { Labels: { "org.opencontainers.image.revision": revision } } },
    { Config: { ...source.Config, Env: ["TEST_VALUE=preserved"] } },
    { Config: { ...source.Config, Env: [`LIX_SOURCE_REVISION=${revision}`] } },
    { Config: { ...source.Config, Env: [...source.Config.Env, `LIX_SOURCE_REVISION=${sourceRevision}`] } },
    { Config: { ...source.Config, OnBuild: ["RUN unexpected-command"] } }]) {
    assert.throws(() => validateServerImage(manifest, { ...source, ...patch }, sourceRevision));
  }
});

test("promotion retains tested layers and rewrites both receipt and image identity", () => {
  const directory = mkdtempSync(join(tmpdir(), "server-promotion-test-"));
  try {
    writeFileSync(join(directory, "server-linux-x64.json"), JSON.stringify(manifest));
    const calls = [];
    const run = (command, args) => {
      assert.equal(command, "docker"); calls.push(args);
      if (args[0] === "build") {
        assert.equal(readFileSync(join(args.at(-1), "Dockerfile"), "utf8"),
          `FROM ${manifest.image}\nLABEL org.opencontainers.image.revision=${revision}\nLABEL io.opral.lix.ci.source-revision=${sourceRevision}\nLABEL io.opral.lix.ci.source-run=42\nENV LIX_SOURCE_REVISION=${revision}\n`);
      }
      if (args[0] === "image") return JSON.stringify([{ ...source, Config: {
        ...source.Config,
        Env: ["TEST_VALUE=preserved", `LIX_SOURCE_REVISION=${args.at(-1).endsWith(revision) ? revision : sourceRevision}`],
        Labels: args.at(-1).endsWith(revision) ? promotedLabels : source.Config.Labels } }]);
    };
    const result = promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42", run });
    assert.equal(result.sourceRevision, revision);
    assert.equal(result.image, `lix-server-ci:${revision}`);
    assert.equal(result.reusedFromRevision, sourceRevision);
    assert.equal(result.reusedFromRun, "42");
    assert.ok(calls.some(args => args[0] === "save" && args.at(-1) === result.image));
    assert.deepEqual(JSON.parse(readFileSync(join(directory, "server-linux-x64.json"), "utf8")), result);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("release promotion sets and checks the version label", () => {
  const directory = mkdtempSync(join(tmpdir(), "server-promotion-test-"));
  try {
    writeFileSync(join(directory, "server-linux-x64.json"), JSON.stringify(manifest));
    const run = (_command, args) => {
      if (args[0] === "build") {
        assert.match(readFileSync(join(args.at(-1), "Dockerfile"), "utf8"),
          /LABEL org\.opencontainers\.image\.version=1\.2\.3\n/);
      }
      if (args[0] === "image") return JSON.stringify([args.at(-1).endsWith(revision)
        ? { ...source, Config: { ...source.Config,
          Env: ["TEST_VALUE=preserved", `LIX_SOURCE_REVISION=${revision}`],
          Labels: { ...promotedLabels, "org.opencontainers.image.version": "1.2.3" } } } : source]);
    };
    promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42", version: "1.2.3", run });
    const missingVersion = (_command, args) => {
      if (args[0] === "image") return JSON.stringify([args.at(-1).endsWith(revision)
        ? { ...source, Config: { ...source.Config,
          Env: ["TEST_VALUE=preserved", `LIX_SOURCE_REVISION=${revision}`],
          Labels: promotedLabels } } : source]);
    };
    writeFileSync(join(directory, "server-linux-x64.json"), JSON.stringify(manifest));
    assert.throws(() => promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42",
      version: "1.2.3", run: missingVersion }), /version label/);
    assert.throws(() => promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42",
      version: "1.2.3\\nRUN evil", run }), /Invalid server image version label/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("changed filesystem layers fail without publishing a replacement receipt", () => {
  const directory = mkdtempSync(join(tmpdir(), "server-promotion-test-"));
  try {
    writeFileSync(join(directory, "server-linux-x64.json"), JSON.stringify(manifest));
    const run = (_command, args) => {
      assert.notEqual(args[0], "save");
      if (args[0] === "image") return JSON.stringify([args.at(-1).endsWith(revision)
        ? { ...source, Config: { ...source.Config, Env: ["TEST_VALUE=preserved", `LIX_SOURCE_REVISION=${revision}`], Labels: promotedLabels }, RootFS: { Layers: ["different"] } } : source]);
    };
    assert.throws(() => promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42", run }), /filesystem layers/);
    assert.deepEqual(JSON.parse(readFileSync(join(directory, "server-linux-x64.json"), "utf8")), manifest);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test("real Docker archive loads with landed revision and unchanged layers", { skip: process.env.LIX_TEST_DOCKER_PROMOTION !== "1" }, () => {
  const directory = mkdtempSync(join(tmpdir(), "server-promotion-docker-"));
  const docker = args => execFileSync("docker", args, { encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] });
  try {
    writeFileSync(join(directory, "marker"), "tested bytes\n");
    writeFileSync(join(directory, "Dockerfile"), `FROM scratch\nCOPY marker /marker\nENV TEST_VALUE=preserved LIX_SOURCE_REVISION=${sourceRevision}\nCMD ["do-not-run"]\nLABEL org.opencontainers.image.revision=${sourceRevision}\n`);
    docker(["build", "--tag", manifest.image, directory]);
    docker(["save", "--output", join(directory, "lix-server-image.tar"), manifest.image]);
    writeFileSync(join(directory, "server-linux-x64.json"), JSON.stringify(manifest));
    const result = promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42", version: "1.2.3", run: (_cmd, args) => docker(args) });
    docker(["image", "rm", result.image]);
    docker(["load", "--input", join(directory, "lix-server-image.tar")]);
    const loaded = JSON.parse(docker(["image", "inspect", result.image]))[0];
    validateServerImage(result, loaded, revision);
    assert.equal(loaded.Config.Labels["org.opencontainers.image.version"], "1.2.3");
    assert.equal(loaded.Config.Labels["io.opral.lix.ci.source-revision"], sourceRevision);
    assert.equal(loaded.Config.Labels["io.opral.lix.ci.source-run"], "42");
    const original = JSON.parse(docker(["image", "inspect", manifest.image]))[0];
    assert.deepEqual(loaded.RootFS.Layers, original.RootFS.Layers);
    assert.deepEqual(loaded.Config.Cmd, ["do-not-run"]);
    assert.deepEqual(loaded.Config.Env, original.Config.Env.map(value => value.startsWith("LIX_SOURCE_REVISION=") ? `LIX_SOURCE_REVISION=${revision}` : value));
  } finally {
    docker(["image", "rm", "--force", manifest.image, `lix-server-ci:${revision}`]);
    rmSync(directory, { recursive: true, force: true });
  }
});

test("promotion rejects unrelated environment changes before saving", () => {
  const directory = mkdtempSync(join(tmpdir(), "server-promotion-test-"));
  try {
    writeFileSync(join(directory, "server-linux-x64.json"), JSON.stringify(manifest));
    const run = (_command, args) => {
      assert.notEqual(args[0], "save");
      if (args[0] === "image") return JSON.stringify([args.at(-1).endsWith(revision)
        ? { ...source, Config: { ...source.Config,
          Env: ["TEST_VALUE=changed", `LIX_SOURCE_REVISION=${revision}`],
          Labels: promotedLabels } } : source]);
    };
    assert.throws(() => promoteServerImage({ directory, sourceRevision, revision, sourceRun: "42", run }), /runtime configuration: Env/);
    assert.deepEqual(JSON.parse(readFileSync(join(directory, "server-linux-x64.json"), "utf8")), manifest);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});
