import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { pathToFileURL } from "node:url";

export function validateServerManifest(manifest, revision) {
  if (!/^[a-f0-9]{40}$/.test(revision) || manifest.schemaVersion !== 1 ||
      manifest.kind !== "lix-server-image" || manifest.target !== "linux-x64" ||
      manifest.sourceRevision !== revision || manifest.image !== `lix-server-ci:${revision}`) {
    throw new Error("Unexpected server artifact provenance");
  }
}

export function validateServerImage(manifest, image, revision) {
  validateServerManifest(manifest, revision);
  if (image.Os !== "linux" || image.Architecture !== "amd64" ||
      image.Config?.Labels?.["org.opencontainers.image.revision"] !== revision ||
      image.Config?.OnBuild?.length || !Array.isArray(image.RootFS?.Layers) ||
      !Array.isArray(image.Config?.Env) ||
      image.Config.Env.filter(value => value.startsWith("LIX_SOURCE_REVISION=")).length !== 1 ||
      !image.Config.Env.includes(`LIX_SOURCE_REVISION=${revision}`)) {
    throw new Error("Unexpected server image platform, revision, telemetry revision, or ONBUILD instructions");
  }
}

// Only image metadata changes. The tested filesystem layers are retained, and
// consumers require the landed SHA in the manifest, OCI label, and telemetry env.
export function promoteServerImage({ directory, sourceRevision, revision, sourceRun, version, run = execFileSync }) {
  if (!/^[a-f0-9]{40}$/.test(revision) || !/^\d+$/.test(String(sourceRun))) {
    throw new Error("Invalid promotion revision or source run");
  }
  if (version !== undefined && !/^[a-zA-Z0-9][a-zA-Z0-9._+-]*$/.test(version)) {
    throw new Error("Invalid server image version label");
  }
  const manifestPath = join(directory, "server-linux-x64.json");
  const archive = join(directory, "lix-server-image.tar");
  const manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
  // Validate the manifest before importing any Docker archive.
  validateServerManifest(manifest, sourceRevision);
  run("docker", ["load", "--input", archive], { stdio: "inherit" });
  const inspect = name => JSON.parse(run("docker", ["image", "inspect", name], { encoding: "utf8" }))[0];
  const source = inspect(manifest.image);
  validateServerImage(manifest, source, sourceRevision);
  const image = `lix-server-ci:${revision}`;
  const context = mkdtempSync(join(tmpdir(), "lix-server-promotion-"));
  try {
    writeFileSync(join(context, "Dockerfile"), `FROM ${manifest.image}\nLABEL org.opencontainers.image.revision=${revision}\nLABEL io.opral.lix.ci.source-revision=${sourceRevision}\nLABEL io.opral.lix.ci.source-run=${sourceRun}\n${version === undefined ? "" : `LABEL org.opencontainers.image.version=${version}\n`}ENV LIX_SOURCE_REVISION=${revision}\n`);
    run("docker", ["build", "--network=none", "--pull=false", "--tag", image, context], { stdio: "inherit" });
    const promoted = inspect(image);
    const result = { ...manifest, sourceRevision: revision, image,
      reusedFromRevision: sourceRevision, reusedFromRun: String(sourceRun) };
    validateServerImage(result, promoted, revision);
    if (promoted.Config.Labels["io.opral.lix.ci.source-revision"] !== sourceRevision ||
        promoted.Config.Labels["io.opral.lix.ci.source-run"] !== String(sourceRun)) {
      throw new Error("Promotion lost tested server source provenance labels");
    }
    if (version !== undefined && promoted.Config.Labels["org.opencontainers.image.version"] !== version) {
      throw new Error("Promotion changed server image version label");
    }
    if (JSON.stringify(source.RootFS.Layers) !== JSON.stringify(promoted.RootFS.Layers)) {
      throw new Error("Promotion changed tested server filesystem layers");
    }
    for (const key of ["Cmd", "Entrypoint", "User", "WorkingDir", "ExposedPorts", "Volumes", "StopSignal", "Healthcheck", "Shell"]) {
      if (JSON.stringify(source.Config[key]) !== JSON.stringify(promoted.Config[key])) {
        throw new Error(`Promotion changed server runtime configuration: ${key}`);
      }
    }
    const expectedEnv = source.Config.Env.map(value => value.startsWith("LIX_SOURCE_REVISION=")
      ? `LIX_SOURCE_REVISION=${revision}` : value);
    if (JSON.stringify(expectedEnv) !== JSON.stringify(promoted.Config.Env)) {
      throw new Error("Promotion changed server runtime configuration: Env");
    }
    run("docker", ["save", "--output", archive, image], { stdio: "inherit" });
    writeFileSync(manifestPath, `${JSON.stringify(result, null, 2)}\n`);
    return result;
  } finally {
    rmSync(context, { recursive: true, force: true });
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  promoteServerImage({ directory: resolve("ci-artifact"),
    sourceRevision: process.env.SOURCE_REVISION, revision: process.env.GITHUB_SHA,
    sourceRun: process.env.SOURCE_RUN, version: process.env.RELEASE_VERSION });
}
