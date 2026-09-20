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
      image.Config?.OnBuild?.length || !Array.isArray(image.RootFS?.Layers)) {
    throw new Error("Unexpected server image platform, revision, or ONBUILD instructions");
  }
}

// Only image metadata changes. The tested filesystem layers are retained, and
// consumers can continue requiring the landed SHA in both manifest and OCI label.
export function promoteServerImage({ directory, sourceRevision, revision, sourceRun, run = execFileSync }) {
  if (!/^[a-f0-9]{40}$/.test(revision) || !/^\d+$/.test(String(sourceRun))) {
    throw new Error("Invalid promotion revision or source run");
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
    writeFileSync(join(context, "Dockerfile"), `FROM ${manifest.image}\nLABEL org.opencontainers.image.revision=${revision}\n`);
    run("docker", ["build", "--network=none", "--pull=false", "--tag", image, context], { stdio: "inherit" });
    const promoted = inspect(image);
    const result = { ...manifest, sourceRevision: revision, image,
      reusedFromRevision: sourceRevision, reusedFromRun: String(sourceRun) };
    validateServerImage(result, promoted, revision);
    if (JSON.stringify(source.RootFS.Layers) !== JSON.stringify(promoted.RootFS.Layers)) {
      throw new Error("Promotion changed tested server filesystem layers");
    }
    for (const key of ["Cmd", "Entrypoint", "Env", "User", "WorkingDir", "ExposedPorts", "Volumes", "StopSignal", "Healthcheck", "Shell"]) {
      if (JSON.stringify(source.Config[key]) !== JSON.stringify(promoted.Config[key])) {
        throw new Error(`Promotion changed server runtime configuration: ${key}`);
      }
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
    sourceRun: process.env.SOURCE_RUN });
}
