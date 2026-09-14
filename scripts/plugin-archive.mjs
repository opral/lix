import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFile, realpath, mkdir, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";
import { currentVersion, PLUGIN_RELEASE_TARGETS, releaseTarget } from "./release.mjs";

const sdkRequire = createRequire(new URL("../packages/js-sdk/package.json", import.meta.url));
const { zipSync } = sdkRequire("fflate");
const { componentWit } = await import(pathToFileURL(sdkRequire.resolve("@bytecodealliance/jco-transpile/wasm-tools")));
export const repositoryRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

export function archivePath(path) {
  if (typeof path !== "string" || !path || isAbsolute(path) || path.includes("\\") || path.includes(":")) {
    throw new Error(`Invalid archive path: ${path}`);
  }
  if (path.split("/").some((part) => !part || part === "." || part === "..")) {
    throw new Error(`Invalid archive path: ${path}`);
  }
  return path;
}

async function sourceFile(directory, path) {
  const root = await realpath(directory);
  const file = await realpath(join(root, archivePath(path)));
  const rel = relative(root, file);
  if (rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel)) {
    throw new Error(`Archive source escapes plugin directory: ${path}`);
  }
  return readFile(file);
}

export async function componentApi(bytes) {
  const wit = await componentWit(bytes);
  const names = [...wit.matchAll(/^\s*(?:import|export)\s+(lix:plugin[^;\s]*);/gm)].map((match) => match[1]);
  const identities = new Set(names.map((name) => {
    const canonical = /^lix:plugin-v([1-9][0-9]*)\/[a-z][a-z0-9-]*$/.exec(name);
    if (canonical) return `lix:plugin-v${canonical[1]}`;
    if (/^lix:plugin\/[a-z][a-z0-9-]*@2\.0\.0$/.test(name)) return "lix:plugin@2.0.0";
    throw new Error(`Unrecognized plugin API interface: ${name}`);
  }));
  if (identities.size !== 1) throw new Error("Component must declare one plugin API identity");
  const apiIdentity = [...identities][0];
  return { apiIdentity, apiMajor: apiIdentity === "lix:plugin@2.0.0" ? 2 : Number(apiIdentity.split("-v")[1]) };
}

export async function packagePlugin(directory, key, wasm) {
  const manifestBytes = await sourceFile(directory, "manifest.json");
  const manifest = JSON.parse(manifestBytes);
  if (manifest.key !== key) throw new Error(`Manifest key must be ${key}`);
  if (!Array.isArray(manifest.schemas) || manifest.schemas.length === 0) throw new Error("Manifest schemas must be nonempty");
  const paths = ["manifest.json", archivePath(manifest.entry), ...manifest.schemas.map(archivePath)];
  if (new Set(paths).size !== paths.length) throw new Error("Duplicate archive paths");
  const api = await componentApi(wasm);
  const entries = Object.create(null);
  for (const path of paths.sort()) {
    const bytes = path === "manifest.json" ? manifestBytes : path === manifest.entry ? wasm : await sourceFile(directory, path);
    // DOS timestamps have local-time fields. Construct local midnight explicitly
    // so archives do not vary with the builder's timezone or wall clock.
    entries[path] = [bytes, { mtime: new Date(1980, 0, 1, 0, 0, 0) }];
  }
  const archive = zipSync(entries, { level: 0 });
  return { archive, ...api, sha256: createHash("sha256").update(archive).digest("hex") };
}

export async function buildPlugin(key, outDir, { root = repositoryRoot } = {}) {
  if (!PLUGIN_RELEASE_TARGETS.includes(key)) throw new Error(`Unknown plugin target: ${key}`);
  const directory = join(root, releaseTarget(key).path);
  const cargo = (...args) => execFileSync("cargo", args, { cwd: root, stdio: "inherit" });
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--format-version", "1", "--no-deps"], { cwd: root, encoding: "utf8" }));
  cargo("build", "--locked", "-p", key, "--target", "wasm32-wasip2", "--release");
  // Cargo's exact target output avoids selecting stale artifacts from a scan.
  const wasm = await readFile(join(metadata.target_directory, "wasm32-wasip2", "release", `${key}.wasm`));
  const { archive, ...details } = await packagePlugin(directory, key, wasm);
  const fileName = `${key}.lixplugin`;
  const result = { key, version: currentVersion(root, key), fileName, ...details };
  await mkdir(outDir, { recursive: true });
  await writeFile(join(outDir, fileName), archive);
  await writeFile(join(outDir, "SHA256SUMS"), `${result.sha256}  ${fileName}\n`);
  await writeFile(join(outDir, "release-metadata.json"), `${JSON.stringify(result, null, 2)}\n`);
  return result;
}
