#!/usr/bin/env node
import { copyFile, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const sourceDir = join(
	packageDir,
	"node_modules",
	"@bytecodealliance",
	"jco-transpile",
	"vendor",
);
const outDir = join(packageDir, "dist", "jco");
const files = [
	"js-component-bindgen-component.core.wasm",
	"js-component-bindgen-component.core2.wasm",
];

const nodeFetchHelper = `const isNode = typeof process !== 'undefined' && process.versions && process.versions.node;
let _fs;
async function fetchCompile (url) {
  if (isNode) {
    _fs = _fs || await import('node:fs/promises');
    return WebAssembly.compile(await _fs.readFile(url));
  }
  return fetch(url).then(WebAssembly.compileStreaming);
}`;
const browserFetchHelper = `async function fetchCompile (url) {
  return fetch(url).then(WebAssembly.compileStreaming);
}`;

await rm(outDir, { recursive: true, force: true });
await mkdir(outDir, { recursive: true });
const vendorJsPath = join(sourceDir, "js-component-bindgen-component.js");
const vendorJs = await readFile(vendorJsPath, "utf8");
if (!vendorJs.includes(nodeFetchHelper)) {
	throw new Error("JCO browser vendor layout changed; update build-jco-browser.js");
}
await writeFile(
	join(outDir, "js-component-bindgen-component.js"),
	vendorJs.replace(nodeFetchHelper, browserFetchHelper),
);
await Promise.all(
	files.map((file) => copyFile(join(sourceDir, file), join(outDir, file))),
);
