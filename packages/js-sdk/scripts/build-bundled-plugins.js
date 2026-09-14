#!/usr/bin/env node
import { rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { buildPlugin } from "../../../scripts/plugin-archive.mjs";

// Keep the existing SDK bundle until independent release assets are available.
const outDir = fileURLToPath(new URL("../dist/bundled-plugins", import.meta.url));
for (const key of ["plugin_csv", "plugin_markdown"]) {
  await buildPlugin(key, outDir);
}
// Publication sidecars describe a single release, not the SDK's combined bundle.
await rm(join(outDir, "SHA256SUMS"));
await rm(join(outDir, "release-metadata.json"));
