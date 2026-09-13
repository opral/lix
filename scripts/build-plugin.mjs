#!/usr/bin/env node
import { resolve } from "node:path";
import { buildPlugin } from "./plugin-archive.mjs";

const [key, flag, directory, ...extra] = process.argv.slice(2);
if (!key || flag !== "--out-dir" || !directory || extra.length) {
  throw new Error("Usage: node scripts/build-plugin.mjs <plugin_key> --out-dir <directory>");
}
console.log(JSON.stringify(await buildPlugin(key, resolve(directory))));
