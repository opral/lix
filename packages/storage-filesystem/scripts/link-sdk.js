#!/usr/bin/env node
// The SDK is a peer dependency. npm with --legacy-peer-deps does not link it,
// even when a local package is supplied explicitly. Link the built workspace
// SDK for development and CI without changing the published dependency graph.
import { mkdir, rm, symlink } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const scope = new URL("../node_modules/@lix-js/", import.meta.url);
const destination = new URL("sdk", scope);
await mkdir(scope, { recursive: true });
await rm(destination, { recursive: true, force: true });
await symlink(
  fileURLToPath(new URL("../../js-sdk/", import.meta.url)),
  fileURLToPath(destination),
  process.platform === "win32" ? "junction" : "dir",
);
