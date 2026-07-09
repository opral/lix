#!/usr/bin/env node
import { rm } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const packageDir = join(dirname(fileURLToPath(import.meta.url)), "..");
await rm(join(packageDir, "dist"), { recursive: true, force: true });
