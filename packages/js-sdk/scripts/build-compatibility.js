import { mkdir, writeFile } from "node:fs/promises";
import { getCompatibility } from "../../../scripts/compatibility.mjs";

const output = new URL("../dist/", import.meta.url);
await mkdir(output, { recursive: true });
await writeFile(new URL("compatibility.js", output),
  `// Generated from the canonical Rust engine constants. No binding is loaded.\nexport const compatibility = Object.freeze(${JSON.stringify(getCompatibility(), null, 2)});\n`);
await writeFile(new URL("compatibility.d.ts", output),
  `/** Compatibility versions of the engine shipped with this SDK. */\nexport declare const compatibility: Readonly<{\n  serverProtocolVersion: number;\n  syncProtocolVersion: number;\n  storageFormatVersion: number;\n}>;\n`);
