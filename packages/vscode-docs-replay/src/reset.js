import { rm } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { dirname } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const DEFAULT_OUTPUT_PATH = join(__dirname, "..", "results", "vscode-docs-first-100.lix");
const outputPath = process.env.VSCODE_REPLAY_OUTPUT_PATH ?? DEFAULT_OUTPUT_PATH;

async function main() {
  await rm(outputPath, { force: true });
  console.log(`[reset] deleted ${outputPath}`);
}

main().catch((error) => {
  console.error("reset failed");
  console.error(error);
  process.exitCode = 1;
});
