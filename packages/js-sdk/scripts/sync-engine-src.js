#!/usr/bin/env node
import { cp, mkdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const repoRoot = join(__dirname, "..", "..", "..");
const jsSdkDir = join(repoRoot, "packages", "js-sdk");
const engineSrcDir = join(repoRoot, "packages", "engine", "src");
const bundledDir = join(jsSdkDir, "dist-engine-src");
const bundledSrcDir = join(bundledDir, "src");

async function main() {
	await rm(bundledDir, { recursive: true, force: true });
	await mkdir(bundledDir, { recursive: true });
	await cp(engineSrcDir, bundledSrcDir, {
		recursive: true,
		force: true,
	});
	await writeFile(
		join(bundledDir, "README.md"),
		[
			"# Bundled Lix Engine Source",
			"",
			"This directory is a generated snapshot of the Rust engine source that backs this @lix-js/sdk release.",
			"",
			"Source in the Lix monorepo: `packages/engine/src`",
			"",
			"Agents should inspect these files when SDK behavior is unclear instead of relying only on SKILL.md prose.",
			"",
			"Useful entry points:",
			"",
			"- `src/sql2/providers/entity.rs` - registered schema SQL surfaces",
			"- `src/sql2/providers/change.rs` - `lix_change` projection",
			"- `src/sql2/providers/branch.rs` - writable `lix_branch` surface",
			"- `src/transaction/validation.rs` - primary-key, unique, foreign-key, and shape validation",
			"- `src/schema/definition.json` - Lix schema-definition meta-schema",
			"- `src/schema/builtin/` - built-in schema definitions",
			"",
			"Regenerate with `pnpm --filter @lix-js/sdk sync:engine-src` from the repo root.",
			"",
		].join("\n"),
	);
}

main().catch((error) => {
	console.error("[sync-engine-src] Failed to sync engine source:\n", error);
	process.exit(1);
});
