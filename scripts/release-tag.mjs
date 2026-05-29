#!/usr/bin/env node
import { currentVersion, releaseTagForHead } from "./release.mjs";

try {
	const manualVersion = process.argv[2]?.replace(/^v/, "");
	if (manualVersion) {
		const version = currentVersion(process.cwd());
		if (version !== manualVersion) {
			throw new Error(`Requested release version ${manualVersion}, but Cargo.toml says ${version}`);
		}
		console.log(`v${version}`);
		process.exit(0);
	}
	const tag = releaseTagForHead(process.cwd());
	if (!tag) {
		console.log("No release tag needed for this commit.");
		process.exit(0);
	}
	console.log(tag);
} catch (error) {
	console.error(error.message);
	process.exit(1);
}
