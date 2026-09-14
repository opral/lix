#!/usr/bin/env node
import { prepareRelease, RELEASE_TARGETS } from "./release.mjs";

try {
	const args = process.argv.slice(2);
	if (args.length === 1 && args[0] === "--list-targets") {
		console.log(JSON.stringify(Object.keys(RELEASE_TARGETS)));
	} else {
		if (args.length !== 0 && (args.length !== 2 || args[0] !== "--target")) {
			throw new Error("Usage: node scripts/prepare-release.mjs [--target <target> | --list-targets]");
		}
		const result = prepareRelease(process.cwd(), { target: args[1] ?? "lix" });
		if (!result) {
			console.log("No change fragments found for this target; no release PR needed.");
		} else {
			for (const key of ["target", "version", "type", "tag", "branch"]) console.log(`${key}=${result[key]}`);
		}
	}
} catch (error) {
	console.error(error.message);
	process.exit(1);
}
