#!/usr/bin/env node
import { prepareRelease } from "./release.mjs";

try {
	const result = prepareRelease(process.cwd());
	if (!result) {
		console.log("No change fragments found; no release PR needed.");
		process.exit(0);
	}
	console.log(`version=${result.version}`);
	console.log(`type=${result.type}`);
} catch (error) {
	console.error(error.message);
	process.exit(1);
}
