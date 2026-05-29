#!/usr/bin/env node
import { releaseTagForHead } from "./release.mjs";

try {
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
