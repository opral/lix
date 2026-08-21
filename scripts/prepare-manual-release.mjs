#!/usr/bin/env node
import { prepareManualRelease } from "./release.mjs";

try {
	const requestedVersion = process.argv[2];
	if (!requestedVersion) {
		throw new Error("Usage: node scripts/prepare-manual-release.mjs <version>");
	}
	const result = prepareManualRelease(process.cwd(), requestedVersion);
	console.log(`version=${result.version}`);
	console.log(`changes=${result.changes.length}`);
} catch (error) {
	console.error(error.message);
	process.exit(1);
}
