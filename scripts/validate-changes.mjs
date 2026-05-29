#!/usr/bin/env node
import { loadChanges } from "./release.mjs";

try {
	const root = process.cwd();
	const changes = loadChanges(root);
	console.log(`Validated ${changes.length} change fragment(s).`);
} catch (error) {
	console.error(error.message);
	process.exit(1);
}
