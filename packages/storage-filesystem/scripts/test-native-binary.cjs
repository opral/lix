// Exercise cross-compiled N-API binaries on their actual target architecture.
const assert = require("node:assert/strict");
const { mkdtempSync, rmSync } = require("node:fs");
const { tmpdir } = require("node:os");
const { join } = require("node:path");
const { Lix } = require("../lix_storage_filesystem.node");

async function main() {
	const path = mkdtempSync(join(tmpdir(), "lix-filesystem-native-"));
	const dispatch = async () => {
		throw new Error("No plugin is used in this smoke test");
	};
	const open = () =>
		Lix.openFilesystemStorage(
			path,
			false,
			undefined,
			undefined,
			undefined,
			undefined,
			undefined,
			dispatch,
		);
	let binding;
	try {
		binding = await open();
		await binding.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('native-smoke', 'persisted')",
			[],
		);
		await binding.close();
		binding = await open();
		assert.equal(
			(
				await binding.execute(
					"SELECT value FROM lix_key_value WHERE key = 'native-smoke'",
					[],
				)
			).rows.length,
			1,
		);
		await binding.close();
		binding = undefined;
		console.log("Native filesystem persistence and reopen passed");
	} finally {
		if (binding) await binding.close();
		rmSync(path, { recursive: true, force: true });
	}
}
main().catch((error) => {
	console.error(error);
	process.exitCode = 1;
});
