import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { expect, test } from "vitest";
import { openNativeLixBinding } from "./binding.node.js";

test.each([false, true])(
	"native close releases filesystem ownership before resolving (syncAllFiles=%s)",
	async (syncAllFiles) => {
		const path = mkdtempSync(join(tmpdir(), "lix-native-close-"));
		const storage = { kind: "filesystem" as const, path, syncAllFiles };
		let binding = await openNativeLixBinding(storage);
		try {
			// Exercise the native promises directly: the public Lix wrapper
			// coalesces close calls and would hide races between native closes.
			for (let iteration = 0; iteration < 32; iteration++) {
				const previous = binding;
				await previous.execute(
					`INSERT INTO lix_key_value (key, value) VALUES ('close-${iteration}', 'persisted')`,
					[],
				);
				const closing = [previous.close(), previous.close(), previous.close()];
				// Every successful close is a release boundary, including the first
				// one to finish. Do not wait for all closes before reopening.
				await Promise.race(closing);
				binding = await openNativeLixBinding(storage);
				await Promise.all(closing);
				await previous.close();
				const persisted = await binding.execute(
					`SELECT value FROM lix_key_value WHERE key = 'close-${iteration}'`,
					[],
				);
				expect(persisted.rows).toHaveLength(1);
			}
		} finally {
			await binding.close();
			rmSync(path, { recursive: true, force: true });
		}
	},
	20_000,
);

test("a rejected native close preserves the actor for rollback and retry", async () => {
	const binding = await openNativeLixBinding({ kind: "memory" });
	try {
		const transaction = await binding.beginTransaction();
		try {
			await expect(binding.close()).rejects.toMatchObject({
				code: "LIX_INVALID_TRANSACTION_STATE",
			});
		} finally {
			await transaction.rollback();
		}
		expect((await binding.execute("SELECT 42 AS answer", [])).rows).toHaveLength(1);
	} finally {
		await binding.close();
	}
});
