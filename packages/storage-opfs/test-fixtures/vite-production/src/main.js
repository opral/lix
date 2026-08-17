import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

globalThis.__storageOpfsProductionSmoke = run();

async function run() {
	const storage = new OpfsStorage({ name: "packed-vite-production" });
	const first = await openLix({ storage });
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
		["packed-vite", "persistent-production"],
	);
	await first.close();

	const reopened = await openLix({ storage });
	try {
		const persisted = await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["packed-vite"],
		);
		return { message: persisted.rows[0]?.get("value") };
	} finally {
		await reopened.close();
	}
}
