import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

test("persists a complete local Lix", async () => {
	const storage = new OpfsStorage({
		name: `lix-opfs-test:${crypto.randomUUID()}`,
	});
	const first = await openLix({ storage });
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["durable-opfs", { value: 42 }],
	);
	await first.close();

	const second = await openLix({ storage });
	try {
		expect(
			(
				await second.execute("SELECT value FROM lix_key_value WHERE key = $1", [
					"durable-opfs",
				])
			).rows[0]?.get("value"),
		).toEqual({ value: 42 });
	} finally {
		await second.close();
	}
});

test("exclusively owns a name across handles", async () => {
	const name = `lix-opfs-owner-test:${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	await expect(openLix({ storage: new OpfsStorage({ name }) })).rejects.toThrow(
		"already open",
	);
	await first.close();
});

test("opens distinct repositories in parallel workers", async () => {
	const first = openLix({
		storage: new OpfsStorage({ name: `lix-opfs-parallel-a:${crypto.randomUUID()}` }),
	});
	const second = openLix({
		storage: new OpfsStorage({ name: `lix-opfs-parallel-b:${crypto.randomUUID()}` }),
	});
	const [left, right] = await Promise.all([first, second]);
	await Promise.all([left.close(), right.close()]);
});
