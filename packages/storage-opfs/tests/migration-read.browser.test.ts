import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

test("current snapshot restore survives a real OPFS heartbeat during candidate verification", async () => {
	const name = `slow-snapshot-migration-${crypto.randomUUID()}`;
	const storage = new OpfsStorage({ name });
	const original = storage.lixStorage;
	const notificationChannel = `${name}-delay`;
	const delayedStorage = {
		lixStorage: {
			version: 3 as const,
			moduleUrl: new URL("./migration-delayed-provider.ts", import.meta.url).href,
			options: {
				moduleUrl: original.moduleUrl,
				providerOptions: original.options,
				notificationChannel,
			},
		},
	};
	// Ordinary opening accepts current-format snapshots. Historical formats
	// are qualified through the detached migration composition suite.
	const source = await openLix();
	let bytes: Uint8Array;
	try {
		for (let i = 0; i < 3; i++) await source.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)", [`heartbeat-${i}`, `preserved-${i}`],
		);
		bytes = new Uint8Array(await new Response(source.exportSnapshot()).arrayBuffer());
	} finally { await source.close(); }
	const expectedLix = await openLix.fromSnapshot(bytes);
	const query = "SELECT key, value FROM lix_key_value ORDER BY key";
	let expected;
	try {
		expected = (await expectedLix.execute(query)).rows;
		expect(expected.length).toBeGreaterThan(2);
	} finally {
		await expectedLix.close();
	}
	const notifications = new BroadcastChannel(notificationChannel);
	let delayed = false;
	notifications.onmessage = (event) => { delayed ||= event.data === "candidate-page-delayed"; };
	let imported;
	try {
		imported = await openLix.fromSnapshot(bytes, { storage: delayedStorage });
		expect(delayed).toBe(true);
		expect((await imported.execute(query)).rows).toEqual(expected);
	} finally {
		await imported?.close();
		notifications.close();
	}
	const reopened = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		expect((await reopened.execute(query)).rows).toEqual(expected);
	} finally {
		await reopened.close();
	}
}, 60_000);
