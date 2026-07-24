import { expect, test } from "vitest";
import type { LixSnapshotStorage } from "./types.js";

class MemorySnapshotStorage implements LixSnapshotStorage {
	readonly snapshots = new Map<string, Uint8Array>();
	saveCalls = 0;

	async load(namespace: string): Promise<Uint8Array | undefined> {
		const snapshot = this.snapshots.get(namespace);
		return snapshot?.slice();
	}

	async save(namespace: string, snapshot: Uint8Array): Promise<void> {
		this.saveCalls += 1;
		this.snapshots.set(namespace, snapshot.slice());
	}
}

test("persistent worker binding saves after createCheckpoint", async () => {
	const { openPersistentLixWorkerBinding } =
		await import("../dist/worker/client.js");
	const storage = new MemorySnapshotStorage();
	const binding = await openPersistentLixWorkerBinding({
		storage,
		namespace: "checkpoint-persistence",
	});
	try {
		await binding.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('checkpoint-test', 'working')",
			[],
		);
		const before = storage.saveCalls;

		await binding.createCheckpoint();

		expect(storage.saveCalls).toBe(before + 1);
	} finally {
		await binding.close();
	}
});

test("persistent worker binding restores an exact Rust snapshot", async () => {
	const { openPersistentLixWorkerBinding } =
		await import("../dist/worker/client.js");
	const storage = new MemorySnapshotStorage();
	const namespace = "remote:https://lix.example/acme/workspace";
	const first = await openPersistentLixWorkerBinding({ storage, namespace });
	try {
		await first.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('atelier-ui', 'history')",
			[],
		);
	} finally {
		await first.close();
	}

	expect(storage.snapshots.get(namespace)?.byteLength).toBeGreaterThan(12);

	const reopened = await openPersistentLixWorkerBinding({ storage, namespace });
	try {
		const result = await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = 'atelier-ui'",
			[],
		);
		expect(result.rows).toHaveLength(1);
		expect(result.rows[0]?.[0]).toMatchObject({
			kind: "json",
			value: "history",
		});
	} finally {
		await reopened.close();
	}
});

test("persistent worker binding keeps namespaces independent", async () => {
	const { openPersistentLixWorkerBinding } =
		await import("../dist/worker/client.js");
	const storage = new MemorySnapshotStorage();
	const first = await openPersistentLixWorkerBinding({
		storage,
		namespace: "client-a",
	});
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('only-a', 'yes')",
		[],
	);
	await first.close();

	const second = await openPersistentLixWorkerBinding({
		storage,
		namespace: "client-b",
	});
	try {
		const result = await second.execute(
			"SELECT value FROM lix_key_value WHERE key = 'only-a'",
			[],
		);
		expect(result.rows).toHaveLength(0);
	} finally {
		await second.close();
	}
});

test("LocalStorage package adapter persists the worker snapshot", async () => {
	const [{ openPersistentLixWorkerBinding }, { LocalStorage }] =
		await Promise.all([
			import("../dist/worker/client.js"),
			import("@lix-js/sdk/local-storage-adapter"),
		]);
	const storage = new LocalStorage({
		prefix: `lix-browser-test-${crypto.randomUUID()}`,
	});
	const namespace = "local-storage-roundtrip";
	const first = await openPersistentLixWorkerBinding({ storage, namespace });
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ('persisted', 'yes')",
		[],
	);
	await first.close();

	const reopened = await openPersistentLixWorkerBinding({ storage, namespace });
	try {
		const result = await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = 'persisted'",
			[],
		);
		expect(result.rows[0]?.[0]).toMatchObject({ value: "yes" });
	} finally {
		await reopened.close();
	}
});
