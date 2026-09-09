import {
	openLix,
	type LixStorageSpace,
	type LixStoragePutEntry,
} from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { OpfsStorageClient } from "../js/client.js";

// Current physical layout fixture. Migration crash/old-format fixtures live in
// the Rust engine; this fixture exercises real OPFS, WASM, and public recovery.
const logicalSpaces = [
	0x10002, 0x20001, 0x40001, 0x40005, 0x40011, 0x40014, 0x40018, 0x4001a,
	0x4001b, 0x4001c, 0x4001d, 0x4001e, 0x4001f, 0x40020, 0x40021, 0x40022,
	0x40023, 0x40024, 0x40025, 0x40027, 0x40028, 0x40029, 0x4002a, 0x4002b,
	0x4002c, 0x4002d, 0x4002e, 0x4002f, 0x40032, 0x40033, 0x50001, 0x50002,
	0x50003, 0x50004, 0x50005, 0x60001, 0x60002, 0x70000, 0x70005, 0x70006,
	0x70007, 0x70010, 0x70011, 0x70014, 0x70015, 0x70016, 0x70017, 0x70018,
	0x80001, 0x80002, 0x80008, 0x80009,
];
const space = (id: number): LixStorageSpace => ({
	id,
	name: `fixture-${id}`,
	valueSemantics: "mutable",
	valueIntegrity: "backendVerified",
});
const epochSpace = space(0x90001);
const encode = (text: string) => new TextEncoder().encode(text);

test("OPFS retained work exports and restores through the real WASM worker", async () => {
	const name = `replica-recovery-${crypto.randomUUID()}`;
	const first = await openLix({ storage: new OpfsStorage({ name }) });
	let client: OpfsStorageClient | undefined;
	try {
		await first.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ('draft', 'device note')",
		);
		await first.execute(
			"INSERT INTO lix_key_value (key, value, lixcol_untracked) VALUES ('private', 'device only', true)",
		);
		await first.execute("SELECT commit_id FROM lix_create_checkpoint()");
		await first.execute(
			"UPDATE lix_key_value SET value = 'pending device note' WHERE key = 'draft'",
		);
		const repositoryId = (
			await first.execute(
				"SELECT value FROM lix_key_value WHERE key = 'lix_id'",
			)
		).rows[0]?.value;
		expect(typeof repositoryId).toBe("string");
		const accountId = await first.activeAccountId();
		client = await OpfsStorageClient.open(name);
		const sessionToken = await client.acquireSession();
		const read = await client.beginRead({
			durability: "durable",
			consistency: "snapshot",
			sessionToken,
		});
		const [pointerValue] = await read.getMany([
			{
				space: epochSpace,
				keys: [encode("active")],
				options: { projection: "fullValue" },
			},
		]);
		if (pointerValue?.kind !== "fullValue")
			throw new Error("fixture requires active epoch");
		const pointer = new TextDecoder().decode(pointerValue.value).split("|");
		expect(pointer[1]).toBe("active");
		const sourcePrefix =
			pointer[2] === "a"
				? 0x40000000
				: pointer[2] === "b"
					? 0x80000000
					: pointer[2] === "legacy"
						? 0
						: Number(pointer[2]?.slice(1)) * 0x100000;
		const copied: Array<{
			space: LixStorageSpace;
			entries: LixStoragePutEntry[];
		}> = [];
		for (const id of logicalSpaces) {
			const cursor = await read.beginScan(
				space(sourcePrefix + id),
				{ lower: { kind: "unbounded" }, upper: { kind: "unbounded" } },
				{ projection: "fullValue", order: "ascending" },
			);
			const entries: LixStoragePutEntry[] = [];
			for (;;) {
				const page = await cursor.nextPage(256);
				for (const entry of page.entries) {
					if (entry.value.kind !== "fullValue")
						throw new Error("fixture requires full values");
					entries.push({ key: entry.key, value: entry.value.value });
				}
				if (!page.hasMore) break;
			}
			if (entries.length) copied.push({ space: space(0x300000 + id), entries });
		}
		const write = await client.beginWrite({
			awaitDurable: true,
			sessionToken,
			preconditions: [],
			batchCapacityHintBytes: 0,
		});
		for (const copy of copied) await write.putMany(copy.space, copy.entries);
		await write.putMany(epochSpace, [
			{
				key: encode("retained/g3"),
				value: encode(
					JSON.stringify({
						bank: "g3",
						source_format: Number(pointer[4]),
						repository_id: repositoryId,
						account_id: accountId,
						recovery_required: true,
					}),
				),
			},
		]);
		await write.commit();
	} finally {
		await first.close();
		await client?.close();
	}

	const lix = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		await lix.execute(
			"UPDATE lix_key_value SET value = 'current server note' WHERE key = 'draft'",
		);
		const sources = await lix.replicaRecoverySources();
		expect(sources).toHaveLength(1);
		expect(sources[0]).toMatchObject({ id: "g3", recoveryRequired: true });
		const bundle = await lix.exportReplicaRecovery("g3");
		expect(bundle.source).toEqual(sources[0]);
		expect(bundle.branches.flatMap((branch) => branch.rows)).toContainEqual(
			expect.objectContaining({
				untracked: true,
				snapshot: expect.objectContaining({
					key: "private",
					value: "device only",
				}),
			}),
		);
		expect(Array.isArray(bundle.commits)).toBe(true);
		const receipt = await lix.recoverReplica("g3");
		expect(receipt.restoredRows).toBeGreaterThan(0);
		expect(receipt.branchIds.length).toBeGreaterThan(0);
		expect(
			(await lix.execute("SELECT value FROM lix_key_value WHERE key = 'draft'"))
				.rows[0]?.value,
		).toBe("current server note");
		const recovered = await lix.openAnotherSession({
			branchId: receipt.branchIds[0],
		});
		try {
			expect(
				(
					await recovered.execute(
						"SELECT value FROM lix_key_value WHERE key = 'draft'",
					)
				).rows[0]?.value,
			).toBe("pending device note");
		} finally {
			await recovered.close();
		}
		expect(await lix.replicaRecoverySources()).toEqual(sources);
	} finally {
		await lix.close();
	}
}, 60_000);
