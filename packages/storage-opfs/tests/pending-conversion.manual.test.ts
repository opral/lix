import { openLix, type Lix } from "@lix-js/sdk";
import { convertReplicaToPartial } from "@lix-js/sdk/migration";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";
import { OpfsStorageClient } from "../js/client.js";

type Entry = { space: number; key: number[]; value: number[] };
type Fixture = { url: string; branchId: string; additionalBranchId: string; entries: Entry[] };
const space = (id: number) => ({ id, name: `fixture-${id}`, valueSemantics: "mutable" as const, valueIntegrity: "backendVerified" as const });

test("pending full OPFS replica resumes lost merge outcomes and preserves both branches", async () => {
	const response = await fetch("/__conversion_fixture.json");
	expect(response.ok).toBe(true);
	const fixture = await response.json() as Fixture;
	const name = `pending-full-conversion-${crypto.randomUUID()}`;
	// Starting the public storage owner precedes direct fixture installation.
	void new OpfsStorage({ name }).lixStorage;
	const client = await OpfsStorageClient.open(name);
	try {
		const sessionToken = await client.acquireSession();
		const write = await client.beginWrite({ awaitDurable: true, sessionToken, preconditions: [], batchCapacityHintBytes: 0 });
		for (const entry of fixture.entries) {
			await write.putMany(space(entry.space), [{ key: new Uint8Array(entry.key), value: new Uint8Array(entry.value) }]);
		}
		await write.commit();
	} finally { await client.close(); }
	const server = { url: fixture.url, fetch: proxyFetch };
	let completed = false;
	let failures = 0;
	for (let attempt = 0; attempt < 4; attempt++) {
		try {
			await convertReplicaToPartial({ storage: new OpfsStorage({ name }), server, branchId: fixture.branchId });
			completed = true;
			break;
		} catch {
			failures++;
			await verifySource(name, fixture.entries);
		}
	}
	// The authority drops the first committed merge response for each branch.
	expect(failures).toBeGreaterThan(0);
	expect(completed).toBe(true);
	for (let reopen = 0; reopen < 2; reopen++) {
		const lix = await openLix({ storage: new OpfsStorage({ name }), server: { ...server, mode: "partial_replica" } });
		try {
			await lix.switchBranch({ branchId: fixture.branchId });
			await verifyMerged(lix, fixture.additionalBranchId);
		} finally { await lix.close(); }
	}
}, 120_000);

async function verifySource(name: string, entries: Entry[]) {
	const client = await OpfsStorageClient.open(name);
	try {
		const sessionToken = await client.acquireSession();
		const read = await client.beginRead({ durability: "durable", consistency: "snapshot", sessionToken });
		for (const entry of entries) {
			if (entry.space === 0x90001) continue; // Mutable migration control/journal.
			const [actual] = await read.getMany([{ space: space(entry.space), keys: [new Uint8Array(entry.key)], options: { projection: "fullValue" } }]);
			expect(actual).toEqual({ kind: "fullValue", value: new Uint8Array(entry.value) });
		}
	} finally { await client.close(); }
}

async function verifyMerged(lix: Lix, branchId: string) {
	const query = "SELECT key, value FROM lix_key_value WHERE key IN ('local','remote') ORDER BY key";
	expect((await lix.execute(query)).rows).toEqual([{ key: "local", value: "L" }, { key: "remote", value: "R" }]);
	expect((await lix.execute("SELECT value FROM migration_custom_note WHERE id='local'")).rows).toEqual([{ value: "after" }]);
	const files = (await lix.execute("SELECT path, content FROM lix_file WHERE path IN ('/local.bin','/remote.bin') ORDER BY path")).rows;
	expect(files).toEqual([{ path: "/local.bin", content: new Uint8Array([8, 9, 10]) }, { path: "/remote.bin", content: new Uint8Array([5, 6, 7]) }]);
	await expect(lix.openAnotherSession({ branchId })).rejects.toMatchObject({
		code: "LIX_PARTIAL_REPLICA_SCOPE_NOT_PREPARED",
	});
	await lix.switchBranch({ branchId });
	expect((await lix.execute(query)).rows).toEqual([{ key: "local", value: "XL" }, { key: "remote", value: "XR" }]);
	expect(await lix.replicaRecoverySources()).toHaveLength(1);
}

async function proxyFetch(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
	const request = new Request(input, init);
	const original = new URL(request.url);
	const proxy = new URL(`/__conversion_authority${original.pathname}${original.search}`, location.href);
	const body = ["GET", "HEAD"].includes(request.method) ? undefined : await request.arrayBuffer();
	return fetch(proxy, { method: request.method, headers: request.headers, body, signal: request.signal });
}
