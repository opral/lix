import { expect, test } from "vitest";
import { openLix, type LixOpenProgress } from "../dist/index.js";
import { compatibility } from "../dist/compatibility.js";
import { OpfsStorage } from "../../storage-opfs/dist/index.js";

type Fixture = {
	format: number;
	repositoryId: string;
	branchId: string;
	records: string;
	storageSha256: string;
	indexRows: number;
	pendingKey: string;
	pendingValue: string;
	cachedKey: string;
	cachedValue: string;
};

async function fixtureRecords(name: string, records: ArrayBuffer, verifySource = false) {
	// Stage the frozen source records without opening them with the current
	// engine first. The backend worker releases its real OPFS owner before open.
	const worker = new Worker(
		new URL("../integration/released-partial-fixture.worker.ts", import.meta.url),
		{ type: "module" },
	);
	try {
		await new Promise<void>((resolve, reject) => {
			worker.onerror = (event) => reject(new Error(event.message));
			worker.onmessage = ({ data }) =>
				data.ok ? resolve() : reject(new Error(data.error));
			worker.postMessage({ name, records, verifySource }, [records]);
		});
	} finally {
		worker.terminate();
	}
}

test("existing partial OPFS data upgrades offline with resident indexes and pending edits", async () => {
	const fixtureUrl = new URL("../test-fixtures/partial-v86/manifest.json", import.meta.url);
	const response = await fetch(fixtureUrl);
	expect(response.ok).toBe(true);
	const fixture: Fixture = await response.json();
	const source = await fetch(new URL(fixture.records, fixtureUrl));
	expect(source.ok).toBe(true);
	const records = await source.arrayBuffer();
	const digest = await crypto.subtle.digest("SHA-256", records);
	expect(Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, "0")).join(""))
		.toBe(fixture.storageSha256);
	expect(fixture.indexRows).toBeGreaterThan(0);
	const name = `partial-preservation-${crypto.randomUUID()}`;
	const originalRecords = records.slice(0);
	await fixtureRecords(name, records);
	const progress: LixOpenProgress[] = [];
	// Durable partial admission supports an offline open with no authority
	// configuration. Neither migration nor the assertions can upload local work.
	const lix = await openLix({
		storage: new OpfsStorage({ name }),
		onProgress: (event) => progress.push(event),
	}).catch(async (error) => {
		// The negative control also verifies that a rejected candidate leaves
		// every original physical record intact, including the pending edits.
		await fixtureRecords(name, originalRecords, true);
		throw error;
	});
	try {
		expect(lix.openReport?.initialized).toBe(false);
		expect(lix.openReport?.migrations).toContainEqual({
			scope: "local",
			fromFormat: fixture.format,
			toFormat: compatibility.storageFormatVersion,
		});
		expect(progress.some((event) =>
			event.scope === "local" && event.phase === "validating" &&
			event.fromFormat === fixture.format,
		)).toBe(true);
		expect((await lix.execute(
			"SELECT value FROM lix_key_value WHERE key = 'lix_id'",
		)).rows).toEqual([{ value: fixture.repositoryId }]);
		expect(await lix.activeBranchId()).toBe(fixture.branchId);
		for (const [key, value] of [
			[fixture.cachedKey, fixture.cachedValue],
			[fixture.pendingKey, fixture.pendingValue],
		]) {
			expect((await lix.execute(
				"SELECT value FROM lix_key_value WHERE key = $1", [key],
			)).rows).toEqual([{ value }]);
		}
		expect((await lix.execute("SELECT id, slug FROM browser_indexed_note ORDER BY id")).rows)
			.toEqual([{ id: "cached-note", slug: "cached-slug" }, { id: "pending-note", slug: "pending-slug" }]);
		expect((await lix.execute("SELECT id FROM browser_indexed_note WHERE slug = 'pending-slug'")).rows)
			.toEqual([{ id: "pending-note" }]);
		expect((await lix.execute("SELECT content FROM lix_file WHERE path = '/released-cache.bin'")).rows)
			.toEqual([{ content: new Uint8Array([1, 2, 3]) }]);
		await lix.execute(
			"UPDATE lix_key_value SET value = $1 WHERE key = $2",
			["still-local-after-upgrade", fixture.pendingKey],
		);
		const other = await lix.openAnotherSession();
		try {
			expect((await other.execute(
				"SELECT value FROM lix_key_value WHERE key = $1", [fixture.pendingKey],
			)).rows).toEqual([{ value: "still-local-after-upgrade" }]);
		} finally {
			await other.close();
		}
	} finally {
		await lix.close();
	}
	const reopened = await openLix({ storage: new OpfsStorage({ name }) });
	try {
		expect(reopened.openReport?.migrations).toEqual([]);
		expect((await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = 'lix_id'",
		)).rows).toEqual([{ value: fixture.repositoryId }]);
		expect(await reopened.activeBranchId()).toBe(fixture.branchId);
		expect((await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = $1", [fixture.pendingKey],
		)).rows).toEqual([{ value: "still-local-after-upgrade" }]);
		expect((await reopened.execute("SELECT id FROM browser_indexed_note WHERE slug = 'pending-slug'")).rows)
			.toEqual([{ id: "pending-note" }]);
	} finally {
		await reopened.close();
	}
}, 120_000);
