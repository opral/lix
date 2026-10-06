import { expect, test } from "vitest";
import { OpfsStorage } from "./rpc-test-storage.js";
import type { LixStorageProvider, LixStorageSpace } from "@lix-js/sdk";

test("bounded OPFS reads preserve snapshot lengths, duplicates and ordered scan prefixes", async () => {
	const registration = new OpfsStorage({
		name: `bounded-opfs:${crypto.randomUUID()}`,
	}).lixStorage;
	const module = (await import(/* @vite-ignore */ registration.moduleUrl)) as {
		createLixStorageProvider(options: unknown): Promise<LixStorageProvider>;
	};
	const provider = await module.createLixStorageProvider(registration.options);
	const sessionToken = await provider.acquireSession();
	const space: LixStorageSpace = {
		id: 700,
		name: "bounded.provider",
		valueSemantics: "mutable",
		valueIntegrity: "backendVerified",
	};
	const mib = 1024 * 1024;
	try {
		const write = await provider.beginWrite({
			sessionToken,
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: 3 * mib,
		});
		await write.putMany(
			space,
			[0, 1, 2].map((index) => ({
				key: new Uint8Array([index]),
				value: new Uint8Array(mib).fill(index),
			})),
		);
		await write.commit();
		const read = await provider.beginRead({
			sessionToken,
			consistency: "snapshot",
			durability: "visible",
		});
		const keys = [2, 9, 0, 2].map((key) => new Uint8Array([key]));
		const requests = [
			{ space, keys, options: { projection: "fullValue" as const } },
		];
		const budget = { maxResultBytes: 3 * mib, maxSingleValueBytes: mib };
		const values = await read.getManyBounded(requests, budget);
		expect(values).toHaveLength(4);
		expect(values[1]).toBeNull();
		expect(values[0]).toEqual(values[3]);
		await expect(
			read.getManyBounded(requests, { ...budget, maxResultBytes: 3 * mib - 1 }),
		).rejects.toMatchObject({ code: "LIX_STORAGE_READ_BUDGET_EXCEEDED" });
		await expect(
			read.getManyBounded(requests, {
				...budget,
				maxSingleValueBytes: mib - 1,
			}),
		).rejects.toMatchObject({
			code: "LIX_STORAGE_SINGLE_VALUE_BUDGET_EXCEEDED",
		});
		const prefixes = [];
		let offset = 0;
		for (;;) {
			const page = await read.getManyBoundedPrefix(requests, offset, 32, {
				maxResultBytes: mib + mib / 2,
				maxSingleValueBytes: mib,
			});
			expect(
				page.values.filter((value) => value?.kind === "fullValue"),
			).toHaveLength(1);
			prefixes.push(...page.values);
			if (page.nextOffset === null) break;
			expect(page.nextOffset).toBe(offset + page.values.length);
			offset = page.nextOffset;
		}
		expect(prefixes).toEqual(values);
		const singleton = await read.getManyBoundedPrefix(requests, 0, 32, {
			maxResultBytes: mib / 2,
			maxSingleValueBytes: mib,
		});
		expect(singleton.values).toHaveLength(1);
		expect(singleton.nextOffset).toBe(1);
		await expect(
			read.getManyBoundedPrefix(requests, 0, 32, {
				maxResultBytes: mib,
				maxSingleValueBytes: mib - 1,
			}),
		).rejects.toMatchObject({
			code: "LIX_STORAGE_SINGLE_VALUE_BUDGET_EXCEEDED",
		});
		const keyOnly = await read.getManyBounded(
			[{ space, keys, options: { projection: "keyOnly" } }],
			{ maxResultBytes: 0, maxSingleValueBytes: 0 },
		);
		expect(keyOnly).toEqual([
			{ kind: "keyOnly" },
			null,
			{ kind: "keyOnly" },
			{ kind: "keyOnly" },
		]);
		const overwrite = await provider.beginWrite({
			sessionToken,
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: 2 * mib,
		});
		await overwrite.putMany(space, [
			{ key: new Uint8Array([0]), value: new Uint8Array(2 * mib) },
		]);
		await overwrite.commit();
		const old = await read.getManyBounded(
			[
				{
					space,
					keys: [new Uint8Array([0])],
					options: { projection: "fullValue" },
				},
			],
			{ maxResultBytes: mib, maxSingleValueBytes: mib },
		);
		expect(old[0]?.kind === "fullValue" ? old[0].value.length : 0).toBe(mib);
		const latest = await provider.beginRead({
			sessionToken,
			consistency: "latest",
			durability: "visible",
		});
		await expect(
			latest.getManyBounded(
				[
					{
						space,
						keys: [new Uint8Array([0])],
						options: { projection: "fullValue" },
					},
				],
				budget,
			),
		).rejects.toMatchObject({
			code: "LIX_STORAGE_SINGLE_VALUE_BUDGET_EXCEEDED",
		});
		for (const order of ["ascending", "descending"] as const) {
			const scan = await read.beginScan(
				space,
				{ lower: { kind: "unbounded" }, upper: { kind: "unbounded" } },
				{ order, projection: "fullValue" },
			);
			const seen: number[] = [];
			for (;;) {
				const page = await scan.nextPageBounded(10, {
					maxResultBytes: mib + mib / 2,
					maxSingleValueBytes: mib,
				});
				expect(page.entries.length).toBeLessThanOrEqual(1);
				if (page.hasMore) expect(page.entries.length).toBe(1);
				seen.push(...page.entries.map((entry) => entry.key[0]!));
				if (!page.hasMore) break;
			}
			expect(seen).toEqual(order === "ascending" ? [0, 1, 2] : [2, 1, 0]);
		}
	} finally {
		await provider.close();
	}
});
