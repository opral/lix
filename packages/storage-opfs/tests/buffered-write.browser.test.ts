import type {
	LixStorageCommitResult,
	LixStorageSpace,
	LixStorageWriteOptions,
} from "@lix-js/sdk";
import { describe, expect, test } from "vitest";
import {
	BufferedOpfsWrite,
	type OpfsWritePayload,
} from "../js/buffered-write.js";

const MUTABLE_SPACE: LixStorageSpace = {
	id: 1,
	name: "mutable",
	valueSemantics: "mutable",
	valueIntegrity: "backendVerified",
};
const IMMUTABLE_SPACE: LixStorageSpace = {
	...MUTABLE_SPACE,
	id: 2,
	name: "immutable",
	valueSemantics: "immutable",
};
const WRITE_OPTIONS: LixStorageWriteOptions = {
	awaitDurable: false,
	preconditions: [],
	batchCapacityHintBytes: 0,
};

for (const mode of ["direct", "shared"] as const) {
	describe(`${mode} buffered write`, () => {
		test("rejects conflicting immutable values", async () => {
			const { write } = createHarness(mode);
			await write.putMany(IMMUTABLE_SPACE, [entry(1, 1)]);

			await expect(
				write.putMany(IMMUTABLE_SPACE, [entry(1, 2)]),
			).rejects.toMatchObject({
				name: "LixStorageError",
				code: "LIX_STORAGE_CORRUPTION",
			});
		});

		test("stages explicit immutable replacements as ordinary puts", async () => {
			const { write, committed } = createHarness(mode);
			await write.replaceMany(IMMUTABLE_SPACE, [entry(1, 2)]);
			await write.commit();

			const payload = committed();
			expect(payload.immutablePuts).toEqual([]);
			expect(payload.puts.map((put) => [...put.value])).toEqual([[2]]);
			await expect(
				createHarness(mode).write.replaceMany(MUTABLE_SPACE, [entry(1, 2)]),
			).rejects.toMatchObject({ code: "LIX_STORAGE_CORRUPTION" });
		});

		test("stages range deletes and reports exact stats", async () => {
			const { write, committed } = createHarness(mode);
			await write.putMany(MUTABLE_SPACE, [
				entry(1, 10),
				entry(2, 20),
				entry(3, 30),
			]);
			await write.deleteRange(MUTABLE_SPACE, {
				lower: { kind: "included", key: bytes(1) },
				upper: { kind: "excluded", key: bytes(3) },
			});
			await write.deleteMany(MUTABLE_SPACE, [bytes(9)]);

			const result = await write.commit();
			const payload = committed();
			expect(payload.puts.map((put) => [...put.key])).toEqual([[3]]);
			expect(payload.deletes.map((deleted) => [...deleted.key])).toEqual([[9]]);
			expect(payload.deleteRanges).toHaveLength(1);
			expect(result.stats).toEqual({
				putEntries: 3,
				deletedEntries: 3,
				deletedRanges: 1,
				writtenBytes: 3,
				storageCalls: 3,
			});
			await expect(write.commit()).rejects.toMatchObject({
				code: "LIX_STORAGE_CLOSED",
			});
		});

		test("rollback closes without committing", async () => {
			const { write, committed } = createHarness(mode);
			await write.putMany(MUTABLE_SPACE, [entry(1, 1)]);
			await write.rollback();

			expect(() => committed()).toThrow("write was not committed");
			await expect(write.rollback()).rejects.toMatchObject({
				code: "LIX_STORAGE_CLOSED",
			});
			await expect(write.commit()).rejects.toMatchObject({
				code: "LIX_STORAGE_CLOSED",
			});
		});

		test("carries the acquired session token through commit", async () => {
			const { write, committed } = createHarness(
				mode,
				{
					...WRITE_OPTIONS,
					sessionToken: "18446744073709551615",
				},
				"owner-epoch",
			);

			await write.commit();

			expect(committed().sessionToken).toBe("18446744073709551615");
			expect(committed().ownerEpoch).toBe("owner-epoch");
		});
	});
}

function createHarness(
	mode: "direct" | "shared",
	options: LixStorageWriteOptions = WRITE_OPTIONS,
	ownerEpoch?: string,
) {
	let payload: OpfsWritePayload | undefined;
	const finish = (next: OpfsWritePayload): LixStorageCommitResult => {
		payload = mode === "shared" ? structuredClone(next) : next;
		return { stats: payload.stats };
	};
	const commit =
		mode === "shared"
			? async (next: OpfsWritePayload) => finish(next)
			: (next: OpfsWritePayload) => finish(next);
	return {
		write: new BufferedOpfsWrite(options, commit, ownerEpoch),
		committed: () => {
			if (!payload) throw new Error("write was not committed");
			return payload;
		},
	};
}

function entry(key: number, value: number) {
	return { key: bytes(key), value: bytes(value) };
}

function bytes(...values: number[]): Uint8Array {
	return new Uint8Array(values);
}
