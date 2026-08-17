/// <reference lib="webworker" />

import type {
	LixStorageProvider,
	LixStorageSpace,
} from "./storage-adapter.js";
import { OpfsStorage } from "@lix-js/storage-opfs";

type BenchmarkRequest = { name: string; rows: number };

const scope = globalThis as unknown as DedicatedWorkerGlobalScope;
const BENCH_SPACE: LixStorageSpace = {
	id: 7,
	name: "opfs.scale.bench",
	valueSemantics: "mutable",
	valueIntegrity: "backendVerified",
};

scope.onmessage = (event: MessageEvent<BenchmarkRequest>) => {
	void run(event.data).then(
		(result) => scope.postMessage({ ok: true, result }),
		(error) =>
			scope.postMessage({
				ok: false,
				error: error instanceof Error ? error.message : String(error),
			}),
	);
};

async function run(request: BenchmarkRequest) {
	const registration = new OpfsStorage({ name: request.name }).lixStorage;
	const { OpfsBackend } = (await import(
		/* @vite-ignore */ registration.moduleUrl
	)) as {
		OpfsBackend: {
			open(name: string): Promise<LixStorageProvider>;
		};
	};
	const backend = await OpfsBackend.open(request.name);
	const seedStarted = performance.now();
	const batchSize = 10_000;
	for (let offset = 0; offset < request.rows; offset += batchSize) {
		const puts = [];
		const end = Math.min(request.rows, offset + batchSize);
		for (let index = offset; index < end; index += 1) {
			puts.push({
				key: encodeKey(index),
				value: new Uint8Array(64).fill(index & 0xff),
			});
		}
		const write = await backend.beginWrite({
			awaitDurable: false,
			preconditions: [],
			batchCapacityHintBytes: puts.length * 72,
		});
		await write.putMany(BENCH_SPACE, puts);
		await write.commit();
	}
	const seedMs = performance.now() - seedStarted;

	const scanStarted = performance.now();
	const firstRead = await backend.beginRead({
		consistency: "snapshot",
		durability: "visible",
	});
	const firstScan = await firstRead.beginScan(
		BENCH_SPACE,
		{ lower: { kind: "unbounded" }, upper: { kind: "unbounded" } },
		{
		order: "ascending",
		projection: "keyOnly",
		},
	);
	const firstPage = await firstScan.nextPage(1_000);
	const firstPageMs = performance.now() - scanStarted;

	const deleteWrite = await backend.beginWrite({
		awaitDurable: false,
		preconditions: [],
		batchCapacityHintBytes: 0,
	});
	await deleteWrite.deleteRange(BENCH_SPACE, {
		lower: { kind: "included", key: encodeKey(0) },
		upper: {
					kind: "excluded",
					key: encodeKey(Math.floor(request.rows / 2)),
		},
	});
	await deleteWrite.commit();
	await backend.close();
	// Web Locks releases after the callback unwinds; yield before reopening the
	// same name so this benchmark measures SQLite reopen rather than lock churn.
	await new Promise<void>((resolve) => setTimeout(resolve, 25));

	const reopenSamples: number[] = [];
	let reopenedPageRows = 0;
	let remainingRows = 0;
	for (let sample = 0; sample < 8; sample += 1) {
		const reopenStarted = performance.now();
		const reopened = await OpfsBackend.open(request.name);
		const reopenedRead = await reopened.beginRead({
			consistency: "snapshot",
			durability: "visible",
		});
		const reopenedScan = await reopenedRead.beginScan(
			BENCH_SPACE,
			{ lower: { kind: "unbounded" }, upper: { kind: "unbounded" } },
			{
			order: "ascending",
			projection: "fullValue",
			},
		);
		const reopenedPage = await reopenedScan.nextPage(1_000);
		reopenSamples.push(performance.now() - reopenStarted);
		if (sample === 0) {
			reopenedPageRows = reopenedPage.entries.length;
			const countScan = await reopenedRead.beginScan(
				BENCH_SPACE,
				{ lower: { kind: "unbounded" }, upper: { kind: "unbounded" } },
				{ order: "ascending", projection: "keyOnly" },
			);
			for (;;) {
				const page = await countScan.nextPage(10_000);
				remainingRows += page.entries.length;
				if (page.entries.length === 0 || !page.hasMore) break;
			}
		}
		await reopened.close();
		await new Promise<void>((resolve) => setTimeout(resolve, 5));
	}

	return {
		rows: request.rows,
		seedMs,
		firstPageMs,
		reopenSamples,
		remainingRows,
		firstPageRows: firstPage.entries.length,
		reopenedPageRows,
	};
}

function encodeKey(index: number): Uint8Array {
	const key = new Uint8Array(8);
	new DataView(key.buffer).setBigUint64(0, BigInt(index), false);
	return key;
}
