import { expect, test } from "vitest";
import { OpfsStorage } from "@lix-js/storage-opfs";

type ScaleResult = {
	rows: number;
	seedMs: number;
	firstPageMs: number;
	reopenSamples: number[];
	remainingRows: number;
	firstPageRows: number;
	reopenedPageRows: number;
};

test(
	"OPFS SQLite scale scorecard reports 10k and 1M rows with deletion",
	async () => {
		const results: ScaleResult[] = [];
		for (const rows of [10_000, 1_000_000]) {
			results.push(await runWorker(rows));
		}
			console.info(
			JSON.stringify({
				benchmark: "lix-opfs-shared-owner-scale",
				path: "Lix worker -> BroadcastChannel -> package owner -> SQLite",
					targets: {
						warmReopenP95Ms: 50,
						firstPageRows: 1_000,
					},
				samples: results.map((result) => ({
					...result,
					reopenMs: summarize(result.reopenSamples),
				})),
			}),
		);
		for (const result of results) {
			expect(result.firstPageRows).toBe(1_000);
			expect(result.reopenedPageRows).toBe(1_000);
			expect(summarize(result.reopenSamples).p95Ms).toBeLessThan(50);
			expect(result.remainingRows).toBe(Math.ceil(result.rows / 2));
		}
	},
	180_000,
);

function runWorker(rows: number): Promise<ScaleResult> {
	return new Promise((resolve, reject) => {
		const name = `lix-opfs-scale:${rows}:${crypto.randomUUID()}`;
		const registration = new OpfsStorage({ name }).lixStorage;
		const worker = new Worker(
			new URL("./opfs-scale.bench.worker.ts", import.meta.url),
			{ type: "module" },
		);
		worker.onmessage = (event: MessageEvent<
			| { ok: true; result: ScaleResult }
			| { ok: false; error: string }
		>) => {
			worker.terminate();
			if (event.data.ok) resolve(event.data.result);
			else reject(new Error(event.data.error));
		};
		worker.onerror = (event) => {
			worker.terminate();
			reject(event.error ?? new Error(event.message));
		};
		worker.postMessage({
			name,
			registration,
			rows,
		});
	});
}

function summarize(samples: number[]) {
	const sorted = [...samples].sort((left, right) => left - right);
	return {
		samples,
		p50Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.5) - 1)]!,
		p95Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)]!,
	};
}
