import { expect, test } from "vitest";

type BenchmarkCase = {
	name: string;
	calls: number;
	itemsPerCall: number;
	valueBytes: number;
};

type BenchmarkResult = BenchmarkCase & {
	samples: number[];
	checksum: number;
};

const cases: BenchmarkCase[] = [
	{ name: "one-item", calls: 1, itemsPerCall: 1, valueBytes: 128 },
	{ name: "100-unbatched", calls: 100, itemsPerCall: 1, valueBytes: 128 },
	{ name: "100-batched", calls: 1, itemsPerCall: 100, valueBytes: 128 },
	{ name: "1000-unbatched", calls: 1000, itemsPerCall: 1, valueBytes: 128 },
	{ name: "1000-batched", calls: 1, itemsPerCall: 1000, valueBytes: 128 },
	{ name: "1000-batched-1k", calls: 1, itemsPerCall: 1000, valueBytes: 1024 },
];

const benchmarkTest =
	(import.meta as ImportMeta & {
		env: { LIX_WASM_STORAGE_BENCH?: string };
	}).env.LIX_WASM_STORAGE_BENCH === "1"
		? test
		: test.skip;

benchmarkTest("Rust-Wasm to JS storage bridge scorecard", async () => {
	const results = await runWorker(cases);
	const userAgent = navigator.userAgent;
	console.info(
		JSON.stringify({
			benchmark: "lix-rust-js-storage-bridge",
			referenceHardware: {
				userAgent,
				hardwareConcurrency: navigator.hardwareConcurrency,
			},
			results: results.map((result) => ({
				name: result.name,
				calls: result.calls,
				itemsPerCall: result.itemsPerCall,
				valueBytes: result.valueBytes,
				...summarize(result.samples),
			})),
		}),
	);
	for (const result of results) {
		expect(result.samples).toHaveLength(30);
	}
});

function runWorker(benchmarkCases: BenchmarkCase[]): Promise<BenchmarkResult[]> {
	return new Promise((resolve, reject) => {
		const worker = new Worker(
			new URL("./storage-bridge.bench.worker.ts", import.meta.url),
			{ type: "module" },
		);
		worker.onmessage = (event: MessageEvent<
			| { ok: true; results: BenchmarkResult[] }
			| { ok: false; error: string }
		>) => {
			worker.terminate();
			if (event.data.ok) resolve(event.data.results);
			else reject(new Error(event.data.error));
		};
		worker.onerror = (event) => {
			worker.terminate();
			reject(event.error ?? new Error(event.message));
		};
		worker.postMessage({ cases: benchmarkCases });
	});
}

function summarize(samples: number[]) {
	const sorted = [...samples].sort((left, right) => left - right);
	return {
		samples,
		p50Ms: percentile(sorted, 0.5),
		p95Ms: percentile(sorted, 0.95),
		meanMs: samples.reduce((sum, sample) => sum + sample, 0) / samples.length,
	};
}

function percentile(sorted: number[], quantile: number): number {
	return sorted[Math.max(0, Math.ceil(sorted.length * quantile) - 1)]!;
}
