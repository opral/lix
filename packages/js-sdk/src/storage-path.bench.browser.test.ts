import { expect, test } from "vitest";

type Samples = {
	read: number[];
	write: number[];
};

test("same-Wasm memory and JS SQLite OPFS execution paths", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const { OpfsStorage } = await import("@lix-js/storage-opfs");
	const results = [];
	for (const storage of [
		{ name: "same-wasm-memory", value: undefined },
		{
			name: "js-sqlite-opfs",
			value: new OpfsStorage({
				name: `lix-storage-path-bench:${crypto.randomUUID()}`,
			}),
		},
	]) {
		const lix = await openLix({ storage: storage.value });
		await lix.executeBatch(
			Array.from({ length: 512 }, (_, index) => ({
				sql: "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
				params: [`bench-${index}`, { index, revision: 0 }],
			})),
		);
		await lix.execute(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["bench-0"],
		);
		await lix.execute(
			"UPDATE lix_key_value SET value = $1 WHERE key = $2",
			[{ index: 0, revision: -1 }, "bench-0"],
		);

		const samples: Samples = { read: [], write: [] };
		for (let sample = 0; sample < 30; sample += 1) {
			let started = performance.now();
			await lix.execute(
				"SELECT value FROM lix_key_value WHERE key = $1",
				[`bench-${sample % 512}`],
			);
			samples.read.push(performance.now() - started);

			started = performance.now();
			await lix.execute(
				"UPDATE lix_key_value SET value = $1 WHERE key = $2",
				[{ index: sample % 512, revision: sample }, `bench-${sample % 512}`],
			);
			samples.write.push(performance.now() - started);
		}
		await lix.close();
		results.push({
			storage: storage.name,
			read: summarize(samples.read),
			write: summarize(samples.write),
		});
	}

	console.info(
		JSON.stringify({
			benchmark: "lix-storage-path-comparison",
			referenceHardware: {
				userAgent: navigator.userAgent,
				hardwareConcurrency: navigator.hardwareConcurrency,
			},
			fixtureRows: 512,
			results,
		}),
	);
	expect(results).toHaveLength(2);
});

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
