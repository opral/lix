import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";
import { expect, test } from "vitest";

const SAMPLE_COUNT = 30;

test(
	"reports warm Lix reopen through the first cached query",
	async () => {
		const name = `lix-opfs-warm-reopen:${crypto.randomUUID()}`;
		const storage = new OpfsStorage({ name });
		const seeded = await openLix({ storage });
		await seeded.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
			["warm-reopen", "ready"],
		);
		await seeded.close();

		const samples: number[] = [];
		for (let index = 0; index < SAMPLE_COUNT; index += 1) {
			const startedAt = performance.now();
			const lix = await openLix({ storage });
			const result = await lix.execute(
				"SELECT value FROM lix_key_value WHERE key = $1",
				["warm-reopen"],
			);
			samples.push(performance.now() - startedAt);
			expect(result.rows[0]?.value).toBe("ready");
			await lix.close();
		}

		const result = summarize(samples);
		console.info(
			JSON.stringify({
				benchmark: "lix-opfs-warm-reopen-first-query",
				targetP95Ms: 50,
				...result,
			}),
		);
	},
	120_000,
);

test(
	"reports local execute through observer delivery",
	async () => {
		const name = `lix-opfs-local-observer:${crypto.randomUUID()}`;
		const lix = await openLix({ storage: new OpfsStorage({ name }) });
		const observation = lix.observe(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["local-observer"],
		);
		try {
			await observation.next();
			const samples: number[] = [];
			for (let index = 0; index < SAMPLE_COUNT; index += 1) {
				const changed = observation.next();
				const startedAt = performance.now();
				await lix.execute(
					"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
					["local-observer", index],
				);
				const event = await withTimeout(changed, 2_000);
				expect(event?.result.rows[0]?.value).toBe(index);
				samples.push(performance.now() - startedAt);
			}

			const result = summarize(samples);
			console.info(
				JSON.stringify({
					benchmark: "lix-opfs-local-execute-observer-delivery",
					targetP95Ms: 50,
					...result,
				}),
			);
		} finally {
			observation.close();
			await lix.close();
		}
	},
	120_000,
);

function summarize(samples: number[]) {
	const sorted = [...samples].sort((left, right) => left - right);
	return {
		samples,
		p50Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.5) - 1)]!,
		p95Ms: sorted[Math.max(0, Math.ceil(sorted.length * 0.95) - 1)]!,
	};
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number): Promise<T> {
	return Promise.race([
		promise,
		new Promise<T>((_, reject) =>
			setTimeout(() => reject(new Error(`timed out after ${timeoutMs}ms`)), timeoutMs),
		),
	]);
}
