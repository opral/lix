import { expect, test } from "vitest";

// Minutes of engine work if nothing stops it.
const LONG_READ = "SELECT count(*) AS n FROM generate_series(1, 4000000000)";

async function within<T>(ms: number, pending: Promise<T>): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	try {
		return await Promise.race([
			pending,
			new Promise<never>((_, reject) => {
				timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
			}),
		]);
	} finally {
		clearTimeout(timer);
	}
}

const started = () => new Promise((resolve) => setTimeout(resolve, 200));

// The browser worker only sees a cancel once the engine yields to its event
// loop; this proves the yield reaches the browser's message queue.
test("aborting a read in the browser worker stops its engine work", async () => {
	const { openLix } = await import("@lix-js/sdk");
	const lix = await openLix();
	try {
		const controller = new AbortController();
		const reading = lix.query(LONG_READ, [], { signal: controller.signal });
		await started();
		controller.abort(new Error("stop reading"));
		await expect(within(3_000, reading)).rejects.toThrow("stop reading");
		expect(await within(3_000, lix.queryOne("SELECT 1 AS n"))).toEqual({ n: 1 });

		const snapshot = await lix.snapshot();
		const pinned = snapshot.query(LONG_READ);
		// Settles while close() is awaited; observed below.
		pinned.catch(() => undefined);
		await started();
		await within(3_000, snapshot.close());
		await expect(within(3_000, pinned)).rejects.toMatchObject({
			code: "LIX_ERROR_SNAPSHOT_CLOSED",
		});
		expect(await within(3_000, lix.queryOne("SELECT 2 AS n"))).toEqual({ n: 2 });
	} finally {
		await lix.close();
	}
});
