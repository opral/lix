import { expect, test } from "vitest";
import { openLix } from "./open-lix.js";

// Minutes of engine work if nothing stops it.
const LONG_READ = "SELECT count(*) AS n FROM generate_series(1, 4000000000)";

/** Rejects if `pending` has not settled within `ms`. */
async function within<T>(ms: number, pending: Promise<T>): Promise<T> {
	let timer: ReturnType<typeof setTimeout> | undefined;
	const deadline = new Promise<never>((_, reject) => {
		timer = setTimeout(() => reject(new Error(`not settled within ${ms} ms`)), ms);
	});
	try {
		return await Promise.race([pending, deadline]);
	} finally {
		clearTimeout(timer);
	}
}

const started = () => new Promise((resolve) => setTimeout(resolve, 200));

/** The worker is free again: a trivial read answers promptly. */
async function expectResponsive(lix: Awaited<ReturnType<typeof openLix>>) {
	expect(await within(3_000, lix.queryOne("SELECT 1 AS n"))).toEqual({ n: 1 });
}

test("aborting an in-flight query stops its engine work", async () => {
	const lix = await openLix();
	try {
		const controller = new AbortController();
		const reading = lix.query(LONG_READ, [], { signal: controller.signal });
		await started();
		controller.abort(new Error("stop reading"));
		await expect(within(3_000, reading)).rejects.toThrow("stop reading");
		await expectResponsive(lix);
	} finally {
		await lix.close();
	}
});

test("aborting an in-flight stream open stops its engine work", async () => {
	const lix = await openLix();
	try {
		const controller = new AbortController();
		const opening = lix.stream(LONG_READ, [], { signal: controller.signal });
		await started();
		controller.abort(new Error("stop streaming"));
		await expect(within(3_000, opening)).rejects.toThrow("stop streaming");
		await expectResponsive(lix);
	} finally {
		await lix.close();
	}
});

test("closing a Lix during a long read cancels the read", async () => {
	const lix = await openLix();
	const reading = lix.query(LONG_READ);
	reading.catch(() => undefined);
	await started();
	await within(5_000, lix.close());
	await expect(within(3_000, reading)).rejects.toMatchObject({ name: "LixError" });
});

test("aborting a transaction cancels its read in flight and rolls back", async () => {
	const lix = await openLix();
	try {
		const controller = new AbortController();
		const running = lix.transaction(
			async (tx) => {
				await tx.execute(
					"INSERT INTO lix_key_value (key, value) VALUES ('cancel-probe', '1')",
				);
				await tx.query(LONG_READ);
			},
			{ signal: controller.signal },
		);
		await started();
		controller.abort(new Error("stop transaction"));
		await expect(within(3_000, running)).rejects.toThrow("stop transaction");
		await expectResponsive(lix);
		expect(
			await lix.queryOne("SELECT key FROM lix_key_value WHERE key = 'cancel-probe'"),
		).toBeUndefined();
	} finally {
		await lix.close();
	}
});

test("an open stream keeps pulling while four long reads run", async () => {
	const lix = await openLix();
	try {
		const rows = await lix.stream<{ n: number }>(
			"SELECT value AS n FROM generate_series(1, 20000) ORDER BY value",
		);
		const pages = rows.pages();
		expect((await pages.next()).done).toBe(false);
		const controller = new AbortController();
		const longReads = Array.from({ length: 4 }, () =>
			lix.query(LONG_READ, [], { signal: controller.signal }),
		);
		for (const read of longReads) read.catch(() => undefined);
		await started();
		// Every independent slot runs a long read; the stream still pages.
		let pulled = 0;
		for (let i = 0; i < 3; i++) {
			const next = await within(3_000, pages.next());
			if (next.done) break;
			pulled += next.value.length;
		}
		expect(pulled).toBeGreaterThan(0);
		controller.abort(new Error("done"));
		await Promise.allSettled(longReads);
		await rows.close();
	} finally {
		await lix.close();
	}
});
