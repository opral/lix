import { expect, test, vi } from "vitest";
import { Lix } from "./lix.js";
import type { LixBinding, ObserveEventsBinding } from "./binding-types.js";

function fixture(next = vi.fn(async (): Promise<any> => undefined)) {
	const close = vi.fn();
	const observe = vi.fn(async () => ({ next, close }));
	const lix = new Lix({
		observe,
		close: vi.fn(async () => {}),
		openReport: () => undefined,
	} as unknown as LixBinding);
	return { lix, next, close, observe };
}
const frame = {
	sequence: 1,
	mutationSequence: 2,
	rows: { columns: [], rows: [], rowsAffected: 0 },
};

test("for-await yields results and break closes the binding once", async () => {
	const f = fixture(vi.fn(async () => frame));
	const events = f.lix.observe("SELECT 1");
	expect("close" in events).toBe(false);
	for await (const event of events) {
		expect(event.result.rows).toEqual([]);
		break;
	}
	await Promise.resolve();
	expect(f.close).toHaveBeenCalledTimes(1);
	expect(await events.next()).toEqual({ done: true, value: undefined });
	await f.lix.close();
	expect(f.close).toHaveBeenCalledTimes(1);
});

test("EOF is terminal even if the consumer continues each loop body", async () => {
	const f = fixture();
	for await (const _ of f.lix.observe("SELECT 1")) {
		continue;
	}
	expect(f.next).toHaveBeenCalledTimes(1);
	expect(f.close).toHaveBeenCalledTimes(1);
	await f.lix.close();
});

test.each(["abort", "return", "lix-close"])(
	"%s settles a pending read even when the binding does not",
	async (action) => {
		const f = fixture(vi.fn(() => new Promise(() => {})));
		const controller = new AbortController();
		const events = f.lix.observe("SELECT 1", [], { signal: controller.signal });
		const pending = events.next();
		await vi.waitFor(() => expect(f.next).toHaveBeenCalledTimes(1));
		if (action === "abort") controller.abort();
		else if (action === "return") await events.return?.();
		else await f.lix.close();
		expect(await pending).toEqual({ done: true, value: undefined });
		expect(await events.next()).toEqual({ done: true, value: undefined });
		expect(f.close).toHaveBeenCalledTimes(1);
		await f.lix.close();
	},
);

test("abort before binding setup completes ends iteration and releases late binding", async () => {
	let resolve!: (binding: ObserveEventsBinding) => void;
	const close = vi.fn();
	const lix = new Lix({
		observe: () =>
			new Promise((r) => {
				resolve = r;
			}),
		openReport: () => undefined,
		close: async () => {},
	} as unknown as LixBinding);
	const controller = new AbortController();
	const events = lix.observe("SELECT 1", [], { signal: controller.signal });
	const pending = events.next();
	controller.abort();
	expect(await pending).toEqual({ done: true, value: undefined });
	resolve({ next: vi.fn(), close });
	await vi.waitFor(() => expect(close).toHaveBeenCalledTimes(1));
	await lix.close();
});

test("terminal errors reject once and clean up", async () => {
	const failure = new Error("observation failed");
	const f = fixture(
		vi.fn(async () => {
			throw failure;
		}),
	);
	const events = f.lix.observe("SELECT 1");
	await expect(events.next()).rejects.toBe(failure);
	expect(await events.next()).toEqual({ done: true, value: undefined });
	expect(f.close).toHaveBeenCalledTimes(1);
	await f.lix.close();
});
