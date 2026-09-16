import { expect, test, vi } from "vitest";
import type { LixBinding } from "./binding-types.js";
import { Lix } from "./lix.js";

for (const limit of [-1, 0.5, NaN, Infinity, 4294967296, "0", null]) {
	test(`rejects invalid automatic retry cap ${String(limit)} before calling the binding`, async () => {
		const execute = vi.fn();
		const executeBatch = vi.fn();
		const lix = new Lix({ execute, executeBatch } as unknown as LixBinding);
		const options = { maxAutoCommitRetries: limit as number };
		await expect(lix.execute("SELECT 1", [], options)).rejects.toMatchObject({
			code: "LIX_INVALID_ARGUMENT",
		});
		await expect(lix.executeBatch([{ sql: "SELECT 1" }], options)).rejects.toMatchObject({
			code: "LIX_INVALID_ARGUMENT",
		});
		expect(execute).not.toHaveBeenCalled();
		expect(executeBatch).not.toHaveBeenCalled();
	});
}

test("forwards the zero retry cap for statements and batches", async () => {
	const stopped = new Error("binding reached");
	const execute = vi.fn(async () => { throw stopped; });
	const executeBatch = vi.fn(async () => { throw stopped; });
	const lix = new Lix({ execute, executeBatch } as unknown as LixBinding);
	await expect(lix.execute("SELECT 1", [], { maxAutoCommitRetries: 0 })).rejects.toBe(stopped);
	await expect(lix.executeBatch([{ sql: "SELECT 1" }], { maxAutoCommitRetries: 0 })).rejects.toBe(stopped);
	expect(execute).toHaveBeenCalledWith("SELECT 1", [], { maxAutoCommitRetries: 0 });
	expect(executeBatch).toHaveBeenCalledWith(expect.any(Array), { maxAutoCommitRetries: 0 });
});
