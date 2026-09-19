import { expect, test, vi } from "vitest";
import { LixTransaction } from "./lix.js";
test("a lost transaction releases its parent lease after execute rejects", async () => {
	const finish = vi.fn();
	const binding = {
		execute: vi
			.fn()
			.mockRejectedValue(
				Object.assign(new Error("lost"), { code: "LIX_TRANSACTION_LOST" }),
			),
		commit: vi.fn(),
		rollback: vi.fn(),
	};
	const transaction = new LixTransaction(binding, finish);
	await expect(transaction.execute("SELECT 1")).rejects.toMatchObject({
		code: "LIX_TRANSACTION_LOST",
	});
	expect(finish).toHaveBeenCalledOnce();
	await expect(transaction.rollback()).rejects.toThrow(/closed/);
	expect(binding.rollback).not.toHaveBeenCalled();
});
