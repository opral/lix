import { expect, test, vi } from "vitest";
import type { LixBinding } from "./binding-types.js";
import { Lix } from "./lix.js";

test("managed Lix close rejects new work and drains an in-flight branch switch", async () => {
	const remoteSwitch = deferred<{ branchId: string }>();
	const order: string[] = [];
	const execute = vi.fn(async () => {
		throw new Error("execute must not reach the binding after close starts");
	});
	const binding = {
		execute,
		beginClose: () => {
			order.push("remote shutdown signaled");
		},
		switchBranch: async () => {
			order.push("remote switch started");
			const receipt = await remoteSwitch.promise;
			order.push("remote switch finished");
			return receipt;
		},
		close: async () => {
			order.push("remote binding closed");
		},
	} as unknown as LixBinding;
	const lix = new Lix(binding);
	const branchListener = vi.fn();
	lix.subscribeActiveBranch(branchListener);

	const switching = lix.switchBranch({ branchId: "draft" });
	const closing = lix.close();
	const readAfterClose = lix.execute("SELECT 1");
	await expect(readAfterClose).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(execute).not.toHaveBeenCalled();
	expect(order).toEqual(["remote switch started", "remote shutdown signaled"]);

	remoteSwitch.resolve({ branchId: "draft" });
	await expect(switching).resolves.toEqual({ branchId: "draft" });
	await expect(closing).resolves.toBeUndefined();
	expect(branchListener).toHaveBeenCalledOnce();
	expect(order).toEqual([
		"remote switch started",
		"remote shutdown signaled",
		"remote switch finished",
		"remote binding closed",
	]);
});

test("an active-transaction close preflight preserves the Lix and observations", async () => {
	const observationClose = vi.fn();
	const binding = {
		observe: vi.fn(async () => ({
			next: async () => undefined,
			close: observationClose,
		})),
		beginTransaction: vi.fn(async () => ({
			execute: vi.fn(),
			commit: vi.fn(async () => undefined),
			rollback: vi.fn(async () => undefined),
		})),
		activeBranchId: vi.fn(async () => "main"),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const lix = new Lix(binding);
	const observation = lix.observe("SELECT 1");
	await observation.next();
	const transaction = await lix.beginTransaction();

	await expect(lix.close()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	expect(binding.close).not.toHaveBeenCalled();
	expect(observationClose).not.toHaveBeenCalled();
	await expect(lix.activeBranchId()).resolves.toBe("main");

	await transaction.rollback();
	await expect(lix.close()).resolves.toBeUndefined();
	expect(binding.close).toHaveBeenCalledOnce();
});

test("a terminal commit failure releases the transaction from its parent Lix", async () => {
	const transactionBinding = {
		execute: vi.fn(),
		commit: vi.fn(async () => {
			throw new Error("durable commit failed");
		}),
		rollback: vi.fn(async () => undefined),
	};
	const binding = {
		beginTransaction: vi.fn(async () => transactionBinding),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const lix = new Lix(binding);
	const transaction = await lix.beginTransaction();

	await expect(transaction.commit()).rejects.toThrow("durable commit failed");
	await expect(transaction.rollback()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	await expect(lix.close()).resolves.toBeUndefined();
	expect(binding.close).toHaveBeenCalledOnce();
	expect(transactionBinding.rollback).not.toHaveBeenCalled();
});

test("a remote close failure is reported", async () => {
	const binding = {
		close: vi.fn(async () => {
			throw new Error("remote close failed");
		}),
	} as unknown as LixBinding;
	const lix = new Lix(binding);

	await expect(lix.close()).rejects.toThrow("remote close failed");
	expect(binding.close).toHaveBeenCalledOnce();
});

function deferred<T>() {
	let resolve!: (value: T | PromiseLike<T>) => void;
	let reject!: (reason?: unknown) => void;
	const promise = new Promise<T>((resolvePromise, rejectPromise) => {
		resolve = resolvePromise;
		reject = rejectPromise;
	});
	return { promise, resolve, reject };
}
