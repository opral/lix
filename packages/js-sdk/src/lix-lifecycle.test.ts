import { expect, test, vi } from "vitest";
import type { LixBinding } from "./binding-types.js";
import { ManagedLixClientState } from "./client-state.js";
import { Lix } from "./lix.js";

test("managed Lix close rejects new work and drains an in-flight branch switch", async () => {
	const remoteSwitch = deferred<{ branchId: string }>();
	const order: string[] = [];
	const execute = vi.fn(async () => {
		throw new Error("execute must not reach the binding after close starts");
	});
	const binding = {
		execute,
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
	const clientBinding = {
		clientStateSet: vi.fn(async () => undefined),
		close: async () => {
			order.push("client binding closed");
		},
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState({
		binding: clientBinding,
		closeBinding: true,
	});
	const lix = new Lix(binding, clientState);
	const branchListener = vi.fn();
	lix.subscribeActiveBranch(branchListener);

	const switching = lix.switchBranch({ branchId: "draft" });
	const closing = lix.close();
	const readAfterClose = lix.execute("SELECT 1");
	const stateReadAfterClose = lix.clientState.get("late");
	const stateWriteAfterClose = lix.clientState.set("late", true);

	await expect(readAfterClose).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(execute).not.toHaveBeenCalled();
	await expect(stateReadAfterClose).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	await expect(stateWriteAfterClose).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(order).toEqual(["remote switch started"]);

	remoteSwitch.resolve({ branchId: "draft" });
	await expect(switching).resolves.toEqual({ branchId: "draft" });
	expect(clientBinding.clientStateSet).not.toHaveBeenCalled();
	await expect(closing).resolves.toBeUndefined();
	expect(branchListener).toHaveBeenCalledOnce();
	expect(order).toEqual([
		"remote switch started",
		"remote switch finished",
		"remote binding closed",
		"client binding closed",
	]);
});

test("an active-transaction close preflight preserves client state and observations", async () => {
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
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const storedClientState = new Map<string, unknown>();
	const clientBinding = {
		clientStateGet: vi.fn(async (key: string) => storedClientState.get(key)),
		clientStateSet: vi.fn(async (key: string, value: unknown) => {
			storedClientState.set(key, value);
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState({
		binding: clientBinding,
		closeBinding: true,
	});
	const lix = new Lix(binding, clientState);
	const observation = lix.observe("SELECT 1");
	await observation.next();
	const transaction = await lix.beginTransaction();

	await expect(lix.close()).rejects.toMatchObject({
		code: "LIX_INVALID_TRANSACTION_STATE",
	});
	expect(binding.close).not.toHaveBeenCalled();
	expect(clientBinding.close).not.toHaveBeenCalled();
	expect(observationClose).not.toHaveBeenCalled();
	await expect(lix.clientState.set("still-open", true)).resolves.toBeUndefined();
	await expect(lix.clientState.get("still-open")).resolves.toBe(true);

	await transaction.rollback();
	await expect(lix.close()).resolves.toBeUndefined();
	expect(binding.close).toHaveBeenCalledOnce();
	expect(clientBinding.close).toHaveBeenCalledOnce();
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

test("client state get reads the binding and preserves operation order", async () => {
	const pendingSet = deferred<void>();
	let stored: unknown = "before";
	const clientBinding = {
		clientStateGet: vi.fn(async () => stored),
		clientStateSet: vi.fn(async (_key: string, value: unknown) => {
			await pendingSet.promise;
			stored = value;
		}),
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState({ binding: clientBinding });

	await expect(clientState.get("preference")).resolves.toBe("before");
	stored = "changed-outside-facade";
	await expect(clientState.get("preference")).resolves.toBe(
		"changed-outside-facade",
	);

	const setting = clientState.set("preference", "after");
	const reading = clientState.get("preference");
	expect(clientBinding.clientStateGet).toHaveBeenCalledTimes(2);
	pendingSet.resolve();
	await expect(setting).resolves.toBeUndefined();
	await expect(reading).resolves.toBe("after");
	expect(clientBinding.clientStateGet).toHaveBeenCalledTimes(3);
});

test("a remote close failure still closes managed local client state", async () => {
	const binding = {
		close: vi.fn(async () => {
			throw new Error("remote close failed");
		}),
	} as unknown as LixBinding;
	const clientBinding = {
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState({
		binding: clientBinding,
		closeBinding: true,
	});
	const lix = new Lix(binding, clientState);

	await expect(lix.close()).rejects.toThrow("remote close failed");
	expect(binding.close).toHaveBeenCalledOnce();
	expect(clientBinding.close).toHaveBeenCalledOnce();
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
