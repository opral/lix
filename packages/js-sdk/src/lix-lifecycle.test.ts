import { expect, test, vi } from "vitest";
import type { LixBinding } from "./binding-types.js";
import { ManagedLixClientState } from "./client-state.js";
import { Lix } from "./lix.js";

test("managed Lix close rejects new work and drains an in-flight branch switch", async () => {
	const remoteSwitch = deferred<{ branchId: string }>();
	const localPersistence = deferred<void>();
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
	const clientStateSet = vi.fn(async () => {
			order.push("client persistence started");
			await localPersistence.promise;
			order.push("client persistence finished");
		});
	const clientBinding = {
		clientStateSet,
		close: async () => {
			order.push("client binding closed");
		},
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState(
		{ binding: clientBinding, closeBinding: true },
		new Map(),
	);
	const lix = new Lix(binding, clientState);
	const branchListener = vi.fn();
	lix.subscribeActiveBranch(branchListener);

	const switching = lix.switchBranch({ branchId: "draft" });
	const closing = lix.close();
	const readAfterClose = lix.execute("SELECT 1");
	const stateWriteAfterClose = lix.clientState.set("late", true);

	await expect(readAfterClose).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(execute).not.toHaveBeenCalled();
	await expect(stateWriteAfterClose).rejects.toMatchObject({
		code: "LIX_ERROR_CLOSED",
	});
	expect(clientStateSet).not.toHaveBeenCalled();
	expect(order).toEqual(["remote switch started"]);

	remoteSwitch.resolve({ branchId: "draft" });
	await vi.waitFor(() => {
		expect(order).toContain("client persistence started");
	});
	expect(order).not.toContain("client binding closed");
	expect(order).not.toContain("remote binding closed");

	localPersistence.resolve();
	await expect(switching).resolves.toEqual({ branchId: "draft" });
	await expect(closing).resolves.toBeUndefined();
	expect(branchListener).toHaveBeenCalledOnce();
	expect(order).toEqual([
		"remote switch started",
		"remote switch finished",
		"client persistence started",
		"client persistence finished",
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
	const clientBinding = {
		clientStateSet: vi.fn(async () => undefined),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState(
		{ binding: clientBinding, closeBinding: true },
		new Map(),
	);
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
	expect(lix.clientState.get("still-open")).toBe(true);

	await transaction.rollback();
	await expect(lix.close()).resolves.toBeUndefined();
	expect(binding.close).toHaveBeenCalledOnce();
	expect(clientBinding.close).toHaveBeenCalledOnce();
});

test("automatic branch preference persistence is best effort but direct writes reject", async () => {
	const storage = {
		save: vi.fn(async () => {
			throw new Error("storage.save failed");
		}),
	};
	const clientBinding = {
		clientStateSet: vi.fn(async () => undefined),
		exportSnapshot: vi.fn(async () => new Uint8Array([1, 2, 3])),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const clientState = new ManagedLixClientState(
		{ binding: clientBinding, saveSnapshot: storage.save },
		new Map(),
	);
	const binding = {
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => ({
			branchId,
		})),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const lix = new Lix(binding, clientState);
	const branchListener = vi.fn();
	lix.subscribeActiveBranch(branchListener);

	await expect(lix.clientState.set("direct", true)).rejects.toThrow(
		"storage.save failed",
	);
	expect(lix.clientState.get("direct")).toBe(true);
	await expect(lix.switchBranch({ branchId: "draft" })).resolves.toEqual({
		branchId: "draft",
	});
	expect(branchListener).toHaveBeenCalledOnce();
	expect(binding.switchBranch).toHaveBeenCalledWith({ branchId: "draft" });
	expect(storage.save).toHaveBeenCalledTimes(2);

	await lix.close();
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
