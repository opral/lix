import { expect, test, vi } from "vitest";
import type {
	BindingExecuteResult,
	LixBinding,
	ObserveEventsBinding,
} from "./binding-types.js";
import { authoritativeHotBinding } from "./authoritative-hot-binding.js";

const result = (value: string): BindingExecuteResult => ({
	columns: [{ name: "value", type: "text" }],
	rows: [[{ kind: "text", value }]],
	rowsAffected: 0,
	notices: [],
});

const fence = (commitId = "active-1"): BindingExecuteResult => ({
	columns: [{ name: "active_commit_id", type: "text" }],
	rows: [[{ kind: "text", value: commitId }]],
	rowsAffected: 0,
	notices: [],
});

const authorityRequired = (executionKind: "history" | "mutation") =>
	Object.assign(new Error("authority required"), {
		code: "LIX_AUTHORITY_EXECUTION_REQUIRED",
		details: { executionKind },
	});

test("a hot read is one local execution with no authority probe", async () => {
	const localExecute = vi.fn(async () => result("local-hot"));
	const authorityExecute = vi.fn();
	const binding = authoritativeHotBinding(
		{ execute: localExecute } as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(binding.execute("SELECT * FROM lix_file", [])).resolves.toEqual(
		result("local-hot"),
	);
	expect(localExecute).toHaveBeenCalledOnce();
	expect(localExecute).toHaveBeenCalledWith(
		"SELECT * FROM lix_file",
		[],
		undefined,
	);
	expect(authorityExecute).not.toHaveBeenCalled();
});

test("history is rejected locally and then executed once on the authority", async () => {
	const localExecute = vi.fn(async () => {
		throw authorityRequired("history");
	});
	const authorityExecute = vi.fn(async () => result("authority-history"));
	const binding = authoritativeHotBinding(
		{ execute: localExecute } as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(
		binding.execute("SELECT * FROM lix_history('lix_file')", []),
	).resolves.toEqual(result("authority-history"));
	expect(localExecute).toHaveBeenCalledOnce();
	expect(authorityExecute).toHaveBeenCalledOnce();
});

test("an authority mutation is followed by one local publication fence", async () => {
	const calls: string[] = [];
	const localExecute = vi.fn(async (sql: string) => {
		calls.push(`local:${sql}`);
		if (sql.startsWith("UPDATE")) throw authorityRequired("mutation");
		return fence("active-2");
	});
	const authorityExecute = vi.fn(async (sql: string) => {
		calls.push(`authority:${sql}`);
		return result("updated");
	});
	const binding = authoritativeHotBinding(
		{ execute: localExecute } as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	const sql = "UPDATE lix_file SET path = '/b' WHERE path = '/a'";
	await expect(binding.execute(sql, [])).resolves.toEqual(result("updated"));
	expect(calls).toEqual([
		`local:${sql}`,
		`authority:${sql}`,
		expect.stringMatching(/^local:SELECT lix_active_branch_commit_id\(\)/),
	]);
	expect(authorityExecute).toHaveBeenCalledOnce();
});

test("an authority-owned batch is fenced locally after authority execution", async () => {
	const statements = [
		{ sql: "SELECT * FROM lix_file", params: [] },
		{ sql: "UPDATE lix_file SET path = '/b'", params: [] },
	];
	const localExecute = vi.fn(async () => fence("active-2"));
	const localExecuteBatch = vi.fn(async () => {
		throw authorityRequired("mutation");
	});
	const authorityExecuteBatch = vi.fn(async () => [result("read"), result("updated")]);
	const authorityExecute = vi.fn();
	const binding = authoritativeHotBinding(
		{ execute: localExecute, executeBatch: localExecuteBatch } as unknown as LixBinding,
		{
			execute: authorityExecute,
			executeBatch: authorityExecuteBatch,
		} as unknown as LixBinding,
	);

	await expect(binding.executeBatch(statements)).resolves.toEqual([
		result("read"),
		result("updated"),
	]);
	expect(localExecuteBatch).toHaveBeenCalledOnce();
	expect(authorityExecuteBatch).toHaveBeenCalledWith(statements, undefined);
	expect(localExecute).toHaveBeenCalledOnce();
	expect(localExecute.mock.calls[0]?.[0]).toContain("lix_active_branch_commit_id");
	expect(authorityExecute).not.toHaveBeenCalled();
});

test("fails closed when the post-mutation local fence is malformed", async () => {
	const malformed = fence();
	malformed.rows[0]![0] = { kind: "null" };
	const localExecute = vi.fn(async (sql: string) => {
		if (sql.startsWith("UPDATE")) throw authorityRequired("mutation");
		return malformed;
	});
	const binding = authoritativeHotBinding(
		{ execute: localExecute } as unknown as LixBinding,
		{ execute: vi.fn(async () => result("updated")) } as unknown as LixBinding,
	);

	await expect(binding.execute("UPDATE lix_file SET path = '/b'", [])).rejects.toMatchObject({
		code: "LIX_AUTHORITY_PUBLICATION_FAILED",
	});
});

test("routes observations to authority because a local stream has no freshness lease", async () => {
	const localObserve = vi.fn();
	const authorityEvents = {} as ObserveEventsBinding;
	const authorityObserve = vi.fn(async () => authorityEvents);
	const binding = authoritativeHotBinding(
		{ observe: localObserve } as unknown as LixBinding,
		{ observe: authorityObserve } as unknown as LixBinding,
	);

	await expect(binding.observe("SELECT * FROM lix_file", [])).resolves.toBe(
		authorityEvents,
	);
	expect(localObserve).not.toHaveBeenCalled();
});

test("realigns both sessions when a local branch switch fails", async () => {
	let localBranch = "main";
	let authorityBranch = "main";
	const local = {
		activeBranchId: vi.fn(async () => localBranch),
		execute: vi.fn(async (sql: string) =>
			sql.includes("lix_active_branch_commit_id")
				? fence()
				: result(`local-${localBranch}`),
		),
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => {
			if (branchId === "feature") throw new Error("local switch failed");
			localBranch = branchId;
			return { branchId };
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const authority = {
		activeBranchId: vi.fn(async () => authorityBranch),
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => {
			authorityBranch = branchId;
			return { branchId };
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const binding = authoritativeHotBinding(local, authority);

	await expect(binding.switchBranch({ branchId: "feature" })).rejects.toThrow(
		"local switch failed",
	);
	expect(localBranch).toBe("main");
	expect(authorityBranch).toBe("main");
	await expect(binding.execute("SELECT * FROM lix_file", [])).resolves.toEqual(
		result("local-main"),
	);
});

test("rejects and closes a child session whose authoritative selection differs", async () => {
	const localChildClose = vi.fn(async () => undefined);
	const authorityChildClose = vi.fn(async () => undefined);
	const localChild = {
		execute: vi.fn(async () => fence()),
		activeBranchId: vi.fn(async () => "main"),
		activeAccountId: vi.fn(async () => "account-a"),
		close: localChildClose,
	} as unknown as LixBinding;
	const authorityChild = {
		activeBranchId: vi.fn(async () => "feature"),
		activeAccountId: vi.fn(async () => "account-a"),
		close: authorityChildClose,
	} as unknown as LixBinding;
	const binding = authoritativeHotBinding(
		{ openAnotherSession: vi.fn(async () => localChild) } as unknown as LixBinding,
		{ openAnotherSession: vi.fn(async () => authorityChild) } as unknown as LixBinding,
	);

	await expect(binding.openAnotherSession({})).rejects.toMatchObject({
		code: "LIX_AUTHORITY_PUBLICATION_FAILED",
	});
	expect(localChildClose).toHaveBeenCalledOnce();
	expect(authorityChildClose).toHaveBeenCalledOnce();
});

test("a hot read cannot cross a partially applied composite branch switch", async () => {
	let localBranch = "main";
	let authorityBranch = "main";
	const authoritySwitched = deferred<void>();
	const finishAuthoritySwitch = deferred<void>();
	const localExecute = vi.fn(async (sql: string) =>
		sql.includes("lix_active_branch_commit_id")
			? fence()
			: result(`local-${localBranch}`),
	);
	const local = {
		execute: localExecute,
		activeBranchId: vi.fn(async () => localBranch),
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => {
			localBranch = branchId;
			return { branchId };
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const authority = {
		activeBranchId: vi.fn(async () => authorityBranch),
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => {
			authorityBranch = branchId;
			authoritySwitched.resolve();
			await finishAuthoritySwitch.promise;
			return { branchId };
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const binding = authoritativeHotBinding(local, authority);

	const switching = binding.switchBranch({ branchId: "feature" });
	await authoritySwitched.promise;
	const reading = binding.execute("SELECT * FROM lix_file", []);
	await Promise.resolve();
	expect(localExecute).not.toHaveBeenCalled();

	finishAuthoritySwitch.resolve();
	await expect(switching).resolves.toEqual({ branchId: "feature" });
	await expect(reading).resolves.toEqual(result("local-feature"));
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
