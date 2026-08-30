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

const cursor = (value: string | number): BindingExecuteResult => ({
	columns: [{ name: "cursor", type: "text" }],
	rows: [[{ kind: "text", value: String(value) }]],
	rowsAffected: 0,
	notices: [],
});

test("keeps hot reads local and routes history authority-first", async () => {
	const localExecute = vi.fn(async (sql: string) => {
		if (sql.includes("lix_sync_publication_cursor")) return cursor(7);
		return result("local-hot");
	});
	const executionRoute = vi.fn(async ([sql]: string[]) =>
		sql.includes("lix_history") ? "history" as const : "hot" as const,
	);
	const authorityExecute = vi.fn(async (sql: string) =>
		sql.includes("lix_sync_publication_cursor")
			? cursor(7)
			: result("authority-history"),
	);
	const binding = authoritativeHotBinding(
		{ execute: localExecute, executionRoute } as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(binding.execute("SELECT * FROM lix_file", [])).resolves.toEqual(
		result("local-hot"),
	);
	await expect(
		binding.execute("SELECT * FROM lix_history('lix_file')", []),
	).resolves.toEqual(result("authority-history"));
	expect(authorityExecute).toHaveBeenCalledTimes(2);
	expect(localExecute.mock.calls.map(([sql]) => sql)).toEqual([
		expect.stringContaining("lix_sync_publication_cursor"),
		"SELECT * FROM lix_file",
	]);
});

test("pins the authority cursor and accepts a replica that advances beyond it", async () => {
	let localCursorReads = 0;
	const localExecute = vi.fn(async (sql: string) => {
		localCursorReads += 1;
		return cursor(
			localCursorReads < 3 ? 4 : "18446744073709551615",
		);
	});
	let authorityCursorReads = 0;
	const authorityExecute = vi.fn(async (sql: string) => {
		if (sql.startsWith("UPDATE")) return result("updated");
		authorityCursorReads += 1;
		return cursor("18446744073709551614");
	});
	const binding = authoritativeHotBinding(
		{
			execute: localExecute,
			executionRoute: vi.fn(async () => "mutation" as const),
		} as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(
		binding.execute("UPDATE lix_file SET path = '/b' WHERE path = '/a'", []),
	).resolves.toEqual(result("updated"));
	expect(localCursorReads).toBe(3);
	expect(authorityCursorReads).toBe(1);
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

test("fails closed when the authority publication cursor is malformed", async () => {
	const binding = authoritativeHotBinding(
		{
			execute: vi.fn(),
			executionRoute: vi.fn(async () => "hot" as const),
		} as unknown as LixBinding,
		{ execute: vi.fn(async () => cursor("01")) } as unknown as LixBinding,
	);

	await expect(binding.execute("SELECT * FROM lix_file", [])).rejects.toMatchObject({
		code: "LIX_AUTHORITY_PUBLICATION_FAILED",
	});
});

test("realigns both sessions when a local branch switch fails", async () => {
	let localBranch = "main";
	let authorityBranch = "main";
	const local = {
		activeBranchId: vi.fn(async () => localBranch),
		execute: vi.fn(async (sql: string) =>
			sql.includes("lix_sync_publication_cursor")
				? cursor(3)
				: result(`local-${localBranch}`),
		),
		executionRoute: vi.fn(async () => "hot" as const),
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => {
			if (branchId === "feature") throw new Error("local switch failed");
			localBranch = branchId;
			return { branchId };
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const authority = {
		activeBranchId: vi.fn(async () => authorityBranch),
		execute: vi.fn(async () => cursor(3)),
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
		execute: vi.fn(async () => cursor(5)),
		activeBranchId: vi.fn(async () => "main"),
		activeAccountId: vi.fn(async () => "account-a"),
		close: localChildClose,
	} as unknown as LixBinding;
	const authorityChild = {
		execute: vi.fn(async () => cursor(5)),
		activeBranchId: vi.fn(async () => "feature"),
		activeAccountId: vi.fn(async () => "account-a"),
		close: authorityChildClose,
	} as unknown as LixBinding;
	const binding = authoritativeHotBinding(
		{
			execute: vi.fn(async () => cursor(5)),
			executionRoute: vi.fn(async () => "hot" as const),
			openAnotherSession: vi.fn(async () => localChild),
		} as unknown as LixBinding,
		{
			openAnotherSession: vi.fn(async () => authorityChild),
		} as unknown as LixBinding,
	);

	await expect(binding.openAnotherSession({})).rejects.toMatchObject({
		code: "LIX_AUTHORITY_PUBLICATION_FAILED",
	});
	expect(localChildClose).toHaveBeenCalledOnce();
	expect(authorityChildClose).toHaveBeenCalledOnce();
});

test("history reaches authority even when the replica cursor is stuck", async () => {
	const localExecute = vi.fn(async () => cursor(1));
	const authorityExecute = vi.fn(async (sql: string) =>
		sql.includes("lix_history") ? result("authority-history") : cursor(99),
	);
	const binding = authoritativeHotBinding(
		{
			executionRoute: vi.fn(async () => "history" as const),
			execute: localExecute,
		} as unknown as LixBinding,
		{ execute: authorityExecute } as unknown as LixBinding,
	);

	await expect(
		binding.execute("SELECT * FROM lix_history('lix_file')", []),
	).resolves.toEqual(result("authority-history"));
	expect(localExecute).not.toHaveBeenCalled();
	expect(authorityExecute).toHaveBeenCalledOnce();
});

test("a hot read cannot cross a partially applied composite branch switch", async () => {
	let localBranch = "main";
	let authorityBranch = "main";
	const authoritySwitched = deferred<void>();
	const finishAuthoritySwitch = deferred<void>();
	const localExecute = vi.fn(async (sql: string) =>
		sql.includes("lix_sync_publication_cursor")
			? cursor(8)
			: result(`local-${localBranch}`),
	);
	const local = {
		executionRoute: vi.fn(async () => "hot" as const),
		execute: localExecute,
		activeBranchId: vi.fn(async () => localBranch),
		switchBranch: vi.fn(async ({ branchId }: { branchId: string }) => {
			localBranch = branchId;
			return { branchId };
		}),
		close: vi.fn(async () => undefined),
	} as unknown as LixBinding;
	const authority = {
		execute: vi.fn(async () => cursor(8)),
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
	expect(localBranch).toBe("feature");
	expect(authorityBranch).toBe("feature");
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
