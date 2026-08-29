import type {
	BindingExecuteResult,
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "./binding-types.js";

const AUTHORITY_REQUIRED = "LIX_AUTHORITY_EXECUTION_REQUIRED";
const COORDINATE_SQL =
	"SELECT lix_active_branch_commit_id() AS head_commit_id, lix_latest_checkpoint_commit_id() AS checkpoint_commit_id";
const CATCH_UP_TIMEOUT_MS = 30_000;

type AuthorityExecutionKind = "history" | "mutation";

/**
 * One connected Lix surface with a deliberately asymmetric data path:
 * certified current reads stay on the local hot replica, while history and
 * mutations execute on the authority. A successful mutation is not released
 * until the local serving coordinate catches the authority coordinate.
 */
export function authoritativeHotBinding(
	local: LixBinding,
	authority: LixBinding,
): LixBinding {
	let closed = false;
	const requireOpen = () => {
		if (closed) throw closedError();
	};
	const waitAfterMutation = async <T>(operation: Promise<T>): Promise<T> => {
		const result = await operation;
		await waitForLocalCoordinate(local, authority);
		return result;
	};

	return {
		openReport: () => local.openReport?.(),
		setTelemetryParent: (parent) => {
			local.setTelemetryParent(parent);
			authority.setTelemetryParent(parent);
		},
		openAnotherSession: async (options) => {
			requireOpen();
			const [localSession, authoritySession] = await Promise.all([
				local.openAnotherSession(options),
				authority.openAnotherSession(options),
			]);
			return authoritativeHotBinding(localSession, authoritySession);
		},
		execute: async (sql, params, options) => {
			requireOpen();
			try {
				return await local.execute(sql, params, options);
			} catch (error) {
				const kind = authorityExecutionKind(error);
				if (kind === null) throw error;
				const execution = authority.execute(sql, params, options);
				return kind === "mutation" ? waitAfterMutation(execution) : execution;
			}
		},
		executeBatch: async (statements, options) => {
			requireOpen();
			try {
				return await local.executeBatch(statements, options);
			} catch (error) {
				const kind = authorityExecutionKind(error);
				if (kind === null) throw error;
				const execution = authority.executeBatch(statements, options);
				return kind === "mutation" ? waitAfterMutation(execution) : execution;
			}
		},
		observe: async (sql, params) => {
			requireOpen();
			try {
				return await local.observe(sql, params);
			} catch (error) {
				if (authorityExecutionKind(error) !== "history") throw error;
				return authority.observe(sql, params);
			}
		},
		beginTransaction: async () => {
			requireOpen();
			return authoritativeTransaction(await authority.beginTransaction(), () =>
				waitForLocalCoordinate(local, authority),
			);
		},
		activeBranchId: () => {
			requireOpen();
			return local.activeBranchId();
		},
		activeAccountId: () => {
			requireOpen();
			return local.activeAccountId();
		},
		createBranch: async (options) => {
			requireOpen();
			const result = await authority.createBranch(options);
			await waitForLocalBranch(local, result.id);
			return result;
		},
		undo: () => {
			requireOpen();
			return waitAfterMutation(authority.undo());
		},
		redo: () => {
			requireOpen();
			return waitAfterMutation(authority.redo());
		},
		switchBranch: async (options) => {
			requireOpen();
			const result = await authority.switchBranch(options);
			await waitForLocalBranch(local, result.branchId);
			await local.switchBranch(options);
			await waitForLocalCoordinate(local, authority);
			return result;
		},
		importFilesystemPaths: async () => {
			throw authorityOnlyFilesystemError("importFilesystemPaths");
		},
		mergeBranchPreview: (options) => {
			requireOpen();
			return authority.mergeBranchPreview(options);
		},
		mergeBranch: (options) => {
			requireOpen();
			return waitAfterMutation(authority.mergeBranch(options));
		},
		syncDiskToLix: async () => {
			throw authorityOnlyFilesystemError("syncDiskToLix");
		},
		exportSnapshot: authority.exportSnapshot
			? () => authority.exportSnapshot!()
			: undefined,
		close: async () => {
			if (closed) return;
			closed = true;
			const [localResult, authorityResult] = await Promise.allSettled([
				local.close(),
				authority.close(),
			]);
			if (localResult.status === "rejected") throw localResult.reason;
			if (authorityResult.status === "rejected") throw authorityResult.reason;
		},
	};
}

function authoritativeTransaction(
	transaction: LixTransactionBinding,
	waitForPublication: () => Promise<void>,
): LixTransactionBinding {
	return {
		execute: (sql, params, options) =>
			transaction.execute(sql, params, options),
		commit: async () => {
			await transaction.commit();
			await waitForPublication();
		},
		rollback: () => transaction.rollback(),
	};
}

function authorityExecutionKind(error: unknown): AuthorityExecutionKind | null {
	if (
		typeof error !== "object" ||
		error === null ||
		!("code" in error) ||
		error.code !== AUTHORITY_REQUIRED
	) {
		return null;
	}
	const details = "details" in error ? error.details : undefined;
	if (
		typeof details !== "object" ||
		details === null ||
		!("executionKind" in details)
	) {
		return null;
	}
	return details.executionKind === "mutation"
		? "mutation"
		: details.executionKind === "history"
			? "history"
			: null;
}

async function waitForLocalCoordinate(
	local: LixBinding,
	authority: LixBinding,
): Promise<void> {
	// Re-read both sides on every attempt. Pinning the first authority value can
	// deadlock if another accepted mutation advances the replica past it before
	// this waiter observes equality.
	await waitUntil(async () =>
		sameCoordinate(
			await readCoordinate(local),
			await readCoordinate(authority),
		),
	);
}

async function waitForLocalBranch(
	local: LixBinding,
	branchId: string,
): Promise<void> {
	await waitUntil(async () => {
		const result = await local.execute(
			"SELECT id FROM lix_branch WHERE id = $1",
			[{ kind: "text", value: branchId }],
		);
		return result.rows.length === 1;
	});
}

async function readCoordinate(
	binding: LixBinding,
): Promise<readonly [string, string]> {
	const result = await binding.execute(COORDINATE_SQL, []);
	const row = result.rows[0];
	const head = row?.[0];
	const checkpoint = row?.[1];
	if (head?.kind !== "text" || checkpoint?.kind !== "text") {
		throw protocolError(
			"repository authority returned an invalid hot coordinate",
		);
	}
	return [head.value, checkpoint.value];
}

function sameCoordinate(
	actual: readonly [string, string],
	expected: readonly [string, string],
): boolean {
	return actual[0] === expected[0] && actual[1] === expected[1];
}

async function waitUntil(predicate: () => Promise<boolean>): Promise<void> {
	const deadline = Date.now() + CATCH_UP_TIMEOUT_MS;
	let delayMs = 1;
	for (;;) {
		if (await predicate()) return;
		if (Date.now() >= deadline) {
			throw protocolError(
				"authority mutation committed, but the certified local hot state did not catch up",
			);
		}
		await new Promise((resolve) => setTimeout(resolve, delayMs));
		delayMs = Math.min(delayMs * 2, 100);
	}
}

function authorityOnlyFilesystemError(
	operation: string,
): Error & { code: string } {
	return protocolError(
		`${operation} is unavailable on an authoritative hot replica; write repository rows through SQL`,
	);
}

function protocolError(message: string): Error & { code: string } {
	const error = new Error(message) as Error & { code: string };
	error.name = "LixError";
	error.code = "LIX_AUTHORITY_PUBLICATION_FAILED";
	return error;
}

function closedError(): Error & { code: string } {
	const error = new Error(
		"authoritative hot Lix binding is closed",
	) as Error & {
		code: string;
	};
	error.name = "LixError";
	error.code = "LIX_ERROR_CLOSED";
	return error;
}
