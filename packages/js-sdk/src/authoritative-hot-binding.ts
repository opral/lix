import type {
	BindingExecuteResult,
	LixBinding,
	LixTransactionBinding,
} from "./binding-types.js";

const AUTHORITY_REQUIRED = "LIX_AUTHORITY_EXECUTION_REQUIRED";
const PUBLICATION_FENCE_SQL =
	"SELECT lix_active_branch_commit_id() AS active_commit_id";

type AuthorityExecutionKind = "history" | "mutation";

/**
 * One connected Lix surface with a deliberately asymmetric data path:
 * certified current reads stay on the local hot replica, while history and
 * mutations execute on the authority. The local Rust execution path fences
 * hot reads against the current authority publication before serving rows.
 */
export function authoritativeHotBinding(
	local: LixBinding,
	authority: LixBinding,
): LixBinding {
	let closed = false;
	const branchGate = new BranchStabilityGate();
	const requireOpen = () => {
		if (closed) throw closedError();
	};
	const waitAfterMutation = async <T>(operation: Promise<T>): Promise<T> => {
		const result = await operation;
		await waitForAuthorityPublication(local, authority);
		return result;
	};
	const withStableBranch = <T>(operation: () => Promise<T>): Promise<T> =>
		branchGate.withShared(async () => {
			requireOpen();
			return operation();
		});
	const executeRouted = async <T>(
		localExecution: () => Promise<T>,
		authorityExecution: () => Promise<T>,
	): Promise<T> => {
		try {
			return await localExecution();
		} catch (error) {
			const kind = authorityExecutionKind(error);
			if (kind === "history") return authorityExecution();
			if (kind === "mutation") {
				return waitAfterMutation(authorityExecution());
			}
			throw error;
		}
	};

	return {
		openReport: () => local.openReport?.(),
		setTelemetryParent: (parent) => {
			local.setTelemetryParent(parent);
			authority.setTelemetryParent(parent);
		},
		openAnotherSession: (options) => withStableBranch(async () => {
			const authoritySession = await authority.openAnotherSession(options);
			let localSession: LixBinding | undefined;
			try {
				localSession = await local.openAnotherSession(options);
				await waitForAuthorityPublication(localSession, authoritySession);
				await assertAuthoritySessionAlignment(localSession, authoritySession);
				return authoritativeHotBinding(localSession, authoritySession);
			} catch (error) {
				await Promise.allSettled([
					localSession?.close(),
					authoritySession.close(),
				]);
				throw error;
			}
		}),
		execute: (sql, params, options) => withStableBranch(async () =>
			executeRouted(
				() => local.execute(sql, params, options),
				() => authority.execute(sql, params, options),
			),
		),
		executeBatch: (statements, options) => withStableBranch(async () =>
			executeRouted(
				() => local.executeBatch(statements, options),
				() => authority.executeBatch(statements, options),
			),
		),
		observe: (sql, params) => withStableBranch(async () => {
			// An ongoing local stream cannot prove freshness during a partition:
			// it may retain or emit a previously certified but now superseded row.
			// Keep observations server-first until the protocol has expiring read
			// leases rather than weakening the connected-state invariant.
			return authority.observe(sql, params);
		}),
		beginTransaction: async () => {
			const release = await branchGate.acquireShared();
			try {
				requireOpen();
				return authoritativeTransaction(
					await authority.beginTransaction(),
					() => waitForAuthorityPublication(local, authority),
					release,
				);
			} catch (error) {
				release();
				throw error;
			}
		},
		activeBranchId: () => withStableBranch(async () => {
			const [localBranch, authorityBranch] = await Promise.all([
				local.activeBranchId(),
				authority.activeBranchId(),
			]);
			if (localBranch !== authorityBranch) {
				throw protocolError("authoritative hot sessions have different active branches");
			}
			return authorityBranch;
		}),
		activeAccountId: () => withStableBranch(async () => {
			const [localAccount, authorityAccount] = await Promise.all([
				local.activeAccountId(),
				authority.activeAccountId(),
			]);
			if (localAccount !== authorityAccount) {
				throw protocolError("authoritative hot sessions have different active accounts");
			}
			return authorityAccount;
		}),
		createBranch: (options) => withStableBranch(async () => {
			const result = await authority.createBranch(options);
			await waitForAuthorityPublication(local, authority);
			return result;
		}),
		undo: () => withStableBranch(() => waitAfterMutation(authority.undo())),
		redo: () => withStableBranch(() => waitAfterMutation(authority.redo())),
		switchBranch: (options) => branchGate.withExclusive(async () => {
			requireOpen();
			const [previousLocalBranch, previousAuthorityBranch] = await Promise.all([
				local.activeBranchId(),
				authority.activeBranchId(),
			]);
			if (previousLocalBranch !== previousAuthorityBranch) {
				closed = true;
				await Promise.allSettled([local.close(), authority.close()]);
				throw protocolError("authoritative hot sessions have different active branches");
			}
			try {
				const result = await authority.switchBranch(options);
				await local.switchBranch(options);
				await waitForAuthorityPublication(local, authority);
				return result;
			} catch (error) {
				try {
					await authority.switchBranch({ branchId: previousAuthorityBranch });
					await local.switchBranch({ branchId: previousLocalBranch });
					await waitForAuthorityPublication(local, authority);
					const [localBranch, authorityBranch] = await Promise.all([
						local.activeBranchId(),
						authority.activeBranchId(),
					]);
					if (
						localBranch !== previousLocalBranch ||
						authorityBranch !== previousAuthorityBranch
					) {
						throw protocolError("failed to restore the composite branch selection");
					}
				} catch {
					closed = true;
					await Promise.allSettled([local.close(), authority.close()]);
					throw protocolError(
						"branch switch failed and the authoritative hot sessions could not be realigned",
					);
				}
				throw error;
			}
		}),
		importFilesystemPaths: async () => {
			throw authorityOnlyFilesystemError("importFilesystemPaths");
		},
		mergeBranchPreview: (options) => withStableBranch(() =>
			authority.mergeBranchPreview(options),
		),
		mergeBranch: (options) => withStableBranch(() =>
			waitAfterMutation(authority.mergeBranch(options)),
		),
		syncDiskToLix: async () => {
			throw authorityOnlyFilesystemError("syncDiskToLix");
		},
		exportSnapshot: authority.exportSnapshot
			? () => authority.exportSnapshot!()
			: undefined,
		close: () => branchGate.withExclusive(async () => {
			if (closed) return;
			closed = true;
			const [localResult, authorityResult] = await Promise.allSettled([
				local.close(),
				authority.close(),
			]);
			if (localResult.status === "rejected") throw localResult.reason;
			if (authorityResult.status === "rejected") throw authorityResult.reason;
		}),
	};
}

function authoritativeTransaction(
	transaction: LixTransactionBinding,
	waitForPublication: () => Promise<void>,
	releaseBranch: () => void,
): LixTransactionBinding {
	let finished = false;
	const finish = async (operation: () => Promise<void>, wait: boolean) => {
		if (finished) return operation();
		finished = true;
		try {
			await operation();
			if (wait) await waitForPublication();
		} finally {
			releaseBranch();
		}
	};
	return {
		execute: (sql, params, options) =>
			transaction.execute(sql, params, options),
		commit: () => finish(() => transaction.commit(), true),
		rollback: () => finish(() => transaction.rollback(), false),
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

export async function waitForAuthorityPublication(
	local: LixBinding,
	_authority: LixBinding,
): Promise<void> {
	// Every hot execution on a connected Rust/worker binding starts with the
	// private sync publication fence. One ordinary existing scalar query is
	// therefore sufficient to wait until the local serving plane has consumed
	// the authority head; no JS-visible cursor or routing API is needed.
	const result = await local.execute(PUBLICATION_FENCE_SQL, []);
	readTextCoordinate(result, 0, "active commit id");
}

export async function assertAuthoritySessionAlignment(
	local: LixBinding,
	authority: LixBinding,
): Promise<void> {
	const [localBranch, authorityBranch, localAccount, authorityAccount] =
		await Promise.all([
			local.activeBranchId(),
			authority.activeBranchId(),
			local.activeAccountId(),
			authority.activeAccountId(),
		]);
	if (localBranch !== authorityBranch || localAccount !== authorityAccount) {
		throw protocolError(
			"certified local and authoritative sessions selected different branch or account state",
		);
	}
}

function readTextCoordinate(
	result: BindingExecuteResult,
	column: number,
	name: string,
): string {
	const value = result.rows[0]?.[column];
	if (value?.kind !== "text" || value.value.length === 0) {
		throw protocolError(`repository authority returned an invalid ${name}`);
	}
	return value.value;
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

type GateWaiter = {
	readonly kind: "shared" | "exclusive";
	readonly resolve: (release: () => void) => void;
};

/** A fair async RW gate: queued writers prevent later readers from starving them. */
class BranchStabilityGate {
	private readers = 0;
	private writer = false;
	private readonly queue: GateWaiter[] = [];

	async acquireShared(): Promise<() => void> {
		if (!this.writer && this.queue.length === 0) {
			this.readers += 1;
			return this.sharedRelease();
		}
		return new Promise((resolve) => {
			this.queue.push({ kind: "shared", resolve });
			this.drain();
		});
	}

	private async acquireExclusive(): Promise<() => void> {
		if (!this.writer && this.readers === 0 && this.queue.length === 0) {
			this.writer = true;
			return this.exclusiveRelease();
		}
		return new Promise((resolve) => {
			this.queue.push({ kind: "exclusive", resolve });
			this.drain();
		});
	}

	async withShared<T>(operation: () => Promise<T>): Promise<T> {
		const release = await this.acquireShared();
		try {
			return await operation();
		} finally {
			release();
		}
	}

	async withExclusive<T>(operation: () => Promise<T>): Promise<T> {
		const release = await this.acquireExclusive();
		try {
			return await operation();
		} finally {
			release();
		}
	}

	private sharedRelease(): () => void {
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.readers -= 1;
			this.drain();
		};
	}

	private exclusiveRelease(): () => void {
		let released = false;
		return () => {
			if (released) return;
			released = true;
			this.writer = false;
			this.drain();
		};
	}

	private drain(): void {
		if (this.writer || this.readers !== 0) return;
		const first = this.queue[0];
		if (!first) return;
		if (first.kind === "exclusive") {
			this.queue.shift();
			this.writer = true;
			first.resolve(this.exclusiveRelease());
			return;
		}
		while (this.queue[0]?.kind === "shared") {
			const waiter = this.queue.shift();
			if (!waiter) break;
			this.readers += 1;
			waiter.resolve(this.sharedRelease());
		}
	}
}
