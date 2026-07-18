import {
	localFilesystemAlreadyOpen,
	localFilesystemNotOpen,
	invalidArgument,
} from "./errors.js";
import type {
	BindingExecuteResult,
	BindingObserveEvent,
} from "./binding-types.js";
import { normalizeOptionals, wrapExecuteResult } from "./result.js";
import { normalizeParam, toNativeValue } from "./value.js";
import { LixWorkerClient, openLixWorker } from "./worker/client.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	LixBatchOptions,
	LixBatchStatement,
	ExecuteOptions,
	ExecuteResult,
	LocalFilesystemOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	ObserveEvent,
	OpenLixOptions,
	SqlParam,
	SQLiteOptions,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "./types.js";

const transactionFinalizer = new FinalizationRegistry<{
	client: LixWorkerClient;
	transactionId: number;
}>(({ client, transactionId }) => {
	client.notify({ kind: "transaction.abandon", transactionId });
});
const observeFinalizer = new FinalizationRegistry<{
	client: LixWorkerClient;
	observeId: Promise<number | undefined>;
}>(({ client, observeId }) => {
	void observeId.then((id) => {
		if (id !== undefined) client.notify({ kind: "observe.close", observeId: id });
	});
});

export class SQLite {
	readonly path: string;

	constructor(options: SQLiteOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("SQLite requires a non-empty path");
		}
		this.path = options.path;
	}
}

const openLocalFilesystems = new WeakMap<LocalFilesystem, LixWorkerClient | null>();

export class LocalFilesystem {
	readonly path: string;
	readonly lixDir: string | undefined;
	readonly syncAllFiles: boolean;

	constructor(options: LocalFilesystemOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("LocalFilesystem requires a non-empty path");
		}
		if (
			options.lixDir !== undefined &&
			(typeof options.lixDir !== "string" || options.lixDir.length === 0)
		) {
			throw new TypeError("LocalFilesystem lixDir must be a non-empty string");
		}
		if (typeof options.syncAllFiles !== "boolean") {
			throw new TypeError("LocalFilesystem syncAllFiles must be a boolean");
		}
		this.path = options.path;
		this.lixDir = options.lixDir;
		this.syncAllFiles = options.syncAllFiles;
	}

	async importPaths(paths: readonly string[]): Promise<void> {
		if (!Array.isArray(paths)) {
			throw new TypeError("importPaths() paths must be an array");
		}
		for (const path of paths) {
			if (typeof path !== "string" || path.length === 0) {
				throw new TypeError(
					"importPaths() paths must contain non-empty strings",
				);
			}
			if (path.endsWith("/")) {
				throw new TypeError(
					"importPaths() paths must contain file paths, not directory paths",
				);
			}
		}
		await this.client("importPaths").request({
			kind: "importFilesystemPaths",
			paths: [...paths],
		});
	}

	async syncDiskToLix(): Promise<void> {
		return this.client("syncDiskToLix").request({ kind: "syncDiskToLix" });
	}

	private client(operation: string): LixWorkerClient {
		const client = openLocalFilesystems.get(this);
		if (!client) {
			throw localFilesystemNotOpen(operation);
		}
		return client;
	}
}

export async function openLix(options: OpenLixOptions = {}): Promise<Lix> {
	if (!options || typeof options !== "object") {
		throw new TypeError("openLix() options must be an object");
	}
	if ("backend" in options) {
		throw new TypeError(
			"openLix() option 'backend' was removed; use 'storage' instead",
		);
	}
	if (options.storage === undefined) {
		return new Lix(await openLixWorker({ kind: "memory" }));
	}
	if (options.storage instanceof SQLite) {
		return new Lix(
			await openLixWorker({ kind: "sqlite", path: options.storage.path }),
		);
	}
	if (options.storage instanceof LocalFilesystem) {
		const storage = options.storage;
		if (openLocalFilesystems.has(storage)) {
			throw localFilesystemAlreadyOpen();
		}
		openLocalFilesystems.set(storage, null);
		try {
			const client = await openLixWorker(
				{
					kind: "localFilesystem",
					path: storage.path,
					lixDir: storage.lixDir,
					syncAllFiles: storage.syncAllFiles,
				},
				() => openLocalFilesystems.delete(storage),
			);
			openLocalFilesystems.set(storage, client);
			return new Lix(client);
		} catch (error) {
			openLocalFilesystems.delete(storage);
			throw error;
		}
	}
	throw new TypeError(
		"openLix() requires storage to be SQLite or LocalFilesystem",
	);
}

export class Lix {
	private closePromise: Promise<void> | undefined;

	constructor(private readonly client: LixWorkerClient) {}

	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult> {
		assertExecuteArgs("lix", sql, params, options);
		return wrapExecuteResult(
			await this.client.request<BindingExecuteResult>({
				kind: "execute",
				sql,
				params: params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
				options,
			}),
		);
	}

	async executeBatch(
		statements: readonly LixBatchStatement[],
		options?: LixBatchOptions,
	): Promise<readonly ExecuteResult[]> {
		const normalizedStatements = normalizeBatchStatements(statements, options);
		const results = await this.client.request<BindingExecuteResult[]>({
			kind: "executeBatch",
			statements: normalizedStatements,
			options,
		});
		return results.map(wrapExecuteResult);
	}

	observe(sql: string, params: SqlParam[] = []): ObserveEvents {
		assertSqlArgs("observe", "lix", sql, params);
		return new ObserveEvents(
			this.client,
			this.client.request<number>({
				kind: "observe",
				sql,
				params: params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
			}),
		);
	}

	async beginTransaction(): Promise<LixTransaction> {
		const transactionId = await this.client.request<number>({
			kind: "beginTransaction",
		});
		return new LixTransaction(this.client, transactionId);
	}

	async activeBranchId(): Promise<string> {
		return this.client.request({ kind: "activeBranchId" });
	}

	async createBranch(
		options: CreateBranchOptions,
	): Promise<CreateBranchReceipt> {
		return this.client.request({ kind: "createBranch", options });
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		return this.client.request({ kind: "switchBranch", options });
	}

	async mergeBranchPreview(
		options: MergeBranchOptions,
	): Promise<MergeBranchPreview> {
		return normalizeOptionals(
			await this.client.request({ kind: "mergeBranchPreview", options }),
		);
	}

	async mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt> {
		const receipt = normalizeOptionals<MergeBranchReceipt>(
			await this.client.request({ kind: "mergeBranch", options }),
		);
		receipt.createdMergeCommitId ??= null;
		return receipt;
	}

	async close(): Promise<void> {
		this.closePromise ??= (async () => {
			await this.client.request({ kind: "close" });
			await this.client.terminate();
		})();
		try {
			await this.closePromise;
		} catch (error) {
			this.closePromise = undefined;
			throw error;
		}
	}
}

export class ObserveEvents {
	private readonly setup: { error?: unknown } = {};
	private closed = false;
	private readonly observeId: Promise<number | undefined>;

	constructor(
		private readonly client: LixWorkerClient,
		observeId: Promise<number>,
	) {
		const setup = this.setup;
		this.observeId = observeId.catch((error: unknown) => {
			setup.error = error;
			return undefined;
		});
		observeFinalizer.register(
			this,
			{ client, observeId: this.observeId },
			this,
		);
	}

	async next(): Promise<ObserveEvent | undefined> {
		if (this.closed) return undefined;
		const observeId = await this.observeId;
		if (observeId === undefined) {
			throw this.setup.error;
		}
		const event = await this.client.request<BindingObserveEvent | undefined>({
			kind: "observe.next",
			observeId,
		});
		if (event == null) {
			return undefined;
		}
		return {
			sequence: event.sequence,
			mutationSequence: event.mutationSequence,
			result: wrapExecuteResult(event.rows),
		};
	}

	close(): void {
		if (this.closed) return;
		this.closed = true;
		observeFinalizer.unregister(this);
		void this.observeId.then((observeId) => {
			if (observeId !== undefined) {
				this.client.notify({ kind: "observe.close", observeId });
			}
		});
	}
}

export class LixTransaction {
	constructor(
		private readonly client: LixWorkerClient,
		private readonly transactionId: number,
	) {
		transactionFinalizer.register(
			this,
			{ client, transactionId },
			this,
		);
	}

	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult> {
		assertExecuteArgs("lixTransaction", sql, params, options);
		return wrapExecuteResult(
			await this.client.request<BindingExecuteResult>({
				kind: "transaction.execute",
				transactionId: this.transactionId,
				sql,
				params: params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
				options,
			}),
		);
	}

	async commit(): Promise<void> {
		return this.finish("transaction.commit");
	}

	async rollback(): Promise<void> {
		return this.finish("transaction.rollback");
	}

	private async finish(
		kind: "transaction.commit" | "transaction.rollback",
	): Promise<void> {
		transactionFinalizer.unregister(this);
		return this.client.request({ kind, transactionId: this.transactionId });
	}
}

function assertExecuteArgs(
	receiver: string,
	sql: string,
	params: SqlParam[],
	options?: ExecuteOptions,
) {
	assertSqlArgs("execute", receiver, sql, params);
	if (options === undefined) {
		return;
	}
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		throw invalidArgument(
			"execute",
			"options",
			"object",
			typeof options,
			receiver,
		);
	}
	if (
		options.originKey !== undefined &&
		typeof options.originKey !== "string"
	) {
		throw invalidArgument(
			"execute",
			"options.originKey",
			"string",
			typeof options.originKey,
			receiver,
		);
	}
}

function assertSqlArgs(
	operation: string,
	receiver: string,
	sql: string,
	params: SqlParam[],
) {
	if (typeof sql !== "string") {
		throw invalidArgument(operation, "sql", "string", typeof sql, receiver);
	}
	if (!Array.isArray(params)) {
		throw invalidArgument(
			operation,
			"params",
			"array",
			typeof params,
			receiver,
		);
	}
}

function normalizeBatchStatements(
	statements: readonly LixBatchStatement[],
	options?: LixBatchOptions,
) {
	if (!Array.isArray(statements)) {
		throw invalidArgument(
			"executeBatch",
			"statements",
			"array",
			typeof statements,
		);
	}
	if (statements.length === 0) {
		throw invalidArgument(
			"executeBatch",
			"statements",
			"non-empty array",
			"empty array",
		);
	}
	assertBatchOptions(options);
	return statements.map((statement, statementIndex) => {
		try {
			if (
				!statement ||
				typeof statement !== "object" ||
				Array.isArray(statement)
			) {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}]`,
					"object",
					Array.isArray(statement) ? "array" : typeof statement,
				);
			}
			if (typeof statement.sql !== "string") {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}].sql`,
					"string",
					typeof statement.sql,
				);
			}
			const params = statement.params ?? [];
			if (!Array.isArray(params)) {
				throw invalidArgument(
					"executeBatch",
					`statements[${statementIndex}].params`,
					"array",
					typeof params,
				);
			}
			return {
				sql: statement.sql,
				params: params.map((param, parameterIndex) =>
					toNativeValue(normalizeParam(param, parameterIndex)),
				),
			};
		} catch (error) {
			throw withBatchStatementIndex(error, statementIndex);
		}
	});
}

function assertBatchOptions(options?: LixBatchOptions) {
	if (options === undefined) return;
	if (!options || typeof options !== "object" || Array.isArray(options)) {
		throw invalidArgument(
			"executeBatch",
			"options",
			"object",
			typeof options,
		);
	}
	if (options.originKey !== undefined && typeof options.originKey !== "string") {
		throw invalidArgument(
			"executeBatch",
			"options.originKey",
			"string",
			typeof options.originKey,
		);
	}
}

function withBatchStatementIndex(error: unknown, statementIndex: number): unknown {
	if (!error || typeof error !== "object") return error;
	const lixError = error as { details?: unknown };
	const details = lixError.details;
	lixError.details = {
		...(details && typeof details === "object" && !Array.isArray(details)
			? details
			: details === undefined
				? {}
				: { cause: details }),
		statementIndex,
	};
	return error;
}
