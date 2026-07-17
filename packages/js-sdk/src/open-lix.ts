import {
	localFilesystemAlreadyOpen,
	localFilesystemNotOpen,
	invalidArgument,
} from "./errors.js";
import type {
	BindingObserveEvent,
	LixBinding,
	LixTransactionBinding,
	ObserveEventsBinding,
} from "./binding-types.js";
import { normalizeOptionals, wrapExecuteResult } from "./result.js";
import { normalizeParam, toNativeValue } from "./value.js";
import { openLixWorkerBinding } from "./worker/client.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
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
	transaction: LixTransactionBinding;
}>(({ transaction }) => {
	void transaction.rollback().catch(() => undefined);
});
const observeFinalizer = new FinalizationRegistry<{
	observe: Promise<ObserveEventsBinding | undefined>;
}>(({ observe }) => {
	void observe.then((events) => {
		events?.close();
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

const openLocalFilesystems = new WeakMap<LocalFilesystem, LixBinding | null>();

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
		await this.client("importPaths").importFilesystemPaths([...paths]);
	}

	async syncDiskToLix(): Promise<void> {
		return this.client("syncDiskToLix").syncDiskToLix();
	}

	private client(operation: string): LixBinding {
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
		return new Lix(await openLixWorkerBinding({ kind: "memory" }));
	}
	if (options.storage instanceof SQLite) {
		return new Lix(
			await openLixWorkerBinding({
				kind: "sqlite",
				path: options.storage.path,
			}),
		);
	}
	if (options.storage instanceof LocalFilesystem) {
		const storage = options.storage;
		if (openLocalFilesystems.has(storage)) {
			throw localFilesystemAlreadyOpen();
		}
		openLocalFilesystems.set(storage, null);
		try {
			const binding = await openLixWorkerBinding(
				{
					kind: "localFilesystem",
					path: storage.path,
					lixDir: storage.lixDir,
					syncAllFiles: storage.syncAllFiles,
				},
				() => openLocalFilesystems.delete(storage),
			);
			openLocalFilesystems.set(storage, binding);
			return new Lix(binding);
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

	constructor(private readonly binding: LixBinding) {}

	async execute(
		sql: string,
		params: SqlParam[] = [],
		options?: ExecuteOptions,
	): Promise<ExecuteResult> {
		assertExecuteArgs("lix", sql, params, options);
		return wrapExecuteResult(
			await this.binding.execute(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
				options,
			),
		);
	}

	observe(sql: string, params: SqlParam[] = []): ObserveEvents {
		assertSqlArgs("observe", "lix", sql, params);
		return new ObserveEvents(
			this.binding.observe(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
			),
		);
	}

	async beginTransaction(): Promise<LixTransaction> {
		return new LixTransaction(await this.binding.beginTransaction());
	}

	async activeBranchId(): Promise<string> {
		return this.binding.activeBranchId();
	}

	async createBranch(
		options: CreateBranchOptions,
	): Promise<CreateBranchReceipt> {
		return this.binding.createBranch(options);
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		return this.binding.switchBranch(options);
	}

	async mergeBranchPreview(
		options: MergeBranchOptions,
	): Promise<MergeBranchPreview> {
		return normalizeOptionals(
			await this.binding.mergeBranchPreview(options),
		);
	}

	async mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt> {
		const receipt = normalizeOptionals<MergeBranchReceipt>(
			await this.binding.mergeBranch(options),
		);
		receipt.createdMergeCommitId ??= null;
		return receipt;
	}

	async close(): Promise<void> {
		this.closePromise ??= (async () => {
			await this.binding.close();
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
	private readonly observeBinding: Promise<ObserveEventsBinding | undefined>;

	constructor(observeBinding: Promise<ObserveEventsBinding>) {
		const setup = this.setup;
		this.observeBinding = observeBinding.catch((error: unknown) => {
			setup.error = error;
			return undefined;
		});
		observeFinalizer.register(
			this,
			{ observe: this.observeBinding },
			this,
		);
	}

	async next(): Promise<ObserveEvent | undefined> {
		if (this.closed) return undefined;
		const binding = await this.observeBinding;
		if (binding === undefined) {
			throw this.setup.error;
		}
		const event: BindingObserveEvent | null | undefined = await binding.next();
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
		void this.observeBinding.then((binding) => {
			binding?.close();
		});
	}
}

export class LixTransaction {
	constructor(private readonly binding: LixTransactionBinding) {
		transactionFinalizer.register(
			this,
			{ transaction: binding },
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
			await this.binding.execute(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
				options,
			),
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
		return kind === "transaction.commit"
			? this.binding.commit()
			: this.binding.rollback();
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
