import { invalidArgument } from "./errors.js";
import { addon } from "./native.js";
import { normalizeOptionals, wrapExecuteResult } from "./result.js";
import { normalizeParam, toNativeValue } from "./value.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteResult,
	FsBackendOptions,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	ObserveEvent,
	OpenLixOptions,
	SqlParam,
	SqliteBackendOptions,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "./types.js";

type NativeExecuteResult = Parameters<typeof wrapExecuteResult>[0];
type NativeObserveEvent = {
	sequence: number;
	mutationSequence: number;
	rows: NativeExecuteResult;
};
type NativeParam = ReturnType<typeof toNativeValue>;

type NativeLix = {
	execute(sql: string, params: NativeParam[]): Promise<NativeExecuteResult>;
	observe(sql: string, params: NativeParam[]): Promise<NativeObserveEvents>;
	beginTransaction(): Promise<NativeLixTransaction>;
	activeBranchId(): Promise<string>;
	createBranch(options: CreateBranchOptions): Promise<CreateBranchReceipt>;
	switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchReceipt>;
	mergeBranchPreview(options: MergeBranchOptions): Promise<MergeBranchPreview>;
	mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt>;
	close(): Promise<void>;
};

type NativeLixTransaction = {
	execute(sql: string, params: NativeParam[]): Promise<NativeExecuteResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

type NativeObserveEvents = {
	next(): Promise<NativeObserveEvent | null | undefined>;
	close(): void;
};

export class SqliteBackend {
	readonly path: string;

	constructor(options: SqliteBackendOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("SqliteBackend requires a non-empty path");
		}
		this.path = options.path;
	}
}

export class FsBackend {
	readonly path: string;
	readonly lixDir: string | undefined;
	readonly filter: { includePaths: readonly string[] } | undefined;

	constructor(options: FsBackendOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("FsBackend requires a non-empty path");
		}
		if ("storage" in options) {
			throw new TypeError("FsBackend storage is no longer supported");
		}
		if (
			options.lixDir !== undefined &&
			(typeof options.lixDir !== "string" || options.lixDir.length === 0)
		) {
			throw new TypeError("FsBackend lixDir must be a non-empty string");
		}
		if (options.filter !== undefined) {
			if (!options.filter || typeof options.filter !== "object") {
				throw new TypeError("FsBackend filter must be an object");
			}
			if (!Array.isArray(options.filter.includePaths)) {
				throw new TypeError("FsBackend filter.includePaths must be an array");
			}
			if (options.filter.includePaths.length === 0) {
				throw new TypeError(
					"FsBackend filter.includePaths must contain at least one path",
				);
			}
			for (const includePath of options.filter.includePaths) {
				if (typeof includePath !== "string" || includePath.length === 0) {
					throw new TypeError(
						"FsBackend filter.includePaths must contain non-empty strings",
					);
				}
				if (includePath.endsWith("/")) {
					throw new TypeError(
						"FsBackend filter.includePaths must contain file paths, not directory paths",
					);
				}
			}
		}
		this.path = options.path;
		this.lixDir = options.lixDir;
		this.filter = options.filter
			? { includePaths: [...options.filter.includePaths] }
			: undefined;
	}
}

export async function openLix(options: OpenLixOptions = {}): Promise<Lix> {
	if (!options || typeof options !== "object") {
		throw new TypeError("openLix() options must be an object");
	}
	if (options.backend === undefined) {
		return new Lix(await addon.Lix.openMemory());
	}
	if (options.backend instanceof SqliteBackend) {
		return new Lix(await addon.Lix.openSqlite(options.backend.path));
	}
	if (options.backend instanceof FsBackend) {
		return new Lix(
			await addon.Lix.openFs(
				options.backend.path,
				options.backend.lixDir,
				options.backend.filter?.includePaths,
			),
		);
	}
	throw new TypeError(
		"openLix() requires backend to be SqliteBackend or FsBackend",
	);
}

export class Lix {
	constructor(private readonly native: NativeLix) {}

	async execute(sql: string, params: SqlParam[] = []): Promise<ExecuteResult> {
		assertExecuteArgs("lix", sql, params);
		return wrapExecuteResult(
			await this.native.execute(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
			),
		);
	}

	observe(sql: string, params: SqlParam[] = []): ObserveEvents {
		assertSqlArgs("observe", "lix", sql, params);
		return new ObserveEvents(
			this.native.observe(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
			),
		);
	}

	async beginTransaction(): Promise<LixTransaction> {
		return new LixTransaction(await this.native.beginTransaction());
	}

	async activeBranchId(): Promise<string> {
		return this.native.activeBranchId();
	}

	async createBranch(
		options: CreateBranchOptions,
	): Promise<CreateBranchReceipt> {
		return this.native.createBranch(options);
	}

	async switchBranch(
		options: SwitchBranchOptions,
	): Promise<SwitchBranchReceipt> {
		return this.native.switchBranch(options);
	}

	async mergeBranchPreview(
		options: MergeBranchOptions,
	): Promise<MergeBranchPreview> {
		return normalizeOptionals(await this.native.mergeBranchPreview(options));
	}

	async mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt> {
		const receipt = normalizeOptionals(await this.native.mergeBranch(options));
		receipt.createdMergeCommitId ??= null;
		return receipt;
	}

	async close(): Promise<void> {
		return this.native.close();
	}
}

export class ObserveEvents {
	private setupError: unknown;
	private readonly native: Promise<NativeObserveEvents | undefined>;

	constructor(native: Promise<NativeObserveEvents>) {
		this.native = native.catch((error: unknown) => {
			this.setupError = error;
			return undefined;
		});
	}

	async next(): Promise<ObserveEvent | undefined> {
		const native = await this.native;
		if (native === undefined) {
			throw this.setupError;
		}
		const event = await native.next();
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
		void this.native.then((native) => native?.close());
	}
}

export class LixTransaction {
	constructor(private readonly native: NativeLixTransaction) {}

	async execute(sql: string, params: SqlParam[] = []): Promise<ExecuteResult> {
		assertExecuteArgs("lixTransaction", sql, params);
		return wrapExecuteResult(
			await this.native.execute(
				sql,
				params.map((param, index) =>
					toNativeValue(normalizeParam(param, index)),
				),
			),
		);
	}

	async commit(): Promise<void> {
		return this.native.commit();
	}

	async rollback(): Promise<void> {
		return this.native.rollback();
	}
}

function assertExecuteArgs(receiver: string, sql: string, params: SqlParam[]) {
	assertSqlArgs("execute", receiver, sql, params);
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
