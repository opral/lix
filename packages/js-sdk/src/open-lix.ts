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
	execute(sql: string, params: NativeParam[]): NativeExecuteResult;
	observe(sql: string, params: NativeParam[]): NativeObserveEvents;
	beginTransaction(): NativeLixTransaction;
	activeBranchId(): string;
	createBranch(options: CreateBranchOptions): CreateBranchReceipt;
	switchBranch(options: SwitchBranchOptions): SwitchBranchReceipt;
	mergeBranchPreview(options: MergeBranchOptions): MergeBranchPreview;
	mergeBranch(options: MergeBranchOptions): MergeBranchReceipt;
	close(): void;
};

type NativeLixTransaction = {
	execute(sql: string, params: NativeParam[]): NativeExecuteResult;
	commit(): void;
	rollback(): void;
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

	constructor(options: FsBackendOptions) {
		if (
			!options ||
			typeof options.path !== "string" ||
			options.path.length === 0
		) {
			throw new TypeError("FsBackend requires a non-empty path");
		}
		this.path = options.path;
	}
}

export async function openLix(options: OpenLixOptions = {}): Promise<Lix> {
	if (!options || typeof options !== "object") {
		throw new TypeError("openLix() options must be an object");
	}
	if (options.backend === undefined) {
		return new Lix(addon.Lix.openMemory());
	}
	if (options.backend instanceof SqliteBackend) {
		return new Lix(addon.Lix.openSqlite(options.backend.path));
	}
	if (options.backend instanceof FsBackend) {
		return new Lix(addon.Lix.openFs(options.backend.path));
	}
	throw new TypeError(
		"openLix() requires { backend: new SqliteBackend({ path }) } or { backend: new FsBackend({ path }) }",
	);
}

export class Lix {
	constructor(private readonly native: NativeLix) {}

	async execute(sql: string, params: SqlParam[] = []): Promise<ExecuteResult> {
		assertExecuteArgs("lix", sql, params);
		return wrapExecuteResult(
			this.native.execute(
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
		return new LixTransaction(this.native.beginTransaction());
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
		return normalizeOptionals(this.native.mergeBranchPreview(options));
	}

	async mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt> {
		const receipt = normalizeOptionals(this.native.mergeBranch(options));
		receipt.createdMergeCommitId ??= null;
		return receipt;
	}

	async close(): Promise<void> {
		return this.native.close();
	}
}

export class ObserveEvents {
	constructor(private readonly native: NativeObserveEvents) {}

	async next(): Promise<ObserveEvent | undefined> {
		const event = await this.native.next();
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
		this.native.close();
	}
}

export class LixTransaction {
	constructor(private readonly native: NativeLixTransaction) {}

	async execute(sql: string, params: SqlParam[] = []): Promise<ExecuteResult> {
		assertExecuteArgs("lixTransaction", sql, params);
		return wrapExecuteResult(
			this.native.execute(
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
