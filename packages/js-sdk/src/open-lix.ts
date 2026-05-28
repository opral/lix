import { invalidArgument, withLixError } from "./errors.js";
import { addon } from "./native.js";
import { normalizeOptionals, wrapExecuteResult } from "./result.js";
import { normalizeParam } from "./value.js";
import type {
	CreateBranchOptions,
	CreateBranchReceipt,
	ExecuteResult,
	MergeBranchOptions,
	MergeBranchPreview,
	MergeBranchReceipt,
	OpenLixOptions,
	SqlParam,
	SqliteBackendOptions,
	SwitchBranchOptions,
	SwitchBranchReceipt,
} from "./types.js";

type NativeExecuteResult = Parameters<typeof wrapExecuteResult>[0];
type NativeParam = ReturnType<typeof normalizeParam>;

type NativeLix = {
	execute(sql: string, params: NativeParam[]): NativeExecuteResult;
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

export class SqliteBackend {
	readonly path: string;

	constructor(options: SqliteBackendOptions) {
		if (!options || typeof options.path !== "string" || options.path.length === 0) {
			throw new TypeError("SqliteBackend requires a non-empty path");
		}
		this.path = options.path;
	}
}

export async function openLix(options: Partial<OpenLixOptions> = {}): Promise<Lix> {
	if (!(options.backend instanceof SqliteBackend)) {
		throw new TypeError("openLix() requires { backend: new SqliteBackend({ path }) }");
	}
	return new Lix(addon.Lix.openSqlite(options.backend.path));
}

export class Lix {
	constructor(private readonly native: NativeLix) {}

	async execute(sql: string, params: SqlParam[] = []): Promise<ExecuteResult> {
		assertExecuteArgs("lix", sql, params);
		return withLixError(() =>
			wrapExecuteResult(
				this.native.execute(
					sql,
					params.map((param, index) => normalizeParam(param, index)),
				),
			),
		);
	}

	async beginTransaction(): Promise<LixTransaction> {
		return withLixError(() => new LixTransaction(this.native.beginTransaction()));
	}

	async activeBranchId(): Promise<string> {
		return withLixError(() => this.native.activeBranchId());
	}

	async createBranch(options: CreateBranchOptions): Promise<CreateBranchReceipt> {
		return withLixError(() => this.native.createBranch(options));
	}

	async switchBranch(options: SwitchBranchOptions): Promise<SwitchBranchReceipt> {
		return withLixError(() => this.native.switchBranch(options));
	}

	async mergeBranchPreview(options: MergeBranchOptions): Promise<MergeBranchPreview> {
		return withLixError(() =>
			normalizeOptionals(this.native.mergeBranchPreview(options)),
		);
	}

	async mergeBranch(options: MergeBranchOptions): Promise<MergeBranchReceipt> {
		return withLixError(() => {
			const receipt = normalizeOptionals(this.native.mergeBranch(options));
			receipt.createdMergeCommitId ??= null;
			return receipt;
		});
	}

	async close(): Promise<void> {
		return withLixError(() => this.native.close());
	}
}

export class LixTransaction {
	constructor(private readonly native: NativeLixTransaction) {}

	async execute(sql: string, params: SqlParam[] = []): Promise<ExecuteResult> {
		assertExecuteArgs("lixTransaction", sql, params);
		return withLixError(() =>
			wrapExecuteResult(
				this.native.execute(
					sql,
					params.map((param, index) => normalizeParam(param, index)),
				),
			),
		);
	}

	async commit(): Promise<void> {
		return withLixError(() => this.native.commit());
	}

	async rollback(): Promise<void> {
		return withLixError(() => this.native.rollback());
	}
}

function assertExecuteArgs(
	receiver: string,
	sql: string,
	params: SqlParam[],
) {
	if (typeof sql !== "string") {
		throw invalidArgument("execute", "sql", "string", typeof sql, receiver);
	}
	if (!Array.isArray(params)) {
		throw invalidArgument("execute", "params", "array", typeof params, receiver);
	}
}
