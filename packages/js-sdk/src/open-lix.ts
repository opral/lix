import { invalidArgument } from "./errors.js";
import { pluginArchivePathFromArchive } from "./plugin-archive.js";
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
	LixFs,
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
	readonly fs: LixFs;

	constructor(private readonly native: NativeLix) {
		this.fs = createFsApi("lix", (sql, params) => this.execute(sql, params));
	}

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

	async installPlugin(archiveBytes: Uint8Array): Promise<void> {
		assertBytesArg("lix", "installPlugin", "archiveBytes", archiveBytes);
		await this.fs.writeFile(
			pluginArchivePathFromArchive(archiveBytes),
			archiveBytes,
		);
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
	readonly fs: LixFs;

	constructor(private readonly native: NativeLixTransaction) {
		this.fs = createFsApi("lixTransaction", (sql, params) =>
			this.execute(sql, params),
		);
	}

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

function createFsApi(
	receiver: string,
	executeSql: (sql: string, params: SqlParam[]) => Promise<ExecuteResult>,
): LixFs {
	return {
		async readFile(path: string): Promise<Uint8Array | undefined> {
			assertPathArg(receiver, "fs.readFile", path);
			const result = await executeSql(
				"SELECT data FROM lix_file WHERE path = $1",
				[path],
			);
			if (result.rows.length === 0) {
				return undefined;
			}
			return result.rows[0]?.value("data").asBytes() ?? new Uint8Array();
		},
		async writeFile(path: string, data: Uint8Array): Promise<void> {
			assertPathArg(receiver, "fs.writeFile", path);
			assertBytesArg(receiver, "fs.writeFile", "data", data);
			await executeSql(
				"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
				[path, data],
			);
		},
	};
}

function assertPathArg(
	receiver: string,
	operation: string,
	path: unknown,
): void {
	if (typeof path !== "string" || path.length === 0) {
		throw invalidArgument(
			operation,
			"path",
			"non-empty string",
			typeof path,
			receiver,
		);
	}
}

function assertBytesArg(
	receiver: string,
	operation: string,
	argument: string,
	value: unknown,
): asserts value is Uint8Array {
	if (!(value instanceof Uint8Array)) {
		throw invalidArgument(
			operation,
			argument,
			"Uint8Array",
			value === null ? "null" : typeof value,
			receiver,
		);
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
