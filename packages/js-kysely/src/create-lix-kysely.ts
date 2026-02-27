import {
	Kysely,
	SqliteAdapter,
	SqliteIntrospector,
	SqliteQueryCompiler,
	type CompiledQuery,
	type DatabaseConnection,
	type Driver,
	type QueryCompiler,
	type QueryResult,
} from "kysely";
import type { LixDatabaseSchema } from "./schema.js";

type LixValue = {
	kind?: string;
	value?: unknown;
};

type LixQueryResult = {
	rows?: unknown;
	columns?: unknown;
};

export type LixExecuteOptions = {
	writerKey?: string | null;
};

type LixExecuteLike = {
	execute(
		sql: string,
		params?: ReadonlyArray<unknown>,
		options?: LixExecuteOptions,
	): Promise<LixQueryResult>;
};

type LixSqlTransactionLike = {
	execute(sql: string, params?: ReadonlyArray<unknown>): Promise<LixQueryResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

type LixBeginTransactionLike = {
	beginTransaction(options?: LixExecuteOptions): Promise<LixSqlTransactionLike>;
};

type LixDbLike = {
	db: unknown;
};

type LixLike = LixExecuteLike | LixDbLike | (LixExecuteLike & LixBeginTransactionLike);
export type CreateLixKyselyOptions = {
	writerKey?: string | null;
};

class LixConnection implements DatabaseConnection {
	readonly #executeSql: (
		sql: string,
		params?: ReadonlyArray<unknown>,
	) => Promise<LixQueryResult>;

	constructor(
		executeSql: (
			sql: string,
			params?: ReadonlyArray<unknown>,
		) => Promise<LixQueryResult>,
	) {
		this.#executeSql = executeSql;
	}

	async executeQuery<R>(compiledQuery: CompiledQuery): Promise<QueryResult<R>> {
		const raw = await this.#executeSql(compiledQuery.sql, compiledQuery.parameters);
		const decodedRows = decodeRows(raw.rows);
		const columnNames =
			decodeColumnNames(raw.columns) ??
			(await this.resolveColumnNames(compiledQuery.query));
		const rows =
			columnNames &&
			decodedRows.every((row) => row.length === columnNames.length)
				? decodedRows.map((row) => rowToObject(row, columnNames))
				: decodedRows;

		const kind =
			compiledQuery.query && typeof compiledQuery.query === "object"
				? (compiledQuery.query as { kind?: unknown }).kind
				: undefined;

		let numAffectedRows: bigint | undefined;
		let insertId: bigint | undefined;
		if (kind !== "SelectQueryNode") {
			numAffectedRows = await this.readIntegerResult("SELECT changes()");
			if (kind === "InsertQueryNode") {
				insertId = await this.readIntegerResult("SELECT last_insert_rowid()");
			}
		}

		return {
			rows: rows as R[],
			numAffectedRows,
			insertId,
		};
	}

	async *streamQuery<R>(
		compiledQuery: CompiledQuery,
	): AsyncIterableIterator<QueryResult<R>> {
		yield await this.executeQuery(compiledQuery);
	}

	async readIntegerResult(sql: string): Promise<bigint | undefined> {
		const raw = await this.#executeSql(sql, undefined);
		const rows = decodeRows(raw.rows);
		if (!rows[0] || rows[0].length === 0) {
			return undefined;
		}
		const value = rows[0][0];
		if (typeof value === "number" && Number.isInteger(value)) {
			return BigInt(value);
		}
		if (typeof value === "bigint") {
			return value;
		}
		if (typeof value === "string" && /^-?\d+$/.test(value)) {
			return BigInt(value);
		}
		return undefined;
	}

	async resolveColumnNames(queryNode: unknown): Promise<string[] | undefined> {
		if (!queryNode || typeof queryNode !== "object") {
			return undefined;
		}

		const query = queryNode as Record<string, unknown>;
		const kind = typeof query.kind === "string" ? query.kind : "";

		if (kind === "SelectQueryNode") {
			const selections = selectSelectionNodes(query);
			if (selections.length > 0) {
				return selections.map(selectionNameFromNode);
			}
			return undefined;
		}

		if (
			kind === "InsertQueryNode" ||
			kind === "UpdateQueryNode" ||
			kind === "DeleteQueryNode"
		) {
			const returning = query.returning;
			if (returning && typeof returning === "object") {
				const selections = selectSelectionNodes(
					returning as Record<string, unknown>,
				);
				if (selections.length > 0) {
					return selections.map(selectionNameFromNode);
				}
			}
		}

		return undefined;
	}
}

class LixDriver implements Driver {
	readonly #lix: LixExecuteLike;
	readonly #lixWithTransactions?: LixExecuteLike & LixBeginTransactionLike;
	readonly #connection: LixConnection;
	readonly #options?: LixExecuteOptions;
	#activeTransaction?: LixSqlTransactionLike;

	constructor(lix: LixExecuteLike, options?: LixExecuteOptions) {
		this.#lix = lix;
		this.#options = options;
		this.#lixWithTransactions = isLixBeginTransactionLike(lix)
			? lix
			: undefined;
		this.#connection = new LixConnection((sql, params) =>
			this.#executeSql(sql, params),
		);
	}

	async init(): Promise<void> {}

	async acquireConnection(): Promise<DatabaseConnection> {
		return this.#connection;
	}

	async beginTransaction(): Promise<void> {
		if (this.#lixWithTransactions) {
			this.#activeTransaction = await this.#lixWithTransactions.beginTransaction(
				this.#options,
			);
			return;
		}
		await this.#lix.execute("BEGIN", undefined, this.#options);
	}

	async commitTransaction(): Promise<void> {
		if (this.#activeTransaction) {
			try {
				await this.#activeTransaction.commit();
			} finally {
				this.#activeTransaction = undefined;
			}
			return;
		}
		await this.#lix.execute("COMMIT", undefined, this.#options);
	}

	async rollbackTransaction(): Promise<void> {
		if (this.#activeTransaction) {
			try {
				await this.#activeTransaction.rollback();
			} finally {
				this.#activeTransaction = undefined;
			}
			return;
		}
		await this.#lix.execute("ROLLBACK", undefined, this.#options);
	}

	async savepoint(
		_connection: DatabaseConnection,
		savepointName: string,
		_compileQuery: QueryCompiler["compileQuery"],
	): Promise<void> {
		await this.#executeSql(`SAVEPOINT ${quoteIdentifier(savepointName)}`, undefined);
	}

	async rollbackToSavepoint(
		_connection: DatabaseConnection,
		savepointName: string,
		_compileQuery: QueryCompiler["compileQuery"],
	): Promise<void> {
		await this.#executeSql(
			`ROLLBACK TO SAVEPOINT ${quoteIdentifier(savepointName)}`,
			undefined,
		);
	}

	async releaseSavepoint(
		_connection: DatabaseConnection,
		savepointName: string,
		_compileQuery: QueryCompiler["compileQuery"],
	): Promise<void> {
		await this.#executeSql(`RELEASE SAVEPOINT ${quoteIdentifier(savepointName)}`, undefined);
	}

	async releaseConnection(): Promise<void> {}

	async destroy(): Promise<void> {}

	async #executeSql(
		sql: string,
		params?: ReadonlyArray<unknown>,
	): Promise<LixQueryResult> {
		if (this.#activeTransaction) {
			return this.#activeTransaction.execute(sql, params ?? []);
		}
		return this.#lix.execute(sql, params, this.#options);
	}
}

const cache = new WeakMap<object, Map<string, Kysely<LixDatabaseSchema>>>();

export function createLixKysely(
	lix: LixLike,
	options: CreateLixKyselyOptions = {},
): Kysely<LixDatabaseSchema> {
	const writerKey = normalizeWriterKey(options.writerKey);
	const cacheKey = writerKeyCacheKey(writerKey);
	if (isLixDbLike(lix)) {
		if (writerKey !== undefined) {
			throw new TypeError(
				"createLixKysely writerKey option requires lix.execute(sql, params, options)",
			);
		}
		return lix.db as Kysely<LixDatabaseSchema>;
	}
	if (!isLixExecuteLike(lix)) {
		throw new TypeError(
			"createLixKysely requires either lix.execute(sql, params) or lix.db",
		);
	}

	const entry = cache.get(lix as object);
	const cached = entry?.get(cacheKey);
	if (cached) {
		return cached;
	}

	const dialect = {
		createAdapter: () => new SqliteAdapter(),
		createDriver: () => new LixDriver(lix, { writerKey }),
		createIntrospector: (db: Kysely<any>) => new SqliteIntrospector(db),
		createQueryCompiler: () => new SqliteQueryCompiler(),
	};

	const db = new Kysely<LixDatabaseSchema>({ dialect });
	if (entry) {
		entry.set(cacheKey, db);
	} else {
		cache.set(lix as object, new Map([[cacheKey, db]]));
	}
	return db;
}

function isLixExecuteLike(value: unknown): value is LixExecuteLike {
	if (!value || typeof value !== "object") {
		return false;
	}
	return typeof (value as { execute?: unknown }).execute === "function";
}

function isLixBeginTransactionLike(
	value: unknown,
): value is LixExecuteLike & LixBeginTransactionLike {
	if (!isLixExecuteLike(value)) {
		return false;
	}
	return (
		typeof (value as { beginTransaction?: unknown }).beginTransaction === "function"
	);
}

function normalizeWriterKey(value: unknown): string | null | undefined {
	if (value === undefined) {
		return undefined;
	}
	if (value === null) {
		return null;
	}
	if (typeof value === "string") {
		return value;
	}
	throw new TypeError("createLixKysely writerKey must be a string or null");
}

function writerKeyCacheKey(writerKey: string | null | undefined): string {
	if (writerKey === undefined) {
		return "__default__";
	}
	if (writerKey === null) {
		return "__null__";
	}
	return `writer:${writerKey}`;
}

function isLixDbLike(value: unknown): value is LixDbLike {
	if (!value || typeof value !== "object") {
		return false;
	}
	return (
		"db" in (value as object) &&
		Boolean((value as { db?: unknown }).db) &&
		typeof (value as { db?: unknown }).db === "object"
	);
}

function decodeRows(rawRows: unknown): unknown[][] {
	if (!Array.isArray(rawRows)) {
		return [];
	}
	return rawRows.map((row) => {
		if (!Array.isArray(row)) {
			return [];
		}
		return row.map((value) => decodeValue(value));
	});
}

function decodeValue(value: unknown): unknown {
	if (!value || typeof value !== "object") {
		return value;
	}

	const raw = value as LixValue;
	if (typeof raw.kind === "string" && "value" in raw) {
		return raw.value;
	}
	return value;
}

function decodeColumnNames(rawColumns: unknown): string[] | undefined {
	if (!Array.isArray(rawColumns)) {
		return undefined;
	}

	const names = rawColumns.filter(
		(value): value is string => typeof value === "string",
	);

	return names.length > 0 ? names : undefined;
}

function rowToObject(
	row: unknown[],
	columns: string[],
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	for (let i = 0; i < columns.length; i++) {
		const column = columns[i];
		if (!column) {
			continue;
		}
		out[column] = row[i];
	}
	return out;
}

function selectSelectionNodes(
	node: Record<string, unknown>,
): Record<string, unknown>[] {
	const selections = node.selections;
	if (!Array.isArray(selections)) {
		return [];
	}
	return selections.filter(
		(selection): selection is Record<string, unknown> =>
			Boolean(selection) && typeof selection === "object",
	);
}

function selectTableNames(node: Record<string, unknown>): string[] {
	const from = node.from;
	if (!from || typeof from !== "object") {
		return [];
	}
	const froms = (from as Record<string, unknown>).froms;
	if (!Array.isArray(froms)) {
		return [];
	}
	const names: string[] = [];

	for (const fromNode of froms) {
		if (!fromNode || typeof fromNode !== "object") {
			continue;
		}
		const table = (fromNode as Record<string, unknown>).table;
		const name = identifierNameFromTableNode(table);
		if (name) {
			names.push(name);
		}
	}

	return names;
}

function selectionNameFromNode(selectionNode: Record<string, unknown>): string {
	const selection = selectionNode.selection;
	if (!selection || typeof selection !== "object") {
		return "column";
	}
	return (
		identifierNameFromSelection(selection as Record<string, unknown>) ??
		"column"
	);
}

function identifierNameFromSelection(
	node: Record<string, unknown>,
): string | undefined {
	const kind = typeof node.kind === "string" ? node.kind : "";
	if (kind === "AliasNode") {
		const alias = node.alias;
		const aliasName = identifierName(alias);
		if (aliasName) return aliasName;
	}

	if (kind === "ReferenceNode") {
		const column = node.column;
		if (!column || typeof column !== "object") {
			return undefined;
		}
		const nested = (column as Record<string, unknown>).column;
		const name = identifierName(nested);
		if (name) return name;
	}

	if (kind === "ColumnNode") {
		const name = identifierName(node.column);
		if (name) return name;
	}

	if (kind === "IdentifierNode") {
		const name = identifierName(node);
		if (name) return name;
	}

	return undefined;
}

function identifierNameFromTableNode(node: unknown): string | undefined {
	if (!node || typeof node !== "object") {
		return undefined;
	}
	const tableNode = node as Record<string, unknown>;
	if (tableNode.kind === "SchemableIdentifierNode") {
		return identifierName(tableNode.identifier);
	}
	return undefined;
}

function identifierName(node: unknown): string | undefined {
	if (!node || typeof node !== "object") {
		return undefined;
	}
	const name = (node as Record<string, unknown>).name;
	return typeof name === "string" ? name : undefined;
}

function quoteIdentifier(value: string): string {
	return `"${value.replaceAll('"', '""')}"`;
}
