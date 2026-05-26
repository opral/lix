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

type LixQueryResult = {
	rows?: unknown;
	columns?: unknown;
	statements?: unknown;
	rowsAffected?: unknown;
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

type LixTransactionLike = {
	execute(
		sql: string,
		params?: ReadonlyArray<unknown>,
	): Promise<LixQueryResult>;
	commit(): Promise<void>;
	rollback(): Promise<void>;
};

type LixTransactionalLike = LixExecuteLike & {
	beginTransaction(): Promise<LixTransactionLike>;
};

type LixDbLike = {
	db: unknown;
};

type LixLike = LixExecuteLike | LixDbLike;
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
		const raw = normalizeLixQueryResult(
			await this.#executeSql(compiledQuery.sql, compiledQuery.parameters),
		);
		const rawColumnNames = decodeColumnNames(raw.columns);
		const decodedRows = decodeRows(raw.rows, rawColumnNames);
		const columnNames =
			rawColumnNames ?? (await this.resolveColumnNames(compiledQuery.query));
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
			numAffectedRows = extractIntegerValue(raw.rowsAffected);
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
		const raw = normalizeLixQueryResult(await this.#executeSql(sql, undefined));
		const rows = decodeRows(raw.rows, decodeColumnNames(raw.columns));
		if (!rows[0] || rows[0].length === 0) {
			return undefined;
		}
		return extractIntegerValue(rows[0][0]);
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
	readonly #connection: LixConnection;
	readonly #options?: LixExecuteOptions;
	#transactionSlotHeld = false;
	#transaction: LixTransactionLike | undefined;
	#waiters: Array<() => void> = [];

	constructor(lix: LixExecuteLike, options?: LixExecuteOptions) {
		this.#lix = lix;
		this.#options = options;
		this.#connection = new LixConnection((sql, params) =>
			this.#executeSql(sql, params),
		);
	}

	async init(): Promise<void> {}

	async acquireConnection(): Promise<DatabaseConnection> {
		return this.#connection;
	}

	async beginTransaction(): Promise<void> {
		if (!isLixTransactionalLike(this.#lix)) {
			throw new Error("This Lix handle does not support transactions");
		}
		await this.#acquireTransactionSlot();
		try {
			this.#transaction = await this.#lix.beginTransaction();
		} catch (error) {
			this.#releaseTransactionSlot();
			throw error;
		}
	}

	async commitTransaction(): Promise<void> {
		if (!this.#transaction) {
			throw new Error("commitTransaction called without active transaction");
		}
		try {
			await this.#transaction.commit();
		} finally {
			this.#transaction = undefined;
			this.#releaseTransactionSlot();
		}
	}

	async rollbackTransaction(): Promise<void> {
		if (!this.#transaction) {
			throw new Error("rollbackTransaction called without active transaction");
		}
		try {
			await this.#transaction.rollback();
		} finally {
			this.#transaction = undefined;
			this.#releaseTransactionSlot();
		}
	}

	async savepoint(
		_connection: DatabaseConnection,
		_savepointName: string,
		_compileQuery: QueryCompiler["compileQuery"],
	): Promise<void> {
		throw new Error(
			"Nested transactions are not supported by createLixKysely() yet",
		);
	}

	async rollbackToSavepoint(
		_connection: DatabaseConnection,
		_savepointName: string,
		_compileQuery: QueryCompiler["compileQuery"],
	): Promise<void> {
		throw new Error(
			"Nested transactions are not supported by createLixKysely() yet",
		);
	}

	async releaseSavepoint(
		_connection: DatabaseConnection,
		_savepointName: string,
		_compileQuery: QueryCompiler["compileQuery"],
	): Promise<void> {
		throw new Error(
			"Nested transactions are not supported by createLixKysely() yet",
		);
	}

	async releaseConnection(): Promise<void> {}

	async destroy(): Promise<void> {}

	async #executeSql(
		sql: string,
		params?: ReadonlyArray<unknown>,
	): Promise<LixQueryResult> {
		if (this.#transaction) {
			return this.#transaction.execute(sql, params);
		}
		return this.#lix.execute(sql, params, this.#options);
	}

	async #acquireTransactionSlot(): Promise<void> {
		while (this.#transactionSlotHeld) {
			await new Promise<void>((resolve) => this.#waiters.push(resolve));
		}
		this.#transactionSlotHeld = true;
	}

	#releaseTransactionSlot(): void {
		this.#transactionSlotHeld = false;
		const waiter = this.#waiters.shift();
		if (waiter) {
			waiter();
		}
	}
}

class LixQueryCompiler extends SqliteQueryCompiler {
	protected override getLeftIdentifierWrapper(): string {
		return "";
	}

	protected override getRightIdentifierWrapper(): string {
		return "";
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
		createQueryCompiler: () => new LixQueryCompiler(),
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

function isLixTransactionalLike(value: unknown): value is LixTransactionalLike {
	if (!value || typeof value !== "object") {
		return false;
	}
	return (
		typeof (value as { execute?: unknown }).execute === "function" &&
		typeof (value as { beginTransaction?: unknown }).beginTransaction ===
			"function"
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

function decodeRows(rawRows: unknown, columns?: string[]): unknown[][] {
	if (!Array.isArray(rawRows)) {
		return [];
	}
	return rawRows.map((row) => {
		if (!Array.isArray(row)) {
			return decodeObjectRow(row, columns);
		}
		return row.map(decodeLixValue);
	});
}

function decodeObjectRow(row: unknown, columns?: string[]): unknown[] {
	if (!row || typeof row !== "object") {
		return [];
	}
	if (typeof (row as { values?: unknown }).values === "function") {
		const values = (row as { values: () => unknown }).values();
		return Array.isArray(values) ? values.map(decodeLixValue) : [];
	}
	const valuesByIndex = (row as { valuesByIndex?: unknown }).valuesByIndex;
	if (Array.isArray(valuesByIndex)) {
		return valuesByIndex.map(decodeLixValue);
	}
	if (typeof (row as { toObject?: unknown }).toObject === "function") {
		const object = (
			row as { toObject: () => Record<string, unknown> }
		).toObject();
		return columns?.map((column) => object[column]) ?? Object.values(object);
	}
	return [];
}

function decodeLixValue(value: unknown): unknown {
	if (!value || typeof value !== "object") {
		return value;
	}
	const candidate = value as {
		kind?: unknown;
		value?: unknown;
		base64?: unknown;
		asBlob?: unknown;
	};
	switch (candidate.kind) {
		case "null":
			return null;
		case "boolean":
		case "integer":
		case "real":
		case "text":
		case "json":
			return candidate.value;
		case "blob":
			if (typeof candidate.asBlob === "function") {
				return candidate.asBlob();
			}
			return typeof candidate.base64 === "string"
				? Uint8Array.from(atob(candidate.base64), (char) => char.charCodeAt(0))
				: undefined;
		default:
			return value;
	}
}

function normalizeLixQueryResult(raw: LixQueryResult): {
	rows?: unknown;
	columns?: unknown;
	rowsAffected?: unknown;
} {
	if (Array.isArray(raw.statements)) {
		const [statement] = raw.statements;
		if (statement && typeof statement === "object") {
			const candidate = statement as {
				rows?: unknown;
				columns?: unknown;
				rowsAffected?: unknown;
			};
			return {
				rows: candidate.rows,
				columns: candidate.columns,
				rowsAffected: candidate.rowsAffected,
			};
		}
	}
	return raw;
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

function extractIntegerValue(value: unknown): bigint | undefined {
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
