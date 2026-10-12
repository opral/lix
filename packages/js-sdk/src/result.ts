import { fromNativeValue, type NativeLixValue, Value } from "./value.js";
import type { CommitSpan, ExecuteResult, ResultColumn, Row } from "./types.js";

type NativeExecuteResult = Omit<ExecuteResult, "rows" | "commit"> & {
	commit?: CommitSpan | null;
	rows: NativeLixValue[][];
};

function ambiguousColumn(column: string): Error {
	const error = new Error(
		`the result has more than one column named '${column}'`,
	) as Error & { code: string; hint: string };
	error.name = "LixError";
	error.code = "LIX_AMBIGUOUS_COLUMN";
	error.hint =
		"Alias the columns so every name is unique, e.g. SELECT a.id AS a_id, b.id AS b_id.";
	return error;
}

function columnNames(columns: readonly (ResultColumn | string)[]): string[] {
	const names = columns.map((column) =>
		typeof column === "string" ? column : column.name,
	);
	const seen = new Set<string>();
	for (const name of names) {
		if (seen.has(name)) throw ambiguousColumn(name);
		seen.add(name);
	}
	return names;
}

/** Decodes native rows into objects keyed by column name. Duplicate column
 * names throw `LIX_AMBIGUOUS_COLUMN`, since an object can hold only one. */
export function rowsOf(result: {
	columns: readonly (ResultColumn | string)[];
	rows: NativeLixValue[][];
}): Row[] {
	const names = columnNames(result.columns);
	// fromEntries defines own properties, so a column named `__proto__` stays data.
	return result.rows.map(
		(row) =>
			Object.fromEntries(
				names.map((name, index) => [
					name,
					Value._fromNative(fromNativeValue(row[index]!)).toJS(),
				]),
			) as Row,
	);
}

export function wrapExecuteResult(result: NativeExecuteResult): ExecuteResult {
	return {
		columns: result.columns,
		rows: rowsOf(result),
		rowsAffected: result.rowsAffected,
		notices: result.notices,
		commit: result.commit ?? null,
	};
}

export function normalizeOptionals<T>(value: T): T {
	if (Array.isArray(value)) return value.map(normalizeOptionals) as T;
	if (!value || typeof value !== "object") return value;
	return Object.fromEntries(
		Object.entries(value).map(([key, entry]) => [
			key,
			entry === undefined ? null : normalizeOptionals(entry),
		]),
	) as T;
}
