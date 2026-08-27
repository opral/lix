import { fromNativeValue, type NativeLixValue, Value } from "./value.js";
import type {
	ExecuteBatchResult,
	ExecuteResult,
	ResultArrayRow,
	ResultObjectRow,
	ResultRow,
} from "./types.js";

type NativeExecuteResult = Omit<ExecuteResult, "rows"> & {
	rows: NativeLixValue[][];
};

export function wrapExecuteResult(result: NativeExecuteResult): ExecuteResult;
export function wrapExecuteResult(
	result: NativeExecuteResult,
	rowMode: "object",
): ExecuteResult<ResultObjectRow>;
export function wrapExecuteResult(
	result: NativeExecuteResult,
	rowMode: "array",
): ExecuteResult<ResultArrayRow>;
export function wrapExecuteResult(
	result: NativeExecuteResult,
	rowMode: "object" | "array",
): ExecuteResult<ResultRow>;
export function wrapExecuteResult(
	result: NativeExecuteResult,
	rowMode: "object" | "array" = "object",
): ExecuteResult<ResultRow> {
	return {
		...result,
		rows: result.rows.map((row) => {
			const values = row.map((value) =>
				Value._fromNative(fromNativeValue(value)).toJS(),
			);
			if (rowMode === "array") return values as ResultArrayRow;
			return Object.fromEntries(
				result.columns.map((column, index) => [column.name, values[index]]),
			) as ResultObjectRow;
		}),
	};
}

export function wrapExecuteBatchResult(
	result: NativeExecuteResult,
	rowMode: "object" | "array" = "object",
): ExecuteBatchResult<ResultRow> {
	const statementIndex = result.statementIndex;
	if (
		typeof statementIndex !== "number" ||
		!Number.isSafeInteger(statementIndex) ||
		statementIndex < 0
	) {
		throw new Error("executeBatch result is missing a valid statementIndex");
	}
	return {
		...wrapExecuteResult(result, rowMode),
		statementIndex,
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
