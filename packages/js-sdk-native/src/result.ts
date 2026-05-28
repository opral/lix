import { Value } from "./value.js";
import type { ExecuteResult, LixValue } from "./types.js";

export class Row {
	constructor(
		private readonly columns: string[],
		private readonly values: Value[],
	) {}

	static fromRaw(columns: string[], values: LixValue[]) {
		return new Row(
			columns,
			values.map((value) => new Value(value)),
		);
	}

	get(column: string): unknown {
		return this.value(column).asJson();
	}

	value(column: string): Value {
		const index = this.columns.indexOf(column);
		if (index === -1) {
			throw new Error(
				`Unknown column "${column}". Available columns: ${this.columns.join(", ")}`,
			);
		}
		const value = this.values[index];
		if (!value) {
			throw new Error(`Column "${column}" is missing a value`);
		}
		return value;
	}

	toObject(): Record<string, unknown> {
		return Object.fromEntries(
			this.columns.map((column, index) => [column, this.values[index]?.asJson()]),
		);
	}

	toValueMap(): Record<string, Value> {
		return Object.fromEntries(
			this.columns.map((column, index) => [column, this.values[index]]),
		);
	}
}

type NativeExecuteResult = Omit<ExecuteResult, "rows"> & {
	rows: LixValue[][];
};

export function wrapExecuteResult(result: NativeExecuteResult): ExecuteResult {
	return {
		...result,
		rows: result.rows.map((row) => Row.fromRaw(result.columns, row)),
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
