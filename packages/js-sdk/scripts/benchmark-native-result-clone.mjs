import { performance } from "node:perf_hooks";
import { wrapExecuteResult } from "../dist/result.js";

const rowCount = Number(process.env.ROWS ?? 20_000);
const iterations = Number(process.env.ITERATIONS ?? 5);
const mode = process.env.MODE ?? "optimized";
const payload = {
	declarations: [
		{ type: "input-variable", name: "username" },
		{ type: "local-variable", name: "greeting", value: "Hello" },
	],
	selectors: [{ type: "variable-reference", name: "username" }],
	matches: [{ type: "select", key: "username", value: "samuel" }],
	pattern: [
		{ type: "text", value: "Hello " },
		{ type: "expression", arg: { type: "variable-reference", name: "username" } },
	],
};

const columns = ["declarations", "selectors", "matches", "pattern"];
const rows = Array.from({ length: rowCount }, () => [
	{ kind: "jsonb", value: payload.declarations },
	{ kind: "jsonb", value: payload.selectors },
	{ kind: "jsonb", value: payload.matches },
	{ kind: "jsonb", value: payload.pattern },
]);

function cloneJsonValue(value) {
	if (Array.isArray(value)) return value.map(cloneJsonValue);
	if (value && typeof value === "object") {
		return Object.fromEntries(
			Object.entries(value).map(([key, entry]) => [key, cloneJsonValue(entry)])
		);
	}
	return value;
}

function nativeRows() {
	if (mode !== "legacy") return rows;
	return rows.map((row) =>
		row.map((value) =>
			value.kind === "jsonb"
				? { kind: "jsonb", value: cloneJsonValue(value.value) }
				: value
		)
	);
}

function run() {
	const started = performance.now();
	let checksum = 0;
	for (let iteration = 0; iteration < iterations; iteration += 1) {
		const result = wrapExecuteResult({
			columns,
			rows: nativeRows(),
			rowsAffected: 0,
			notices: [],
		});
		for (const row of result.rows) {
			checksum += row.toObject().pattern.length;
		}
	}
	return {
		mode,
		rowCount,
		iterations,
		meanMs: (performance.now() - started) / iterations,
		checksum,
	};
}

console.log(JSON.stringify(run()));
