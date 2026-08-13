import { expect, test, vi } from "vitest";
import {
	decodeExecuteBatchResult,
	decodeExecuteResult,
	decodeHandshake,
	decodeObserveEvent,
	encodeWireValue,
} from "./server-protocol.js";

test("remote executeBatch requires positional metadata and preserves labels", () => {
	const result = decodeExecuteBatchResult({
		statementIndex: 7,
		label: "same-label",
		columns: [],
		rows: [],
		rowsAffected: 1,
		notices: [],
	});
	expect(result.statementIndex).toBe(7);
	expect(result.label).toBe("same-label");
	expect(() =>
		decodeExecuteBatchResult({
			columns: [],
			rows: [],
			rowsAffected: 1,
			notices: [],
		}),
	).toThrow("statementIndex must be a non-negative safe integer");
});

test("remote blobs use native typed-array base64 when available", () => {
	const prototype = Uint8Array.prototype as Uint8Array & {
		toBase64?: () => string;
	};
	const constructor = Uint8Array as Uint8ArrayConstructor & {
		fromBase64?: (value: string) => Uint8Array;
	};
	const originalToBase64 = Object.getOwnPropertyDescriptor(
		prototype,
		"toBase64",
	);
	const originalFromBase64 = Object.getOwnPropertyDescriptor(
		constructor,
		"fromBase64",
	);
	const toBase64 = vi.fn(() => "native-encoded");
	const fromBase64 = vi.fn(() => new Uint8Array([4, 5, 6]));

	try {
		Object.defineProperty(prototype, "toBase64", {
			configurable: true,
			value: toBase64,
		});
		Object.defineProperty(constructor, "fromBase64", {
			configurable: true,
			value: fromBase64,
		});

		const bytes = new Uint8Array([1, 2, 3]);
		expect(encodeWireValue({ kind: "blob", value: null, blob: bytes })).toEqual({
			kind: "blob",
			base64: "native-encoded",
		});
		expect(toBase64).toHaveBeenCalledOnce();
		expect(toBase64.mock.contexts[0]).toBe(bytes);

		const decoded = decodeExecuteResult({
			columns: ["content"],
			rows: [[{ kind: "blob", base64: "native-input" }]],
			rowsAffected: 0,
			notices: [],
		});
		expect(decoded.rows[0]?.[0]).toEqual({
			kind: "blob",
			value: null,
			blob: new Uint8Array([4, 5, 6]),
		});
		expect(fromBase64).toHaveBeenCalledWith("native-input");

		fromBase64.mockImplementationOnce(() => {
			throw new SyntaxError("invalid base64");
		});
		expect(() =>
			decodeExecuteResult({
				columns: ["content"],
				rows: [[{ kind: "blob", base64: "%%%" }]],
				rowsAffected: 0,
				notices: [],
			}),
		).toThrow(
			expect.objectContaining({
				code: "LIX_SERVER_PROTOCOL_ERROR",
				message: "blob wire value contains invalid base64",
			}),
		);
	} finally {
		restoreProperty(prototype, "toBase64", originalToBase64);
		restoreProperty(constructor, "fromBase64", originalFromBase64);
	}
});

test("remote blob base64 falls back on runtimes without native support", () => {
	const prototype = Uint8Array.prototype as Uint8Array & {
		toBase64?: () => string;
	};
	const constructor = Uint8Array as Uint8ArrayConstructor & {
		fromBase64?: (value: string) => Uint8Array;
	};
	const originalToBase64 = Object.getOwnPropertyDescriptor(
		prototype,
		"toBase64",
	);
	const originalFromBase64 = Object.getOwnPropertyDescriptor(
		constructor,
		"fromBase64",
	);

	try {
		Reflect.deleteProperty(prototype, "toBase64");
		Reflect.deleteProperty(constructor, "fromBase64");
		expect(
			encodeWireValue({
				kind: "blob",
				value: null,
				blob: new Uint8Array([1, 2, 3]),
			}),
		).toEqual({ kind: "blob", base64: "AQID" });
		const decoded = decodeExecuteResult({
			columns: ["content"],
			rows: [[{ kind: "blob", base64: "BAUG" }]],
			rowsAffected: 0,
			notices: [],
		});
		expect(decoded.rows[0]?.[0]).toEqual({
			kind: "blob",
			value: null,
			blob: new Uint8Array([4, 5, 6]),
		});
	} finally {
		restoreProperty(prototype, "toBase64", originalToBase64);
		restoreProperty(constructor, "fromBase64", originalFromBase64);
	}
});

function restoreProperty(
	target: object,
	key: PropertyKey,
	descriptor: PropertyDescriptor | undefined,
): void {
	if (descriptor) {
		Object.defineProperty(target, key, descriptor);
	} else {
		Reflect.deleteProperty(target, key);
	}
}
test("observe blob deltas fail closed without an exact non-overlapping base", () => {
	const full = {
		sequence: 0,
		mutationSequence: 0,
		result: {
			columns: ["content"],
			rows: [[{ kind: "blob", base64: "YWJjZGVm" }]],
			rowsAffected: 0,
			notices: [],
		},
	};
	const base = decodeObserveEvent(full);
	const delta = {
		sequence: 1,
		mutationSequence: 1,
		delta: {
			kind: "single-blob-splice",
			baseSequence: 0,
			prefixBytes: 2,
			suffixBytes: 2,
			insertBase64: "WA==",
		},
	};
	expect(decodeObserveEvent(delta, base).rows.rows[0]?.[0]).toEqual({
		kind: "blob",
		value: null,
		blob: new TextEncoder().encode("abXef"),
	});
	expect(() => decodeObserveEvent(delta)).toThrow(
		"observe blob delta does not match its transport base",
	);
	expect(() =>
		decodeObserveEvent(
			{
				...delta,
				delta: { ...delta.delta, prefixBytes: 5, suffixBytes: 2 },
			},
			base,
		),
	).toThrow("observe blob delta prefix and suffix overlap");
	expect(() => decodeObserveEvent({ ...delta, result: full.result }, base)).toThrow(
		"observe event requires exactly one of result or delta",
	);
});

test("observe row deltas splice replacement insertion and deletion", () => {
	const full = {
		sequence: 0,
		mutationSequence: 0,
		result: {
			columns: ["value"],
			rows: [
				[{ kind: "text", value: "a" }],
				[{ kind: "text", value: "b" }],
				[{ kind: "text", value: "c" }],
			],
			rowsAffected: 0,
			notices: [],
		},
	};
	const base = decodeObserveEvent(full);
	const replacement = decodeObserveEvent(
		{
			sequence: 1,
			mutationSequence: 1,
			delta: {
				kind: "row-splice",
				baseSequence: 0,
				prefixRows: 1,
				deleteRows: 1,
				insertRows: [[{ kind: "text", value: "x" }]],
			},
		},
		base,
	);
	expect(replacement.rows.rows.map((row) => row[0]?.value)).toEqual([
		"a",
		"x",
		"c",
	]);
	const inserted = decodeObserveEvent(
		{
			sequence: 2,
			mutationSequence: 2,
			delta: {
				kind: "row-splice",
				baseSequence: 1,
				prefixRows: 2,
				deleteRows: 0,
				insertRows: [[{ kind: "text", value: "y" }]],
			},
		},
		replacement,
	);
	expect(inserted.rows.rows.map((row) => row[0]?.value)).toEqual([
		"a",
		"x",
		"y",
		"c",
	]);
	const deleted = decodeObserveEvent(
		{
			sequence: 3,
			mutationSequence: 3,
			delta: {
				kind: "row-splice",
				baseSequence: 2,
				prefixRows: 1,
				deleteRows: 2,
				insertRows: [],
			},
		},
		inserted,
	);
	expect(deleted.rows.rows.map((row) => row[0]?.value)).toEqual(["a", "c"]);
});

test("observe row deltas reject invalid bases ranges and row shapes", () => {
	const base = decodeObserveEvent({
		sequence: 0,
		mutationSequence: 0,
		result: {
			columns: ["value"],
			rows: [[{ kind: "text", value: "a" }]],
			rowsAffected: 0,
			notices: [],
		},
	});
	const delta = {
		sequence: 1,
		mutationSequence: 1,
		delta: {
			kind: "row-splice",
			baseSequence: 0,
			prefixRows: 0,
			deleteRows: 1,
			insertRows: [[{ kind: "text", value: "x" }]],
		},
	};
	expect(() => decodeObserveEvent(delta)).toThrow(
		"observe row delta does not match its transport base",
	);
	expect(() =>
		decodeObserveEvent(
			{
				...delta,
				delta: { ...delta.delta, prefixRows: 2 },
			},
			base,
		),
	).toThrow("observe row delta splice range is outside its transport base");
	expect(() =>
		decodeObserveEvent(
			{
				...delta,
				delta: { ...delta.delta, insertRows: [[], []] },
			},
			base,
		),
	).toThrow("observe row delta insert row 0 has 0 values for 1 columns");
});
