import { expect, test, vi } from "vitest";
import { decodeExecuteResult, encodeWireValue } from "./protocol.js";

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
			columns: ["data"],
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
				columns: ["data"],
				rows: [[{ kind: "blob", base64: "%%%" }]],
				rowsAffected: 0,
				notices: [],
			}),
		).toThrow(
			expect.objectContaining({
				code: "LIX_REMOTE_PROTOCOL_ERROR",
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
			columns: ["data"],
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
