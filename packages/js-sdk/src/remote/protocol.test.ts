import { expect, test, vi } from "vitest";
import {
	decodeExecuteResult,
	decodeHandshake,
	decodeObserveEvent,
	encodeWireValue,
} from "./protocol.js";

test.each([
	[undefined, false],
	[{}, false],
	[{ requestBlobSplice: false }, false],
	[{ requestBlobSplice: "true" }, false],
	[{ requestBlobSplice: true }, true],
])(
	"remote handshake negotiates only the exact blob splice capability: %j",
	(capabilities, expected) => {
		expect(
			decodeHandshake({
				protocolVersion: 1,
				activeBranchId: "main-id",
				sessionId: "session-1",
				...(capabilities === undefined ? {} : { capabilities }),
			}).requestBlobSplice,
		).toBe(expected);
	},
);

test.each([
	[undefined, undefined],
	[{ kind: "workspace" }, { kind: "workspace" }],
	[
		{ kind: "branch", branchId: "draft-id" },
		{ kind: "branch", branchId: "draft-id" },
	],
])("remote handshake decodes an additive session scope: %j", (scope, expected) => {
	expect(
		decodeHandshake({
			protocolVersion: 1,
			activeBranchId: "main-id",
			sessionId: "session-1",
			...(scope === undefined ? {} : { sessionScope: scope }),
		}).sessionScope,
	).toEqual(expected);
});

test.each([
	null,
	{},
	{ kind: "branch" },
	{ kind: "branch", branchId: "" },
	{ kind: "unknown" },
])("remote handshake rejects an invalid session scope: %j", (sessionScope) => {
	expect(() =>
		decodeHandshake({
			protocolVersion: 1,
			activeBranchId: "main-id",
			sessionId: "session-1",
			sessionScope,
		}),
	).toThrow(
		expect.objectContaining({
			code: "LIX_REMOTE_PROTOCOL_ERROR",
			message: expect.stringContaining("remote handshake sessionScope"),
		}),
	);
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
test("observe blob deltas fail closed without an exact non-overlapping base", () => {
	const full = {
		sequence: 0,
		mutationSequence: 0,
		result: {
			columns: ["data"],
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
