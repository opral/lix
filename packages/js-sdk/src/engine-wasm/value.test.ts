import { expect, test } from "vitest";
import { Value } from "./index.js";

test("Value.asBytes returns empty Uint8Array for canonical empty blob", () => {
	const decoded = Value.from({ kind: "blob", base64: "" }).asBytes();
	expect(decoded).toBeInstanceOf(Uint8Array);
	expect(decoded?.byteLength).toBe(0);
});

test("Value.asBytes roundtrips non-empty canonical blob", () => {
	const decoded = Value.from({ kind: "blob", base64: "AQID" }).asBytes();
	expect(decoded).toEqual(new Uint8Array([1, 2, 3]));
});
