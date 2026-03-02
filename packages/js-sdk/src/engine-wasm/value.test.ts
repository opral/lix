import { expect, test } from "vitest";
import { Value } from "./index.js";

test("Value.asBlob returns empty Uint8Array for canonical empty blob", () => {
	const decoded = Value.from({ kind: "blob", base64: "" }).asBlob();
	expect(decoded).toBeInstanceOf(Uint8Array);
	expect(decoded?.byteLength).toBe(0);
});

test("Value.asBlob roundtrips non-empty canonical blob", () => {
	const decoded = Value.from({ kind: "blob", base64: "AQID" }).asBlob();
	expect(decoded).toEqual(new Uint8Array([1, 2, 3]));
});
