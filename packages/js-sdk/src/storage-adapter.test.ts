import { expect, test } from "vitest";
import { isJsProviderLixStorage } from "./storage-adapter.js";

test("accepts only the hard-cut storage provider registration version", () => {
	const provider = {
		moduleUrl: "https://example.test/storage-provider.js",
		options: {},
	};

	expect(
		isJsProviderLixStorage({
			lixStorage: { version: 3, ...provider },
		}),
	).toBe(true);
	expect(
		isJsProviderLixStorage({
			lixStorage: { version: 2, ...provider },
		}),
	).toBe(false);
});
