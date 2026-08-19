import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
	define: {
		"import.meta.env.LIX_WASM_STORAGE_BENCH": JSON.stringify(
			process.env.LIX_WASM_STORAGE_BENCH ?? "0",
		),
	},
	test: {
		include: ["src/**/*.browser.test.ts"],
		browser: {
			enabled: true,
			headless: true,
			provider: playwright(),
			instances: [{ browser: "chromium" }],
		},
	},
});
