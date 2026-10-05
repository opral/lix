import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
	server: {
		fs: {
			allow: [
				fileURLToPath(new URL(".", import.meta.url)),
				fileURLToPath(new URL("../lix/tests/fixtures/plugin-api", import.meta.url)),
			],
		},
	},
	optimizeDeps: {
		include: [
			"@bytecodealliance/jco-transpile/wasm-tools",
			"@bytecodealliance/jco-transpile/component",
			"binaryen",
		],
	},
	define: {
		"import.meta.env.LIX_WASM_STORAGE_BENCH": JSON.stringify(
			process.env.LIX_WASM_STORAGE_BENCH ?? "0",
		),
	},
	test: {
		include: ["src/**/*.browser.test.ts"],
		// Each file boots real WASM workers. Isolate their startup budgets;
		// concurrency and cross-worker races remain exercised within each test.
		fileParallelism: false,
		browser: {
			enabled: true,
			headless: true,
			provider: playwright(),
			instances: [{ browser: "chromium" }],
		},
	},
});
