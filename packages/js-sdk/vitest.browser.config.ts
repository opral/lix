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
		include: ["@bytecodealliance/jco-transpile/wasm-tools"],
	},
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
