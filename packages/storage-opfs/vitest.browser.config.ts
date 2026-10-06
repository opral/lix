import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
	optimizeDeps: {
		// The SDK loads its component host lazily. Discovering these dependencies
		// after tests start can reload the page while OPFS workers are active.
		// Resolve through the SDK so this also works with isolated dependencies.
		include: [
			"@lix-js/sdk > @bytecodealliance/jco-transpile/component",
			"@lix-js/sdk > binaryen",
		],
	},
	server: {
		fs: {
			// The provider's peer SDK is a sibling package during workspace tests.
			allow: [new URL("..", import.meta.url).pathname],
		},
	},
	test: {
		include: ["tests/**/*.browser.test.ts"],
		// Each file boots real WASM/OPFS workers. Isolate their startup budgets;
		// concurrency and ownership races remain exercised within each test.
		fileParallelism: false,
		browser: {
			enabled: true,
			headless: true,
			provider: playwright(),
			instances: [{ browser: "chromium" }],
		},
	},
});
