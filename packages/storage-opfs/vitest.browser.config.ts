import { playwright } from "@vitest/browser-playwright";
import { defineConfig } from "vitest/config";

export default defineConfig({
	server: {
		fs: {
			// The provider's peer SDK is a sibling package during workspace tests.
			allow: [new URL("..", import.meta.url).pathname],
		},
	},
	test: {
		include: ["tests/**/*.browser.test.ts"],
		browser: {
			enabled: true,
			headless: true,
			provider: playwright(),
			instances: [{ browser: "chromium" }],
		},
	},
});
