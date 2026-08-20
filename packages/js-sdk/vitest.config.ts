import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
	resolve: {
		alias: {
			"#binding": fileURLToPath(new URL("./src/binding.node.ts", import.meta.url)),
			"#worker-factory": fileURLToPath(
				new URL("./src/worker/factory.node.ts", import.meta.url),
			),
		},
	},
	test: {
		environment: "node",
		include: ["src/**/*.test.ts"],
		exclude: ["src/**/*.browser.test.ts"],
	},
});
