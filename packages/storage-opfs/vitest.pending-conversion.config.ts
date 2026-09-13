import { readFileSync } from "node:fs";
import { defineConfig, mergeConfig } from "vitest/config";
import browserConfig from "./vitest.browser.config.js";

const path = process.env.LIX_BROWSER_CONVERSION_MANIFEST;
if (!path) throw new Error("Set LIX_BROWSER_CONVERSION_MANIFEST to the synthetic native fixture manifest");
const manifest = readFileSync(path, "utf8");
const authority = new URL(JSON.parse(manifest).url);
if (!["127.0.0.1", "localhost", "[::1]"].includes(authority.hostname)) {
	throw new Error("Pending conversion QA requires a disposable loopback authority");
}
const config = mergeConfig(browserConfig, defineConfig({
	plugins: [{
		name: "pending-conversion-fixture",
		configureServer(server) {
			server.middlewares.use((request, response, next) => {
				if (request.url !== "/__conversion_fixture.json") return next();
				response.setHeader("content-type", "application/json");
				response.end(manifest);
			});
		},
	}],
	server: { proxy: {
		"/__conversion_authority": {
			target: authority.origin,
			changeOrigin: true,
			rewrite: (path) => path.replace("/__conversion_authority", ""),
		},
	} },
	test: { fileParallelism: false },
}));
config.test!.include = ["tests/pending-conversion.manual.test.ts"];
export default config;
