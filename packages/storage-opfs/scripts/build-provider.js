import { build } from "esbuild";
import { sqliteBundlePlugin } from "./sqlite-bundle-plugin.mjs";

const testOwner = process.argv.includes("--test-owner");
await build({
	entryPoints: [testOwner ? "tests/legacy-rpc/owner.ts" : "js/provider.ts"],
	bundle: true,
	format: "esm",
	platform: "browser",
	target: "es2022",
	loader: { ".wasm": "dataurl" },
	sourcemap: true,
	outfile: testOwner ? "tests/.generated/owner.js" : "dist/direct.js",
	plugins: [sqliteBundlePlugin()],
});
