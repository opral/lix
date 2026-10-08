import { build } from "esbuild";
import { sqliteBundlePlugin } from "./sqlite-bundle-plugin.mjs";

// SQLite's package entry also initializes the unused Worker1-promiser API.
// Remove that separately marked upstream module so consumers cannot discover
// and rebundle its unrelated worker URL inside our dedicated migration worker.
await build({
  entryPoints: ["js/migration.worker.ts"],
  bundle: true,
  format: "esm",
  platform: "browser",
  target: "es2022",
  loader: { ".wasm": "dataurl" },
  external: ["@lix-js/sdk/migration"],
  sourcemap: true,
  outfile: "dist/migration.worker.js",
  plugins: [sqliteBundlePlugin()],
});
