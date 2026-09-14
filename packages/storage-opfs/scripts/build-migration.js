import { build } from "esbuild";
import { readFile } from "node:fs/promises";

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
  plugins: [
    {
      name: "sqlite-initializer-only",
      setup(builder) {
        builder.onLoad(
          { filter: /@sqlite\.org[\/]sqlite-wasm[\/]dist[\/]index\.mjs$/ },
          async ({ path }) => {
            const source = await readFile(path, "utf8");
            const moduleStart = "//#region src/bin/sqlite3-worker1-promiser.mjs";
            const initializerStart = "//#region src/bin/sqlite3-bundler-friendly.mjs";
            const extraExport = ", sqlite3_worker1_promiser_default as sqlite3Worker1Promiser";
            if (
              !source.startsWith(moduleStart) ||
              !source.includes(initializerStart) ||
              !source.includes(extraExport)
            )
              throw new Error(
                "SQLite package module layout changed; review initializer-only migration bundle",
              );
            return {
              contents: source.slice(source.indexOf(initializerStart)).replace(extraExport, ""),
              loader: "js",
            };
          },
        );
      },
    },
  ],
});
