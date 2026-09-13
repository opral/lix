import { readFile } from "node:fs/promises";
import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";
import { registerPluginApiAdditionTest } from "../tests/plugin-api-additions.js";

registerPluginApiAdditionTest(async () => new Uint8Array(await readFile(
  new URL("../../lix/tests/fixtures/plugin-api/v2/import-subset.wasm", import.meta.url),
)));

for (const api of ["legacy-v2", "v2"]) {
  registerPluginExecutionContract(
    `frozen ${api} Node`,
    async () => import("./index.js"),
    async () =>
      Promise.all(
        ["plugin_csv", "plugin_markdown"].map(async (key) => ({
          key,
          fileName: `${key}.lixplugin`,
          archiveBytes: new Uint8Array(
            await readFile(
              new URL(
                `../../lix/tests/fixtures/plugin-api/${api}/${key}.lixplugin`,
                import.meta.url,
              ),
            ),
          ),
        })),
      ),
  );
}
