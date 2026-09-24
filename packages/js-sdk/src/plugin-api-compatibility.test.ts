import { readFile } from "node:fs/promises";
import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";
import { registerPluginApiAdditionTest } from "../tests/plugin-api-additions.js";
import { loadTestPluginArchives } from "./plugin-test-archives.node.js";

registerPluginApiAdditionTest(async () => new Uint8Array(await readFile(
  new URL("../../lix/tests/fixtures/plugin-api/v2/import-subset.wasm", import.meta.url),
)));

for (const api of ["legacy-v2", "v2"]) {
  registerPluginExecutionContract(
    `frozen ${api} Node`,
    async () => import("./index.js"),
    () => loadTestPluginArchives(api),
  );
}
