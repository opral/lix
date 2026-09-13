import { registerPluginExecutionContract } from "../tests/plugin-execution-contract.js";
import { registerPluginApiAdditionTest } from "../tests/plugin-api-additions.js";

registerPluginApiAdditionTest(async () => {
  const response = await fetch(new URL(
    "../../lix/tests/fixtures/plugin-api/v2/import-subset.wasm", import.meta.url,
  ));
  if (!response.ok) throw new Error(`Could not load frozen component: ${response.status}`);
  return new Uint8Array(await response.arrayBuffer());
});

// Static URLs let Vite serve the exact same files used by native tests.
const fixtures = [
  {
    api: "legacy-v2",
    archives: [
      [
        "plugin_csv",
        new URL(
          "../../lix/tests/fixtures/plugin-api/legacy-v2/plugin_csv.lixplugin",
          import.meta.url,
        ),
      ],
      [
        "plugin_markdown",
        new URL(
          "../../lix/tests/fixtures/plugin-api/legacy-v2/plugin_markdown.lixplugin",
          import.meta.url,
        ),
      ],
    ],
  },
  {
    api: "v2",
    archives: [
      [
        "plugin_csv",
        new URL(
          "../../lix/tests/fixtures/plugin-api/v2/plugin_csv.lixplugin",
          import.meta.url,
        ),
      ],
      [
        "plugin_markdown",
        new URL(
          "../../lix/tests/fixtures/plugin-api/v2/plugin_markdown.lixplugin",
          import.meta.url,
        ),
      ],
    ],
  },
] as const;
for (const { api, archives } of fixtures) {
  registerPluginExecutionContract(
    `frozen ${api} Chromium`,
    async () => import("./index.js"),
    async () =>
      Promise.all(
        archives.map(async ([key, url]) => {
          const response = await fetch(url);
          if (!response.ok)
            throw new Error(
              `Could not load frozen archive: ${response.status}`,
            );
          return {
            key,
            fileName: `${key}.lixplugin`,
            archiveBytes: new Uint8Array(await response.arrayBuffer()),
          };
        }),
      ),
  );
}
