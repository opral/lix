import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { expect, test } from "vitest";

const execFileAsync = promisify(execFile);

test("closing a native plugin runtime releases Node's event loop while the binding stays reachable", async () => {
	const bindingUrl = new URL("../dist/binding.node.js", import.meta.url).href;
	const pluginArchivePath = new URL(
		"../../lix/tests/fixtures/plugin-api/v2/plugin_csv.lixplugin",
		import.meta.url,
	).pathname;
	const script = `
  import { readFile } from "node:fs/promises";
  import { openNativeLixBinding } from ${JSON.stringify(bindingUrl)};
  import { Value } from ${JSON.stringify(new URL("../dist/value.js", import.meta.url).href)};
  const binding = await openNativeLixBinding({ kind: "memory" });
  globalThis.closedBinding = binding;
  const execute = (sql, params) => binding.execute(sql, params.map(value => Value.from(value)._toNative()));
  const csvArchive = new Uint8Array(await readFile(${JSON.stringify(pluginArchivePath)}));
  await execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", ["/.lix/plugins/plugin_csv.lixplugin", csvArchive]);
  await execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", ["/lifecycle.csv", new TextEncoder().encode("name\\nAda\\n")]);
  const rows = await execute("SELECT cells FROM csv_row ORDER BY order_key", []);
  if (rows.rows.length !== 2) throw new Error("Plugin did not execute");
  await binding.close();
  console.log("closed");
 `;
  const result = await execFileAsync(
    process.execPath,
    ["--input-type=module", "--eval", script],
    {
      timeout: 45_000,
      maxBuffer: 1024 * 1024,
    },
  );
  expect(result.stdout.trim()).toBe("closed");
}, 60_000);
