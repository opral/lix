import { describe, expect, test } from "vitest";
import type * as LixSdk from "../src/index.js";

type Sdk = typeof LixSdk;
type Lix = Awaited<ReturnType<Sdk["openLix"]>>;

/** Exercise identical archives through each platform's public SDK and worker. */
export function registerPluginExecutionContract(
  name: string,
  loadSdk: () => Promise<Sdk>,
  loadArchives?: Sdk["bundledPluginArchives"],
): void {
  describe(`${name} plugin execution`, () => {
    test("CSV detects file edits, renders SQL edits, and merges independent rows", async () => {
      const sdk = await loadSdk();
      const { openLix } = sdk;
      const bundledPluginArchives = loadArchives ?? sdk.bundledPluginArchives;
      const lix = await openLix();
      try {
        const csv = (await bundledPluginArchives()).find(
          (p) => p.key === "plugin_csv",
        );
        if (!csv) throw new Error("expected bundled CSV archive");
        await write(lix, `/.lix/plugins/${csv.fileName}`, csv.archiveBytes);
        await write(lix, "/people.csv", "name,age\nAda,36\nGrace,37\n");
        expect(await cells(lix)).toEqual([
          ["name", "age"],
          ["Ada", "36"],
          ["Grace", "37"],
        ]);
        await write(lix, "/people.csv", "name,age\nAda,38\nGrace,37\n");
        expect(await cells(lix)).toEqual([
          ["name", "age"],
          ["Ada", "38"],
          ["Grace", "37"],
        ]);
        const rows = (
          await lix.execute("SELECT id FROM csv_row ORDER BY order_key")
        ).rows;
        const main = await lix.activeBranchId();
        const draft = await lix.createBranch({ name: "Plugin draft" });
        await lix.switchBranch({ branchId: draft.id });
        await lix.execute("UPDATE csv_row SET cells = $1 WHERE id = $2", [
          ["Ada", "39"],
          rows[1]!.id as string,
        ]);
        expect(await read(lix, "/people.csv")).toBe(
          "name,age\nAda,39\nGrace,37\n",
        );
        await lix.switchBranch({ branchId: main });
        await lix.execute("UPDATE csv_row SET cells = $1 WHERE id = $2", [
          ["Grace", "40"],
          rows[2]!.id as string,
        ]);
        expect(
          (await lix.mergeBranchPreview({ sourceBranchId: draft.id }))
            .outcome,
        ).toEqual("mergeCommitted");
        await lix.mergeBranch({ sourceBranchId: draft.id });
        expect(await cells(lix)).toEqual([
          ["name", "age"],
          ["Ada", "39"],
          ["Grace", "40"],
        ]);
        expect(await read(lix, "/people.csv")).toBe(
          "name,age\nAda,39\nGrace,40\n",
        );
      } finally {
        await lix.close();
      }
    }, 120_000);

    test("CSV merges different cells of the same row through the plugin column merger", async () => {
      const sdk = await loadSdk();
      const { openLix } = sdk;
      const bundledPluginArchives = loadArchives ?? sdk.bundledPluginArchives;
      const lix = await openLix();
      try {
        const csv = (await bundledPluginArchives()).find(
          (p) => p.key === "plugin_csv",
        );
        if (!csv) throw new Error("expected bundled CSV archive");
        await write(lix, `/.lix/plugins/${csv.fileName}`, csv.archiveBytes);
        await write(lix, "/same-row.csv", "name,age\nAda,36\n");
        const rows = (
          await lix.execute("SELECT id FROM csv_row ORDER BY order_key")
        ).rows;
        const id = rows[1]!.id as string;
        const main = await lix.activeBranchId();
        const draft = await lix.createBranch({ name: "Edit name" });
        await lix.switchBranch({ branchId: draft.id });
        await lix.execute("UPDATE csv_row SET cells = $1 WHERE id = $2", [
          ["Ada Lovelace", "36"],
          id,
        ]);
        await lix.switchBranch({ branchId: main });
        await lix.execute("UPDATE csv_row SET cells = $1 WHERE id = $2", [
          ["Ada", "37"],
          id,
        ]);
        await lix.mergeBranch({ sourceBranchId: draft.id });
        // Both branches changed the same `cells` column. LWW would lose one
        // edit; only the CSV guest merger can compose these cell updates.
        expect(await cells(lix)).toEqual([
          ["name", "age"],
          ["Ada Lovelace", "37"],
        ]);
        expect(await read(lix, "/same-row.csv")).toBe(
          "name,age\nAda Lovelace,37\n",
        );
        const snapshot = new Uint8Array(
          await new Response(lix.exportSnapshot()).arrayBuffer(),
        );
        await lix.close();
        const reopened = await openLix.fromSnapshot(snapshot);
        try {
          await reopened.execute(
            "UPDATE csv_row SET cells = $1 WHERE id = $2",
            [["Ada Lovelace", "38"], id],
          );
          expect(await read(reopened, "/same-row.csv")).toBe(
            "name,age\nAda Lovelace,38\n",
          );
        } finally {
          await reopened.close();
        }
      } finally {
        await lix.close();
      }
    }, 120_000);

    test("Markdown detects nodes and renders a SQL edit after snapshot restore", async () => {
      const sdk = await loadSdk();
      const { openLix } = sdk;
      const bundledPluginArchives = loadArchives ?? sdk.bundledPluginArchives;
      const source = await openLix();
      let restored: Lix | undefined;
      try {
        const markdown = (await bundledPluginArchives()).find(
          (p) => p.key === "plugin_markdown",
        );
        if (!markdown) throw new Error("expected bundled Markdown archive");
        await write(
          source,
          `/.lix/plugins/${markdown.fileName}`,
          markdown.archiveBytes,
        );
        await write(source, "/notes.md", "# Heading\n\nOriginal paragraph.\n");
        expect(
          (
            await source.execute("SELECT kind FROM markdown_node ORDER BY kind")
          ).rows.map((r) => r.kind),
        ).toEqual(["document", "heading", "paragraph"]);
        restored = await openLix.fromSnapshot(source.exportSnapshot());
        await restored.execute(
          "UPDATE markdown_node SET payload_json = $1 WHERE kind = 'paragraph'",
          [
            JSON.stringify({
              inline: [{ type: "text", value: "Edited paragraph." }],
            }),
          ],
        );
        expect(await read(restored, "/notes.md")).toBe(
          "# Heading\n\nEdited paragraph.\n",
        );
        expect(await read(source, "/notes.md")).toBe(
          "# Heading\n\nOriginal paragraph.\n",
        );
      } finally {
        await restored?.close();
        await source.close();
      }
    }, 120_000);
  });
}

async function write(
  lix: Lix,
  path: string,
  content: string | Uint8Array,
): Promise<void> {
  await lix.execute(
    "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET content = excluded.content",
    [
      path,
      typeof content === "string" ? new TextEncoder().encode(content) : content,
    ],
  );
}
async function read(lix: Lix, path: string): Promise<string> {
  const content = (
    await lix.execute("SELECT content FROM lix_file WHERE path = $1", [path])
  ).rows[0]?.content;
  expect(content).toBeInstanceOf(Uint8Array);
  return new TextDecoder().decode(content as Uint8Array);
}
async function cells(lix: Lix): Promise<unknown[]> {
  return (
    await lix.execute("SELECT cells FROM csv_row ORDER BY order_key")
  ).rows.map((row) => row.cells);
}
