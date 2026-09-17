import {openLix, bundledPluginArchives} from '@lix-js/sdk';
import {FilesystemStorage} from '@lix-js/storage-filesystem';
const lix = await openLix({storage: new FilesystemStorage({path: process.argv[2] ?? './repository'})});
try {
 const markdown = (await bundledPluginArchives()).find(plugin => plugin.key === 'plugin_markdown');
 if (!markdown) throw new Error('Published Markdown plugin missing');
 await lix.execute("INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_markdown.lixplugin', $1)", [markdown.archiveBytes]);
 await lix.execute("INSERT INTO lix_file (path, content) VALUES ('/notes.md', $1)", [new TextEncoder().encode('# Notes\n\nCheckpoint paragraph.\n')]);
 await lix.execute('SELECT commit_id FROM lix_create_checkpoint()');
 await lix.execute("UPDATE lix_file SET content = $1 WHERE path = '/notes.md'", [new TextEncoder().encode('# Notes\n\nWorking paragraph.\n')]);
 const nodes = await lix.execute('SELECT kind FROM markdown_node ORDER BY kind');
 if (nodes.rows.map(row => row.kind).join(',') !== 'document,heading,paragraph') throw new Error('Plugin did not track Markdown nodes');
 console.log((await lix.execute('SELECT id FROM lix_commit WHERE is_checkpoint')).rows);
} finally {await lix.close();}
