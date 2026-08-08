import { LocalFilesystem } from "@lix-js/sdk";

const positional = new LocalFilesystem("/tmp/workspace");
void positional;

// Every predecessor form must be unnameable. These directives make tsc fail
// if a removed API accidentally survives (unused @ts-expect-error).
// @ts-expect-error removed object/options constructor
new LocalFilesystem({ path: "/tmp/workspace", syncAllFiles: true });
// @ts-expect-error removed second constructor argument
new LocalFilesystem("/tmp/workspace", { syncAllFiles: false });
// @ts-expect-error removed public synchronization method
positional.importPaths(["note.md"]);
// @ts-expect-error removed public synchronization method
positional.syncDiskToLix();
// @ts-expect-error removed option property
positional.lixDir;
// @ts-expect-error removed option property
positional.syncAllFiles;

// @ts-expect-error removed exported option type
type RemovedOptions = import("@lix-js/sdk").LocalFilesystemOptions;
void (null as unknown as RemovedOptions);
