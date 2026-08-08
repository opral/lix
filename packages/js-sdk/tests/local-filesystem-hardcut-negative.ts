import { LocalFilesystem } from "../src/index.js";
import type { LixBinding, LixStorageConfig } from "../src/binding-types.js";
import type { WorkerOperation } from "../src/worker/protocol.js";

const positional = new LocalFilesystem("/tmp/workspace");
void positional.path;

// Every directive must remain used. TypeScript reports an unused directive if
// any deleted public/native/worker surface becomes nameable again.
// @ts-expect-error removed options object constructor
new LocalFilesystem({ path: "/tmp/workspace", syncAllFiles: true });
// @ts-expect-error removed second constructor argument
new LocalFilesystem("/tmp/workspace", { syncAllFiles: false });
// @ts-expect-error removed public options property
void positional.lixDir;
// @ts-expect-error removed public options property
void positional.syncAllFiles;
// @ts-expect-error removed public manual import
void positional.importPaths;
// @ts-expect-error removed public manual sync
void positional.syncDiskToLix;
// @ts-expect-error removed exported options type
type RemovedOptions = import("../src/index.js").LocalFilesystemOptions;

declare const binding: LixBinding;
// @ts-expect-error removed native manual import binding
void binding.importFilesystemPaths;
// @ts-expect-error removed native manual sync binding
void binding.syncDiskToLix;

type LocalConfig = Extract<LixStorageConfig, { kind: "localFilesystem" }>;
declare const localConfig: LocalConfig;
// @ts-expect-error removed native lixDir option
void localConfig.lixDir;
// @ts-expect-error removed native syncAllFiles option
void localConfig.syncAllFiles;

const oldImport: WorkerOperation = {
	// @ts-expect-error removed worker manual import operation
	kind: "importFilesystemPaths",
	paths: ["note.md"],
};
// @ts-expect-error removed worker manual sync operation
const oldSync: WorkerOperation = { kind: "syncDiskToLix" };

void (null as unknown as RemovedOptions);
void oldImport;
void oldSync;
