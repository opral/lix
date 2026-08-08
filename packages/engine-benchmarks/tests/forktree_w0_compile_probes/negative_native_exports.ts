// MUST fail type-checking after the filesystem API hard cut. These types are
// imported from the real JS SDK; the probe deliberately does not declare the
// removed members itself.
import { LocalFilesystem } from "../../../js-sdk/src/open-lix.js";
import type { LixBinding } from "../../../js-sdk/src/binding-types.js";

const filesystem = null as unknown as LocalFilesystem;
void filesystem.syncAllFiles;
void filesystem.lixDir;
void filesystem.importFilesystemPaths;
void filesystem.syncDiskToLix;

function probeNativeBinding(binding: LixBinding): void {
  void binding.importFilesystemPaths([]);
  void binding.syncDiskToLix();
}

void probeNativeBinding;
