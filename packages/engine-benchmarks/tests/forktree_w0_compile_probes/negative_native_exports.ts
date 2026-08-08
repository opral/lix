// MUST fail type-checking: removed native filesystem compatibility methods.
declare const filesystem: {
  syncAllFiles(): void;
  lixDir(): string;
  importFilesystemPaths(paths: string[]): void;
  syncDiskToLix(): void;
};

filesystem.syncAllFiles();
filesystem.lixDir();
filesystem.importFilesystemPaths([]);
filesystem.syncDiskToLix();
