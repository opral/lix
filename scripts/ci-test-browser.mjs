import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";

export async function runBrowserSuites(run) {
  // Building and opening both packed wasm fixtures concurrently can exceed
  // the OPFS smoke test's open deadline on a busy CI host.
  for (const directory of ["packages/js-sdk", "packages/storage-opfs"]) {
    for (const script of ["test:browser:built", "test:browser:production"]) await run(directory, script);
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runBrowserSuites((cwd, script) => new Promise((resolve, reject) => {
    console.log(`Running ${cwd}: ${script}`);
    const child = spawn("npm", ["run", script], { cwd, stdio: "inherit" });
    child.on("error", reject);
    child.on("exit", (code, signal) => code === 0 ? resolve() : reject(new Error(`${cwd} ${script} exited ${code ?? signal}`)));
  }));
}
