import { spawn } from "node:child_process";
import { pathToFileURL } from "node:url";

export async function runBrowserSuites(run) {
  // Each package uses independent browser contexts and temporary Vite fixtures.
  // Keep built-package and packed-production coverage in both suites.
  const results = await Promise.allSettled(["packages/js-sdk", "packages/storage-opfs"].map(async directory => {
    for (const script of ["test:browser:built", "test:browser:production"]) await run(directory, script);
  }));
  const failures = results.filter(result => result.status === "rejected");
  if (failures.length) throw new AggregateError(failures.map(result => result.reason), "Browser integration suites failed");
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  await runBrowserSuites((cwd, script) => new Promise((resolve, reject) => {
    console.log(`Running ${cwd}: ${script}`);
    const child = spawn("npm", ["run", script], { cwd, stdio: "inherit" });
    child.on("error", reject);
    child.on("exit", (code, signal) => code === 0 ? resolve() : reject(new Error(`${cwd} ${script} exited ${code ?? signal}`)));
  }));
}
