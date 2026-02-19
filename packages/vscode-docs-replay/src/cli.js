import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __dirname = dirname(fileURLToPath(import.meta.url));
const packageRoot = join(__dirname, "..");

async function main() {
  const argv = process.argv.slice(2);
  if (argv[0] === "--") {
    argv.shift();
  }
  const [command, ...args] = argv;

  if (!command || command === "help" || command === "--help" || command === "-h") {
    printHelp();
    return;
  }

  switch (command) {
    case "replay": {
      const commits = readNumericFlag(args, "--commits");
      const env = {
        ...process.env,
      };
      if (commits !== undefined) {
        env.VSCODE_REPLAY_COMMITS = String(commits);
      }

      await run("pnpm", ["run", "bootstrap"], { env });
      await run("pnpm", ["run", "replay:raw"], { env });
      return;
    }

    case "analyze": {
      await run("pnpm", ["run", "analyze:raw"]);
      return;
    }

    case "reset": {
      await run("pnpm", ["run", "reset:raw"]);
      return;
    }

    default: {
      throw new Error(`unknown command '${command}'`);
    }
  }
}

function printHelp() {
  console.log("Usage:");
  console.log("  replay --commits 100");
  console.log("  analyze");
  console.log("  reset");
}

function readNumericFlag(args, flag) {
  const index = args.indexOf(flag);
  if (index < 0) {
    return undefined;
  }
  const raw = args[index + 1];
  if (!raw) {
    throw new Error(`${flag} requires a value`);
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error(`${flag} must be a positive integer, got '${raw}'`);
  }
  return parsed;
}

async function run(command, args, options = {}) {
  await new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: packageRoot,
      env: options.env ?? process.env,
      stdio: "inherit",
    });

    child.on("error", reject);
    child.on("exit", (code) => {
      if (code === 0) {
        resolve();
      } else {
        reject(new Error(`${command} ${args.join(" ")} exited with code ${code}`));
      }
    });
  });
}

main().catch((error) => {
  console.error("cli failed");
  console.error(error);
  process.exitCode = 1;
});
