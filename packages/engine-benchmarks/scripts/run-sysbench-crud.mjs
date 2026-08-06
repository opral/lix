#!/usr/bin/env node

import { execFileSync, spawnSync } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const workspace = resolve(scriptDir, "../../..");
const options = parseArgs(process.argv.slice(2));
const qualification = options.has("qualify");
const postgresUrl = options.get("postgres-url");
if (!postgresUrl) fail("--postgres-url is required");

const timestamp = new Date().toISOString().replaceAll(":", "-");
const outputDir = resolve(
  workspace,
  options.get("output-dir") ?? `target/sysbench-results/${timestamp}`,
);
if (existsSync(outputDir)) fail(`refusing to reuse output directory: ${outputDir}`);
mkdirSync(outputDir, { recursive: true });

run("cargo", [
  "build",
  "-p",
  "lix_engine_benchmarks",
  "--release",
  "--example",
  "sysbench_crud",
  "--no-default-features",
  "--features",
  "sqlite,slatedb,postgres",
]);

const binary = resolve(workspace, "target/release/examples/sysbench_crud");
const revision = capture("git", ["rev-parse", "HEAD"]);
const dirty = capture("git", ["status", "--porcelain"]).length > 0;
const profile = qualification
  ? {
      name: "qualification",
      sizes: [1_000],
      clients: [1, 2],
      repetitions: 1,
      eventsPerClient: 20,
      warmupSeconds: 0,
      timeSeconds: 0,
      settleMs: 100,
    }
  : {
      name: "publication",
      sizes: parseList(options.get("sizes") ?? "100000,1000000"),
      clients: parseList(options.get("clients") ?? "1,4,16"),
      repetitions: Number(options.get("repetitions") ?? "5"),
      eventsPerClient: null,
      warmupSeconds: Number(options.get("warmup-seconds") ?? "15"),
      timeSeconds: Number(options.get("time-seconds") ?? "60"),
      settleMs: Number(options.get("settle-ms") ?? "1000"),
    };
validateProfile(profile);

const engines = ["lix-slatedb", "sqlite", "postgres"];
const workloads = [
  "point-select",
  "insert",
  "update-index",
  "update-non-index",
  "delete",
];
const counterbalancedOrders = [
  ["lix-slatedb", "sqlite", "postgres"],
  ["postgres", "sqlite", "lix-slatedb"],
  ["sqlite", "postgres", "lix-slatedb"],
  ["lix-slatedb", "postgres", "sqlite"],
  ["postgres", "lix-slatedb", "sqlite"],
  ["sqlite", "lix-slatedb", "postgres"],
];

const manifest = {
  schemaVersion: 1,
  suite: "sysbench-1.0.20-oltp-derived-common-feature",
  profile,
  revision,
  dirty,
  startedAt: new Date().toISOString(),
  host: hostMetadata(),
  runnerArguments: redactArguments(process.argv.slice(2)),
  binarySha256: sha256(binary),
  cargoLockSha256: sha256(resolve(workspace, "Cargo.lock")),
  engineOrder: [],
  resultFiles: [],
};
writeJson(resolve(outputDir, "manifest.in-progress.json"), manifest);

let matrixOrdinal = 0;
for (const tableSize of profile.sizes) {
  for (const clients of profile.clients) {
    for (const workload of workloads) {
      for (let repetition = 0; repetition < profile.repetitions; repetition++) {
        const engineOrder =
          counterbalancedOrders[(matrixOrdinal + repetition) % counterbalancedOrders.length];
        const seed = deterministicSeed(matrixOrdinal, repetition);
        manifest.engineOrder.push({
          tableSize,
          clients,
          workload,
          repetition: repetition + 1,
          seed,
          engines: engineOrder,
        });
        for (const engine of engineOrder) {
          if (!engines.includes(engine)) fail(`invalid engine order entry: ${engine}`);
          const filename = [
            engine,
            workload,
            `n${tableSize}`,
            `c${clients}`,
            `r${repetition + 1}`,
          ].join("-") + ".json";
          const output = resolve(outputDir, filename);
          const args = [
            "--engine",
            engine,
            "--workload",
            workload,
            "--table-size",
            String(tableSize),
            "--clients",
            String(clients),
            "--seed",
            String(seed),
            "--load-batch-size",
            "10000",
            "--settle-ms",
            String(profile.settleMs),
            "--target-revision",
            revision,
            "--target-dirty",
            String(dirty),
            "--output",
            output,
          ];
          if (profile.eventsPerClient !== null) {
            args.push("--events-per-client", String(profile.eventsPerClient));
          } else {
            args.push(
              "--warmup-seconds",
              String(profile.warmupSeconds),
              "--time-seconds",
              String(profile.timeSeconds),
            );
          }
          if (engine === "postgres") {
            args.push("--postgres-url", postgresUrl);
          }
          run(binary, args);
          const result = JSON.parse(readFileSync(output, "utf8"));
          validateResult(result, { engine, workload, tableSize, clients, seed });
          manifest.resultFiles.push(filename);
          writeJson(resolve(outputDir, "manifest.in-progress.json"), manifest);
        }
      }
      matrixOrdinal++;
    }
  }
}

manifest.finishedAt = new Date().toISOString();
writeJson(resolve(outputDir, "manifest.json"), manifest, true);
console.log(`Completed ${manifest.resultFiles.length} runs in ${outputDir}`);

function parseArgs(args) {
  const parsed = new Map();
  for (let index = 0; index < args.length; index++) {
    const flag = args[index];
    if (!flag.startsWith("--")) fail(`unexpected argument: ${flag}`);
    const name = flag.slice(2);
    if (name === "qualify") {
      parsed.set(name, true);
      continue;
    }
    const value = args[++index];
    if (value === undefined) fail(`${flag} requires a value`);
    parsed.set(name, value);
  }
  return parsed;
}

function parseList(value) {
  return value.split(",").map((item) => Number(item));
}

function validateProfile(value) {
  for (const [name, values] of [
    ["sizes", value.sizes],
    ["clients", value.clients],
  ]) {
    if (values.length === 0 || values.some((item) => !Number.isInteger(item) || item <= 0)) {
      fail(`${name} must contain positive integers`);
    }
  }
  if (!Number.isInteger(value.repetitions) || value.repetitions <= 0) {
    fail("repetitions must be a positive integer");
  }
}

function validateResult(result, expected) {
  for (const [field, value] of Object.entries(expected)) {
    if (result[field] !== value) {
      fail(`result qualification failed: ${field}=${result[field]} expected ${value}`);
    }
  }
  if (result.failedEvents !== 0 || result.successfulEvents <= 0) {
    fail(`result qualification failed for ${expected.engine}/${expected.workload}`);
  }
  if (result.targetRevision !== revision || result.targetDirty !== dirty) {
    fail("result revision attestation does not match the orchestrator");
  }
}

function deterministicSeed(matrixOrdinal, repetition) {
  return 1_000_003 + matrixOrdinal * 10_007 + repetition * 101;
}

function hostMetadata() {
  return {
    uname: safeCapture("uname", ["-a"]),
    cpuModel: firstMatchingLine("/proc/cpuinfo", "model name"),
    memory: firstMatchingLine("/proc/meminfo", "MemTotal"),
    blockDevices: safeCapture("lsblk", [
      "--output",
      "NAME,MODEL,TYPE,SIZE,ROTA,MOUNTPOINTS",
    ]),
    filesystem: safeCapture("findmnt", [
      "--target",
      workspace,
      "--output",
      "TARGET,SOURCE,FSTYPE,OPTIONS",
      "--noheadings",
    ]),
    rustc: safeCapture("rustc", ["--version"]),
    cargo: safeCapture("cargo", ["--version"]),
    node: process.version,
  };
}

function redactArguments(args) {
  const redacted = [...args];
  const urlIndex = redacted.indexOf("--postgres-url");
  if (urlIndex !== -1 && urlIndex + 1 < redacted.length) redacted[urlIndex + 1] = "<redacted>";
  return redacted;
}

function sha256(path) {
  return capture("sha256sum", [path]).split(/\s+/, 1)[0];
}

function firstMatchingLine(path, prefix) {
  try {
    return readFileSync(path, "utf8")
      .split("\n")
      .find((line) => line.startsWith(prefix)) ?? null;
  } catch {
    return null;
  }
}

function run(command, args) {
  const result = spawnSync(command, args, { cwd: workspace, stdio: "inherit" });
  if (result.error) fail(`${command} failed to start: ${result.error.message}`);
  if (result.status !== 0) fail(`${command} exited with status ${result.status}`);
}

function capture(command, args) {
  return execFileSync(command, args, { cwd: workspace, encoding: "utf8" }).trim();
}

function safeCapture(command, args) {
  try {
    return capture(command, args);
  } catch {
    return null;
  }
}

function writeJson(path, value, exclusive = false) {
  writeFileSync(path, `${JSON.stringify(value, null, 2)}\n`, {
    flag: exclusive ? "wx" : "w",
  });
}

function fail(message) {
  console.error(`run-sysbench-crud: ${message}`);
  process.exit(2);
}
