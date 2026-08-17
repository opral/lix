import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

let observedLix;
let observation;
let pendingObservation;
let benchmarkWriterLix;
let benchmarkWriterChannel;

const storageName =
	new URL(globalThis.location.href).searchParams.get("storage") ??
	"packed-vite-production";
const benchmarkChannelName = `lix-opfs-benchmark:${storageName}`;

globalThis.__storageOpfsProductionSmoke = run();
globalThis.__storageOpfsProductionFailover = failover;
globalThis.__storageOpfsStartObservation = startObservation;
globalThis.__storageOpfsCommitObservedValue = commitObservedValue;
globalThis.__storageOpfsFinishObservation = finishObservation;
globalThis.__storageOpfsRecoverObservation = recoverObservation;
globalThis.__storageOpfsPrepareBenchmarkWriter = prepareBenchmarkWriter;
globalThis.__storageOpfsBenchmarkCrossTab = benchmarkCrossTab;

async function run() {
	const [first, second] = await Promise.all([
		openLix({ storage: new OpfsStorage({ name: storageName }) }),
		openLix({ storage: new OpfsStorage({ name: storageName }) }),
	]);
	await first.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
		["packed-vite", "persistent-production"],
	);
	const shared = await second.execute(
		"SELECT value FROM lix_key_value WHERE key = $1",
		["packed-vite"],
	);
	if (shared.rows[0]?.get("value") !== "persistent-production") {
		throw new Error("second Lix worker did not observe the owner commit");
	}
	await Promise.all([first.close(), second.close()]);

	const reopened = await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	try {
		const persisted = await reopened.execute(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["packed-vite"],
		);
		return { message: persisted.rows[0]?.get("value") };
	} finally {
		await reopened.close();
	}
}

async function failover() {
	const lix = await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	try {
		const result = await lix.execute(
			"SELECT value FROM lix_key_value WHERE key = $1",
			["packed-vite"],
		);
		return result.rows[0]?.get("value");
	} finally {
		await lix.close();
	}
}

async function startObservation() {
	observedLix = await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	observation = observedLix.observe(
		"SELECT value FROM lix_key_value WHERE key = $1",
		["cross-tab-observe"],
	);
	await observation.next();
	pendingObservation = observation.next();
}

async function commitObservedValue(value) {
	const lix = await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	try {
		await lix.execute(
			"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
			["cross-tab-observe", value],
		);
	} finally {
		await lix.close();
	}
}

async function finishObservation() {
	try {
		const event = await Promise.race([
			pendingObservation,
			new Promise((_, reject) =>
				setTimeout(() => reject(new Error("cross-tab observation timed out")), 5_000),
			),
		]);
		return event?.result.rows[0]?.get("value");
	} finally {
		observation?.close();
		await observedLix?.close();
		observation = undefined;
		observedLix = undefined;
		pendingObservation = undefined;
	}
}

async function recoverObservation(value) {
	const startedAt = performance.now();
	await commitObservedValue(value);
	const observed = await finishObservation();
	return { value: observed, elapsedMs: performance.now() - startedAt };
}

async function prepareBenchmarkWriter() {
	benchmarkWriterLix ??= await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	benchmarkWriterChannel ??= new BroadcastChannel(benchmarkChannelName);
	benchmarkWriterChannel.onmessage = async (event) => {
		const request = event.data;
		if (!request || request.kind !== "commit") return;
		try {
			await benchmarkWriterLix.execute(
				"INSERT INTO lix_key_value (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = excluded.value",
				["cross-tab-benchmark", request.value],
			);
		} catch (error) {
			benchmarkWriterChannel.postMessage({
				kind: "error",
				sequence: request.sequence,
				message: error instanceof Error ? error.message : String(error),
			});
		}
	};
}

async function benchmarkCrossTab(sampleCount) {
	const lix = await openLix({
		storage: new OpfsStorage({ name: storageName }),
	});
	const observed = lix.observe(
		"SELECT value FROM lix_key_value WHERE key = $1",
		["cross-tab-benchmark"],
	);
	const control = new BroadcastChannel(benchmarkChannelName);
	try {
		await observed.next();
		const samples = [];
		for (let sequence = 0; sequence < sampleCount; sequence += 1) {
			const changed = observed.next();
			const startedAt = performance.now();
			control.postMessage({ kind: "commit", sequence, value: sequence });
			const event = await Promise.race([
				changed,
				new Promise((_, reject) =>
					setTimeout(
						() => reject(new Error("cross-tab benchmark timed out")),
						5_000,
					),
				),
			]);
			if (event?.result.rows[0]?.get("value") !== sequence) {
				throw new Error(`cross-tab benchmark observed the wrong value at ${sequence}`);
			}
			samples.push(performance.now() - startedAt);
		}
		return samples;
	} finally {
		control.close();
		observed.close();
		await lix.close();
	}
}
