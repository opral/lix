export type SqliteBackendOptions = {
	path: string;
};

export type FsBackendOptions = {
	path: string;
	lixDir?: string;
	syncAllFiles: boolean;
};

export type OpenLixOptions = {
	backend?:
		| import("./open-lix.js").SqliteBackend
		| import("./open-lix.js").FsBackend;
};

export type LixValue =
	| { kind: "null"; value: null }
	| { kind: "boolean"; value: boolean }
	| { kind: "integer"; value: number }
	| { kind: "real"; value: number }
	| { kind: "text"; value: string }
	| { kind: "json"; value: JsonValue }
	| { kind: "blob"; value: Uint8Array };

export type JsonValue =
	| null
	| boolean
	| number
	| string
	| readonly JsonValue[]
	| { readonly [key: string]: JsonValue };

export type SqlParam = JsonValue | Uint8Array | import("./value.js").Value;

export type ExecuteOptions = {
	originKey?: string;
};

export type ExecuteResult = {
	columns: string[];
	rows: RowLike[];
	rowsAffected: number;
	notices: Array<{
		code: string;
		message: string;
		hint?: string;
	}>;
};

export type ObserveEvent = {
	sequence: number;
	mutationSequence: number;
	result: ExecuteResult;
};

export type RowLike = {
	get(column: string): unknown;
	value(column: string): ValueLike;
	toObject(): Record<string, unknown>;
	toValueMap(): Record<string, ValueLike>;
};

export type ValueLike = {
	readonly kind: LixValue["kind"];
	toJS(): unknown;
	asBytes(): Uint8Array | undefined;
};

export type CreateBranchOptions = {
	id?: string;
	name: string;
	fromCommitId?: string;
};

export type CreateBranchReceipt = {
	id: string;
	name: string;
	hidden: boolean;
	commitId: string;
};

export type SwitchBranchOptions = {
	branchId: string;
};

export type SwitchBranchReceipt = {
	branchId: string;
};

export type MergeBranchOptions = {
	sourceBranchId: string;
};

export type MergeBranchOutcome =
	| "alreadyUpToDate"
	| "fastForward"
	| "mergeCommitted";

export type MergeBranchReceipt = {
	outcome: MergeBranchOutcome;
	targetBranchId: string;
	sourceBranchId: string;
	baseCommitId: string;
	targetHeadBeforeCommitId: string;
	sourceHeadBeforeCommitId: string;
	targetHeadAfterCommitId: string;
	createdMergeCommitId: string | null;
	changeStats: MergeChangeStats;
};

export type MergeBranchPreview = {
	outcome: MergeBranchOutcome;
	targetBranchId: string;
	sourceBranchId: string;
	baseCommitId: string;
	targetHeadCommitId: string;
	sourceHeadCommitId: string;
	changeStats: MergeChangeStats;
	conflicts: MergeConflict[];
};

export type MergeChangeStats = {
	total: number;
	added: number;
	modified: number;
	removed: number;
};

export type MergeConflict = {
	kind: "sameEntityChanged";
	schemaKey: string;
	entityPk: unknown;
	fileId: string | null;
	target: MergeConflictSide;
	source: MergeConflictSide;
};

export type MergeConflictSide = {
	kind: "added" | "modified" | "removed";
	beforeChangeId: string | null;
	afterChangeId: string | null;
};
