export type SqliteBackendOptions = {
	path: string;
};

export type OpenLixOptions = {
	backend: {
		readonly path: string;
	};
};

export type LixValue =
	| { kind: "null"; value: null }
	| { kind: "boolean"; value: boolean }
	| { kind: "integer"; value: number }
	| { kind: "real"; value: number }
	| { kind: "text"; value: string }
	| { kind: "json"; value: unknown }
	| { kind: "blob"; base64: string };

export type SqlParam =
	| null
	| boolean
	| number
	| string
	| Uint8Array
	| Date
	| ArrayBufferView
	| Record<string, unknown>
	| LixValue
	| { readonly raw: LixValue };

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

export type RowLike = {
	get(column: string): unknown;
	value(column: string): ValueLike;
	toObject(): Record<string, unknown>;
	toValueMap(): Record<string, ValueLike>;
};

export type ValueLike = {
	readonly kind: LixValue["kind"];
	asJson(): unknown;
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
