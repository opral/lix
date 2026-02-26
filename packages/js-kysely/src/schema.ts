import {
	LixAccountSchema,
	LixActiveAccountSchema,
	LixActiveVersionSchema,
	LixChangeAuthorSchema,
	LixChangeSchema,
	LixChangeSetElementSchema,
	LixChangeSetSchema,
	LixCommitEdgeSchema,
	LixCommitSchema,
	LixDirectoryDescriptorSchema,
	LixEntityLabelSchema,
	LixFileDescriptorSchema,
	LixKeyValueSchema,
	LixLabelSchema,
	LixStoredSchemaSchema,
	LixVersionDescriptorSchema,
} from "@lix-js/sdk";
import type { Generated } from "kysely";
import type { FromSchema, JSONSchema } from "json-schema-to-ts";

type LixPropertySchema = JSONSchema & {
	"x-lix-default"?: string;
};

type LixSchemaDefinition = JSONSchema & {
	type: "object";
	additionalProperties: false;
	properties?: Record<string, LixPropertySchema>;
};

export type LixGenerated<T> = T & {
	readonly __lixGenerated?: true;
};

type IsLixGenerated<T> = T extends { readonly __lixGenerated?: true }
	? true
	: false;

type ExtractFromGenerated<T> = T extends LixGenerated<infer U> ? U : T;

type IsNever<T> = [T] extends [never] ? true : false;
type IsAny<T> = 0 extends 1 & T ? true : false;

type TransformEmptyObject<T> =
	IsAny<T> extends true
		? any
		: IsNever<T> extends true
			? never
			: T extends object
				? keyof T extends never
					? Record<string, any>
					: T
				: T;

type IsEmptyObjectSchema<P> = P extends { type: "object" }
	? P extends { properties: any }
		? false
		: true
	: false;

type GetNullablePart<P> = P extends { nullable: true } ? null : never;

type PropertyHasDefault<P> = P extends { "x-lix-default": any }
	? true
	: P extends { default: any }
		? true
		: false;

type ApplyLixGenerated<TSchema extends LixSchemaDefinition> = TSchema extends {
	properties: infer Props;
}
	? {
			[K in keyof FromSchema<TSchema>]: K extends keyof Props
				? PropertyHasDefault<Props[K]> extends true
					? LixGenerated<TransformEmptyObject<FromSchema<TSchema>[K]>>
					: IsEmptyObjectSchema<Props[K]> extends true
						? Record<string, any> | GetNullablePart<Props[K]>
						: TransformEmptyObject<FromSchema<TSchema>[K]>
				: TransformEmptyObject<FromSchema<TSchema>[K]>;
		}
	: never;

export type FromLixSchemaDefinition<T extends LixSchemaDefinition> =
	ApplyLixGenerated<T>;

type ToKysely<T> = {
	[K in keyof T]: IsLixGenerated<T[K]> extends true
		? Generated<ExtractFromGenerated<T[K]>>
		: T[K];
};

type EntityStateColumns = {
	lixcol_entity_id: LixGenerated<string>;
	lixcol_schema_key: LixGenerated<string>;
	lixcol_file_id: LixGenerated<string>;
	lixcol_plugin_key: LixGenerated<string>;
	lixcol_inherited_from_version_id: LixGenerated<string | null>;
	lixcol_created_at: LixGenerated<string>;
	lixcol_updated_at: LixGenerated<string>;
	lixcol_change_id: LixGenerated<string>;
	lixcol_untracked: LixGenerated<boolean>;
	lixcol_commit_id: LixGenerated<string>;
	lixcol_writer_key: LixGenerated<string | null>;
};

type EntityStateByVersionColumns = EntityStateColumns & {
	lixcol_version_id: LixGenerated<string>;
	lixcol_metadata: LixGenerated<Record<string, any> | null>;
};

type EntityStateHistoryColumns = {
	lixcol_entity_id: LixGenerated<string>;
	lixcol_schema_key: LixGenerated<string>;
	lixcol_file_id: LixGenerated<string>;
	lixcol_plugin_key: LixGenerated<string>;
	lixcol_schema_version: LixGenerated<string>;
	lixcol_change_id: LixGenerated<string>;
	lixcol_commit_id: LixGenerated<string>;
	lixcol_root_commit_id: LixGenerated<string>;
	lixcol_depth: LixGenerated<number>;
	lixcol_metadata: LixGenerated<Record<string, any> | null>;
};

type EntityStateView<T> = T & EntityStateColumns;
type EntityStateByVersionView<T> = T & EntityStateByVersionColumns;
type EntityStateHistoryView<T> = T & EntityStateHistoryColumns;

type EntityViews<
	TSchema extends LixSchemaDefinition,
	TViewName extends string,
	TOverride = object,
> = {
	[K in TViewName]: ToKysely<
		EntityStateView<FromLixSchemaDefinition<TSchema> & TOverride>
	>;
} & {
	[K in `${TViewName}_by_version`]: ToKysely<
		EntityStateByVersionView<FromLixSchemaDefinition<TSchema> & TOverride>
	>;
} & {
	[K in `${TViewName}_history`]: ToKysely<
		EntityStateHistoryView<FromLixSchemaDefinition<TSchema> & TOverride>
	>;
};

type StateByVersionView = {
	entity_id: string;
	schema_key: string;
	file_id: string;
	plugin_key: string;
	snapshot_content: Record<string, any>;
	schema_version: string;
	version_id: string;
	created_at: Generated<string>;
	updated_at: Generated<string>;
	inherited_from_version_id: string | null;
	change_id: Generated<string>;
	untracked: Generated<boolean>;
	commit_id: Generated<string>;
	writer_key: string | null;
	metadata: Generated<Record<string, any> | null>;
};

type StateView = Omit<StateByVersionView, "version_id">;

type StateWithTombstonesView = {
	entity_id: string;
	schema_key: string;
	file_id: string;
	plugin_key: string;
	snapshot_content: Record<string, any> | null;
	schema_version: string;
	version_id: string;
	created_at: Generated<string>;
	updated_at: Generated<string>;
	inherited_from_version_id: string | null;
	change_id: Generated<string>;
	untracked: Generated<boolean>;
	commit_id: Generated<string>;
	writer_key: string | null;
	metadata: Generated<Record<string, any> | null>;
};

type StateHistoryView = {
	entity_id: string;
	schema_key: string;
	file_id: string;
	plugin_key: string;
	snapshot_content: Record<string, any>;
	metadata: Record<string, any> | null;
	schema_version: string;
	change_id: string;
	commit_id: string;
	root_commit_id: string;
	depth: number;
};

type LixActiveVersion = FromLixSchemaDefinition<typeof LixActiveVersionSchema>;
type LixKeyValue = FromLixSchemaDefinition<typeof LixKeyValueSchema> & {
	value: any;
};

type ChangeView = ToKysely<
	FromLixSchemaDefinition<typeof LixChangeSchema> & {
		metadata: Record<string, any> | null;
		snapshot_content: Record<string, any> | null;
	}
>;

type DirectoryDescriptorView = ToKysely<
	EntityStateView<
		FromLixSchemaDefinition<typeof LixDirectoryDescriptorSchema> & {
			path: LixGenerated<string>;
		}
	>
>;

type DirectoryDescriptorByVersionView = ToKysely<
	EntityStateByVersionView<
		FromLixSchemaDefinition<typeof LixDirectoryDescriptorSchema> & {
			path: LixGenerated<string>;
		}
	>
>;

type DirectoryDescriptorHistoryView = ToKysely<
	EntityStateHistoryView<
		FromLixSchemaDefinition<typeof LixDirectoryDescriptorSchema> & {
			path: LixGenerated<string>;
		}
	>
>;

export type LixDatabaseSchema = {
	active_account: EntityViews<
		typeof LixActiveAccountSchema,
		"active_account"
	>["active_account"];
	active_version: ToKysely<LixActiveVersion>;

	state: StateView;
	state_by_version: StateByVersionView;
	state_with_tombstones: StateWithTombstonesView;
	state_history: StateHistoryView;

	change: ChangeView;
	directory: DirectoryDescriptorView;
	directory_by_version: DirectoryDescriptorByVersionView;
	directory_history: DirectoryDescriptorHistoryView;
} & EntityViews<
	typeof LixKeyValueSchema,
	"key_value",
	{ value: LixKeyValue["value"] }
> &
	EntityViews<
		typeof LixKeyValueSchema,
		"lix_key_value",
		{ value: LixKeyValue["value"] }
> &
	EntityViews<typeof LixAccountSchema, "account"> &
	EntityViews<typeof LixChangeSetSchema, "change_set"> &
	EntityViews<typeof LixChangeSetElementSchema, "change_set_element"> &
	EntityViews<typeof LixChangeAuthorSchema, "change_author"> &
	EntityViews<
		typeof LixFileDescriptorSchema,
		"file",
		{
			data: Uint8Array;
			path: LixGenerated<string>;
			directory_id: LixGenerated<string | null>;
			name: LixGenerated<string>;
			extension: LixGenerated<string | null>;
		}
	> &
	EntityViews<typeof LixLabelSchema, "label"> &
	EntityViews<typeof LixEntityLabelSchema, "entity_label"> &
	EntityViews<typeof LixStoredSchemaSchema, "stored_schema", { value: any }> &
	EntityViews<
		typeof LixVersionDescriptorSchema,
		"version",
		{ commit_id: LixGenerated<string>; working_commit_id: LixGenerated<string> }
	> &
	EntityViews<typeof LixCommitSchema, "commit"> &
	EntityViews<typeof LixCommitEdgeSchema, "commit_edge">;
