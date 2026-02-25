import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { LixDatabaseSchema } from "./schema.js";

type LixEntityCanonical = {
	schema_key: string;
	file_id: string;
	entity_id: string;
};

type LixEntity = {
	lixcol_schema_key: string;
	lixcol_file_id: string;
	lixcol_entity_id: string;
};

const CANONICAL_TABLES = [
	"state",
	"state_by_version",
	"entity_label",
	"entity_conversation",
	"entity_conversation_by_version",
] as const;

export function ebEntity<
	TB extends keyof LixDatabaseSchema = keyof LixDatabaseSchema,
>(entityType?: TB) {
	const isCanonicalTable = entityType
		? CANONICAL_TABLES.includes(entityType as any)
		: undefined;

	const detectColumnType = (
		entity: LixEntity | LixEntityCanonical,
	): boolean => {
		return (
			"entity_id" in entity && "schema_key" in entity && "file_id" in entity
		);
	};

	const getColumnNames = (entity?: LixEntity | LixEntityCanonical) => {
		if (entityType !== undefined) {
			return {
				entityIdCol: isCanonicalTable ? "entity_id" : "lixcol_entity_id",
				schemaKeyCol: isCanonicalTable ? "schema_key" : "lixcol_schema_key",
				fileIdCol: isCanonicalTable ? "file_id" : "lixcol_file_id",
			};
		}

		if (entity) {
			const useCanonical = detectColumnType(entity);
			return {
				entityIdCol: useCanonical ? "entity_id" : "lixcol_entity_id",
				schemaKeyCol: useCanonical ? "schema_key" : "lixcol_schema_key",
				fileIdCol: useCanonical ? "file_id" : "lixcol_file_id",
			};
		}

		return {
			entityIdCol: "lixcol_entity_id",
			schemaKeyCol: "lixcol_schema_key",
			fileIdCol: "lixcol_file_id",
		};
	};

	return {
		hasLabel(
			label: { id: string; name?: string } | { name: string; id?: string },
		) {
			return (
				eb: ExpressionBuilder<LixDatabaseSchema, TB>,
			): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
				const { entityIdCol } = getColumnNames();
				const columnRef = entityType
					? `${entityType}.${entityIdCol}`
					: entityIdCol;
				return eb(eb.ref(columnRef as any), "in", (subquery: any) =>
					subquery
						.selectFrom("entity_label")
						.innerJoin("label", "label.id", "entity_label.label_id")
						.select("entity_label.entity_id")
						.$if("name" in label, (qb: any) =>
							qb.where("label.name", "=", label.name!),
						)
						.$if("id" in label, (qb: any) =>
							qb.where("label.id", "=", label.id!),
						),
				);
			};
		},
		equals(entity: LixEntity | LixEntityCanonical) {
			return (
				eb: ExpressionBuilder<LixDatabaseSchema, TB>,
			): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
				const targetEntityId =
					"entity_id" in entity ? entity.entity_id : entity.lixcol_entity_id;
				const targetSchemaKey =
					"schema_key" in entity ? entity.schema_key : entity.lixcol_schema_key;
				const targetFileId =
					"file_id" in entity ? entity.file_id : entity.lixcol_file_id;

				const { entityIdCol, schemaKeyCol, fileIdCol } = getColumnNames(entity);
				const entityIdRef = entityType
					? `${entityType}.${entityIdCol}`
					: entityIdCol;
				const schemaKeyRef = entityType
					? `${entityType}.${schemaKeyCol}`
					: schemaKeyCol;
				const fileIdRef = entityType ? `${entityType}.${fileIdCol}` : fileIdCol;

				return eb.and([
					eb(eb.ref(entityIdRef as any), "=", targetEntityId),
					eb(eb.ref(schemaKeyRef as any), "=", targetSchemaKey),
					eb(eb.ref(fileIdRef as any), "=", targetFileId),
				]);
			};
		},
		in(entities: Array<LixEntityCanonical | LixEntity>) {
			return (
				eb: ExpressionBuilder<LixDatabaseSchema, TB>,
			): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
				if (entities.length === 0) {
					return eb.val(false);
				}

				const entityIds = entities.map((entity) =>
					"entity_id" in entity ? entity.entity_id : entity.lixcol_entity_id,
				);

				const { entityIdCol } = getColumnNames(entities[0]);
				const columnRef = entityType
					? `${entityType}.${entityIdCol}`
					: entityIdCol;

				return eb(eb.ref(columnRef as any), "in", entityIds);
			};
		},
	};
}
