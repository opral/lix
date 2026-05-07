import type { ExpressionBuilder, ExpressionWrapper, SqlBool } from "kysely";
import type { LixDatabaseSchema } from "./schema.js";

type LixEntityId = string[];

type LixEntityCanonical = {
	schema_key: string;
	file_id: string | null;
	entity_id: LixEntityId;
};

type LixEntity = {
	lixcol_schema_key: string;
	lixcol_file_id: string | null;
	lixcol_entity_id: LixEntityId;
};

const CANONICAL_TABLES = [
	"lix_state",
	"lix_state_by_version",
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

	const getColumnRefs = (entity?: LixEntity | LixEntityCanonical) => {
		const { entityIdCol, schemaKeyCol, fileIdCol } = getColumnNames(entity);
		return {
			entityIdRef: entityType ? `${entityType}.${entityIdCol}` : entityIdCol,
			schemaKeyRef: entityType ? `${entityType}.${schemaKeyCol}` : schemaKeyCol,
			fileIdRef: entityType ? `${entityType}.${fileIdCol}` : fileIdCol,
		};
	};

	const getTargetValues = (entity: LixEntity | LixEntityCanonical) => {
		return {
			targetEntityId:
				"entity_id" in entity ? entity.entity_id : entity.lixcol_entity_id,
			targetSchemaKey:
				"schema_key" in entity ? entity.schema_key : entity.lixcol_schema_key,
			targetFileId: "file_id" in entity ? entity.file_id : entity.lixcol_file_id,
		};
	};

	const equalsExpression = (
		eb: ExpressionBuilder<LixDatabaseSchema, TB>,
		entity: LixEntity | LixEntityCanonical,
	): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
		const { targetEntityId, targetSchemaKey, targetFileId } =
			getTargetValues(entity);
		const { entityIdRef, schemaKeyRef, fileIdRef } = getColumnRefs(entity);
		return eb.and([
			eb(eb.ref(entityIdRef as any), "=", targetEntityId),
			eb(eb.ref(schemaKeyRef as any), "=", targetSchemaKey),
			targetFileId === null
				? eb(eb.ref(fileIdRef as any), "is", null)
				: eb(eb.ref(fileIdRef as any), "=", targetFileId),
		]);
	};

	return {
		hasLabel(
			label: { id: string; name?: string } | { name: string; id?: string },
		) {
			return (
				eb: ExpressionBuilder<LixDatabaseSchema, TB>,
			): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
				const { entityIdRef, schemaKeyRef, fileIdRef } = getColumnRefs();
				const labelQuery = eb
					.selectFrom("lix_label_assignment" as any)
					.innerJoin(
						"lix_label" as any,
						"lix_label.id" as any,
						"lix_label_assignment.label_id" as any,
					) as any;
				return eb.exists(
					labelQuery
						.select("lix_label_assignment.target_entity_id" as any)
						.whereRef(
							"lix_label_assignment.target_entity_id" as any,
							"=",
							entityIdRef as any,
						)
						.whereRef(
							"lix_label_assignment.target_schema_key" as any,
							"=",
							schemaKeyRef as any,
						)
						.whereRef(
							"lix_label_assignment.target_file_id" as any,
							"is",
							fileIdRef as any,
						)
						.$if("name" in label, (qb: any) =>
							qb.where("lix_label.name", "=", label.name!),
						)
						.$if("id" in label, (qb: any) =>
							qb.where("lix_label.id", "=", label.id!),
						),
				);
			};
		},
		equals(entity: LixEntity | LixEntityCanonical) {
			return (
				eb: ExpressionBuilder<LixDatabaseSchema, TB>,
			): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
				return equalsExpression(eb, entity);
			};
		},
		in(entities: Array<LixEntityCanonical | LixEntity>) {
			return (
				eb: ExpressionBuilder<LixDatabaseSchema, TB>,
			): ExpressionWrapper<LixDatabaseSchema, TB, SqlBool> => {
				if (entities.length === 0) {
					return eb.val(false);
				}

				return eb.or(entities.map((entity) => equalsExpression(eb, entity)));
			};
		},
	};
}
