// eslint-disable-next-line @typescript-eslint/ban-ts-comment
// @ts-nocheck
/**
 * THIS WAS A PROTOYPE FOR DETECTING JSONB COLUMNS
 * INSTEAD OF RELYING ON HEURISTICS BUT WAS TOO COMPLEX
 * FOR v1 of lix.
 *
 * Code still exists for future reference.
 *
 * https://github.com/opral/lix-sdk/issues/145
 */
import { AliasNode, FromNode, SelectionNode, } from "kysely";
export function ParseJsonBPluginV2(jsonbColumns) {
    const data = new WeakMap();
    return {
        transformResult: async (args) => {
            const jsonbColumNames = data.get(args.queryId)?.jsonbColumNames;
            if (!jsonbColumNames)
                return args.result;
            for (const row of args.result.rows) {
                for (const col of jsonbColumNames) {
                    row[col] = JSON.parse(row[col]);
                }
            }
            return args.result;
        },
        transformQuery: (args) => {
            const query = args.node;
            if (query.kind !== "SelectQueryNode") {
                return query;
            }
            // Transform the selections (handles both select and selectAll)
            const { transformedSelections, jsonbColumNames } = transformSelections(query.selections, query.from?.froms ?? [], jsonbColumns);
            // Store the transformed column aliases for result parsing
            data.set(args.queryId, { jsonbColumNames });
            // Return the updated query node
            return {
                ...query,
                selections: transformedSelections,
            };
        },
    };
}
/**
 * Transforms selections to include JSONB column parsing, respecting aliases.
 */
function transformSelections(selections, fromNodes, jsonbColumns) {
    if (!fromNodes)
        return { transformedSelections: selections ?? [], jsonbColumNames: [] };
    const jsonbSelections = [];
    const jsonbColumNames = [];
    // Extract the fallback table name if there's only one table in fromNodes
    let fallbackTableName;
    let fallbackTableAlias;
    if (fromNodes.length === 1) {
        const node = fromNodes[0];
        if (node.kind === "TableNode") {
            fallbackTableName = node.table?.identifier.name;
        }
        else if (node.kind === "AliasNode") {
            fallbackTableName = node.node.table?.identifier.name;
            fallbackTableAlias = node.alias?.name;
        }
    }
    // Iterate over the selection nodes
    for (const selection of selections ?? []) {
        let tableName;
        let columnName;
        let aliasName;
        if (selection.selection?.kind === "ReferenceNode") {
            const potentialTableName = selection.selection.table?.table?.identifier?.name || fallbackTableName;
            const alias = fromNodes.find((n) => n.kind === "AliasNode" && n.alias.name === potentialTableName);
            if (alias) {
                tableName = alias.node.table?.identifier.name;
                aliasName = alias.alias.name;
            }
            else {
                tableName = potentialTableName;
            }
            columnName = selection.selection.column?.column?.name;
            const isAlias = fromNodes.some((n) => n.kind === "AliasNode" && n.alias.name === tableName);
            // If the table name is an alias, find the real name
            if (isAlias) {
                const aliasNode = fromNodes.find((n) => n.kind === "AliasNode" && n.alias.name === tableName);
                tableName = aliasNode.node.table?.identifier.name;
            }
        }
        else if (selection.selection?.kind === "AliasNode") {
            // AliasNode: Extract alias and reference details
            aliasName = selection.selection.alias?.name;
            if (selection.selection.node?.kind === "ReferenceNode") {
                tableName =
                    selection.selection.node?.table?.table?.identifier?.name ||
                        fallbackTableName;
                columnName = selection.selection.node?.column?.column?.name;
            }
        }
        else if (selection.selection.kind === "SelectAllNode") {
            tableName =
                selection.selection.table?.table?.identifier?.name || fallbackTableName;
            aliasName = selection.selection.alias?.name || fallbackTableAlias;
        }
        if (!tableName)
            continue;
        const jsonbCols = jsonbColumns[tableName] ?? [];
        for (const col of jsonbCols) {
            const effectiveAlias = aliasName ?? columnName ?? col;
            // Track the column key for result transformation
            jsonbColumNames.push(effectiveAlias);
            // Add the JSON transformation
            jsonbSelections.push({
                kind: "SelectionNode",
                selection: {
                    kind: "AliasNode",
                    node: {
                        kind: "FunctionNode",
                        func: "json",
                        arguments: [
                            {
                                kind: "ReferenceNode",
                                table: {
                                    kind: "TableNode",
                                    table: {
                                        kind: "SchemableIdentifierNode",
                                        identifier: {
                                            kind: "IdentifierNode",
                                            name: tableName,
                                        },
                                    },
                                },
                                column: {
                                    kind: "ColumnNode",
                                    column: { kind: "IdentifierNode", name: effectiveAlias },
                                },
                            },
                        ],
                    },
                    alias: { kind: "IdentifierNode", name: effectiveAlias },
                },
            });
        }
    }
    // Merge existing selections with new JSONB transformations
    return {
        transformedSelections: [...(selections ?? []), ...jsonbSelections],
        jsonbColumNames,
    };
}
//# sourceMappingURL=parse-jsonb-plugin-v2.js.map