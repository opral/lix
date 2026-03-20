import { OnConflictNode, OperationNodeTransformer, sql, ValueListNode, ValueNode, ValuesNode, } from "kysely";
export function SerializeJsonBPlugin() {
    const parseJsonTransformer = new SerializeJsonbTransformer();
    return {
        transformResult: async (args) => args.result,
        transformQuery(args) {
            if (args.node.kind === "InsertQueryNode" ||
                args.node.kind === "UpdateQueryNode") {
                const result = parseJsonTransformer.transformNode(args.node);
                return result;
            }
            return args.node;
        },
    };
}
class SerializeJsonbTransformer extends OperationNodeTransformer {
    transformOnConflict(node) {
        return super.transformOnConflict({
            ...node,
            updates: node.updates?.map((updateItem) => {
                if (updateItem.kind !== "ColumnUpdateNode") {
                    return updateItem;
                }
                return {
                    kind: "ColumnUpdateNode",
                    column: updateItem.column,
                    value: this.transformValue(
                    // @ts-expect-error - type mismatch
                    updateItem.value),
                };
            }),
        });
    }
    transformValue(node) {
        const { value } = node;
        const serializedValue = maybeSerializeJson(value);
        if (value === serializedValue) {
            return node;
        }
        // @ts-expect-error - type mismatch
        return sql `jsonb(${serializedValue})`.toOperationNode();
    }
    /**
     * Transforms the value list node by replacing all JSON objects with `jsonb` function calls.
     */
    transformValueList(node) {
        return super.transformValueList({
            ...node,
            values: node.values.map((listNodeItem) => {
                if (listNodeItem.kind !== "ValueNode") {
                    return listNodeItem;
                }
                // @ts-expect-error - type mismatch
                const { value } = listNodeItem;
                const serializedValue = maybeSerializeJson(value);
                if (value === serializedValue) {
                    return listNodeItem;
                }
                return sql `jsonb(${serializedValue})`.toOperationNode();
            }),
        });
    }
    /**
     * changes PrimitiveValueListNode to ValueListNode to allow kysely to process objects
     */
    transformValues(node) {
        return super.transformValues({
            ...node,
            values: node.values.map((valueItemNode) => {
                if (valueItemNode.kind !== "PrimitiveValueListNode") {
                    return valueItemNode;
                }
                // change PrimitiveValueListNode to ValueListNode
                return {
                    kind: "ValueListNode",
                    values: valueItemNode.values.map((value) => ({
                        kind: "ValueNode",
                        value,
                    })),
                };
            }),
        });
    }
}
function maybeSerializeJson(value) {
    if (
    // binary data
    value instanceof ArrayBuffer ||
        // uint8array, etc
        ArrayBuffer.isView(value) ||
        value === null ||
        value === undefined) {
        return value;
    }
    else if (typeof value === "object" || Array.isArray(value)) {
        return JSON.stringify(value);
    }
    return value;
}
//# sourceMappingURL=serialize-jsonb-plugin.js.map