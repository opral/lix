/* eslint-disable @typescript-eslint/no-unused-vars */
import { assertType, test } from "vitest";
test("json schema type of a detected change", () => {
    const MockChangeSchema = {
        key: "mock",
        type: "json",
        schema: {
            type: "object",
            properties: {
                name: { type: "string" },
                age: { type: "number" },
                location: { type: "object" },
            },
            required: ["name", "age", "location"],
        },
    };
    const change = {
        entity_id: "123",
        schema: MockChangeSchema,
        snapshot: {
            name: "John",
            age: 5,
            location: {
                city: "New York",
                country: "USA",
            },
        },
    };
    assertType(change);
});
test("file.data is potentially undefined", () => {
    const plugin = {
        key: "plugin1",
        applyChanges: async ({ file }) => {
            assertType(file.data);
            return { fileData: new Uint8Array() };
        },
    };
});
//# sourceMappingURL=lix-plugin.test-d.js.map