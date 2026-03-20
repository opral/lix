import { test, assertType } from "vitest";
test("a json change schema should be infer the properties", () => {
    const jsonChangeSchema = {
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
    const snapshot = {
        name: "John",
        age: 5,
        location: {
            city: "New York",
            country: "USA",
        },
    };
    assertType(snapshot);
});
test("a blob change schema should be infer the properties", () => {
    const blobChangeSchema = {
        key: "mock",
        type: "blob",
    };
    const snapshot = new ArrayBuffer(0);
    assertType(snapshot);
});
//# sourceMappingURL=types.test-d.js.map