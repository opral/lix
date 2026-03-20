import { jsonSha256 } from "./json-sha-256.js";
/**
 *
 * Util function for tests that creates a snapshot that looks like one you got returned from the database after inserting
 *
 */
export function mockJsonSnapshot(content) {
    return {
        id: jsonSha256(content),
        content: content,
    };
}
//# sourceMappingURL=mock-json-snapshot.js.map