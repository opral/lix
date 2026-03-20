export function applyKeyValueDatabaseSchema(sqlite) {
    return sqlite.exec `
	CREATE TABLE IF NOT EXISTS key_value (
		key TEXT PRIMARY KEY,
		value TEXT NOT NULL,

		-- Options
		skip_change_control INT DEFAULT FALSE
	) STRICT;

	INSERT OR IGNORE INTO key_value (key, value)
	VALUES ('lix_id', nano_id(18));

	-- default value for lix sync to false
	-- if not exist to remove conditional logic
	-- if the key exists or not
	INSERT OR IGNORE INTO key_value (key, value, skip_change_control)
	VALUES ('lix_sync', 'false', 1);
`;
}
//# sourceMappingURL=database-schema.js.map