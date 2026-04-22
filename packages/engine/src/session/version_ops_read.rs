pub(crate) type VersionOpsReadRef<'a> = &'a mut (dyn crate::QueryExecutor + 'a);
