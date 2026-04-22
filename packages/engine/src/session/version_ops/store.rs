pub(crate) type VersionOpsBackendRef<'a> = &'a (dyn crate::LixBackend + 'a);
pub(crate) type VersionOpsTransactionRef<'a> = &'a mut (dyn crate::LixBackendTransaction + 'a);
