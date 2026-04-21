#![allow(dead_code)]

//! SQL-backed adapter slot for canonical persistence.
//!
//! This module is the intended home for `CanonicalReadStore` and
//! `CanonicalWriteStore` implementations that still rely on `LixBackend`,
//! `LixBackendTransaction`, or lower `backend/*` helpers during the MVP.

use crate::backend::QueryExecutor;
use crate::{LixBackend, LixBackendTransaction};

/// Thin read adapter over canonical read backends/executors.
pub(crate) struct SqlCanonicalReadStore<'a> {
    backend: &'a dyn LixBackend,
}

impl<'a> SqlCanonicalReadStore<'a> {
    pub(crate) fn new(backend: &'a dyn LixBackend) -> Self {
        Self { backend }
    }

    pub(crate) fn backend(&self) -> &'a dyn LixBackend {
        self.backend
    }
}

/// Thin executor-shaped adapter for canonical reads that must run inside an
/// existing unit of work.
pub(crate) struct SqlCanonicalExecutorReadStore<'a> {
    executor: &'a mut dyn QueryExecutor,
}

impl<'a> SqlCanonicalExecutorReadStore<'a> {
    pub(crate) fn new(executor: &'a mut dyn QueryExecutor) -> Self {
        Self { executor }
    }

    pub(crate) fn executor(&mut self) -> &mut dyn QueryExecutor {
        self.executor
    }
}

/// Thin write adapter over the current write unit.
pub(crate) struct SqlCanonicalWriteStore<'a> {
    transaction: &'a mut dyn LixBackendTransaction,
}

impl<'a> SqlCanonicalWriteStore<'a> {
    pub(crate) fn new(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self { transaction }
    }

    pub(crate) fn transaction(&mut self) -> &mut dyn LixBackendTransaction {
        self.transaction
    }
}
