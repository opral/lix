#![allow(dead_code)]

//! SQL-backed adapter slot for binary CAS persistence.
//!
//! This module is the intended home for `BinaryCasReadStore` and
//! `BinaryCasWriteStore` implementations that still rely on `LixBackend`,
//! `LixBackendTransaction`, or lower `backend/*` helpers during the MVP.

use crate::{LixBackend, LixBackendTransaction};

/// Thin read adapter over a committed backend view.
pub(crate) struct SqlBinaryCasReadStore<'a> {
    backend: &'a dyn LixBackend,
}

impl<'a> SqlBinaryCasReadStore<'a> {
    pub(crate) fn new(backend: &'a dyn LixBackend) -> Self {
        Self { backend }
    }

    pub(crate) fn backend(&self) -> &'a dyn LixBackend {
        self.backend
    }
}

/// Thin write adapter over the current write unit.
pub(crate) struct SqlBinaryCasWriteStore<'a> {
    transaction: &'a mut dyn LixBackendTransaction,
}

impl<'a> SqlBinaryCasWriteStore<'a> {
    pub(crate) fn new(transaction: &'a mut dyn LixBackendTransaction) -> Self {
        Self { transaction }
    }

    pub(crate) fn transaction(&mut self) -> &mut dyn LixBackendTransaction {
        self.transaction
    }
}
