//! Pure acceptance model for the historical point/scan fail-closed boundary.
//! This file has no Lix or adapter dependency and is not production code.

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Fixture {
    ValidAbsentKey,
    MissingCommitCatalog,
    MissingRootObject,
    WrongKindRoot,
    MalformedCatalog,
    MalformedRoot,
    ValidTombstone,
    ValidNull,
    ValidValue,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cell {
    Absent,
    Tombstone,
    Null,
    Value,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    Corruption,
    ReadProtocol,
}

#[derive(Debug, Eq, PartialEq)]
pub struct ReadTrace {
    pub primary_read_id: u64,
    pub operation_read_ids: Vec<u64>,
    pub begin_read_count: usize,
    pub retry_count: usize,
    pub fallback_count: usize,
    pub cache_hit_count: usize,
}

impl ReadTrace {
    pub fn one_read(read_id: u64) -> Self {
        Self {
            primary_read_id: read_id,
            operation_read_ids: vec![read_id],
            begin_read_count: 1,
            retry_count: 0,
            fallback_count: 0,
            cache_hit_count: 0,
        }
    }

    fn is_coherent(&self) -> bool {
        self.begin_read_count == 1
            && self.retry_count == 0
            && self.fallback_count == 0
            && self.cache_hit_count == 0
            && self
                .operation_read_ids
                .iter()
                .all(|read_id| *read_id == self.primary_read_id)
    }
}

/// Desired post-correction semantics: only a validated root may report a
/// missing key, and the read trace cannot hide a second source of truth.
pub fn resolve(fixture: Fixture, trace: &ReadTrace) -> Result<Cell, ErrorKind> {
    if !trace.is_coherent() {
        return Err(ErrorKind::ReadProtocol);
    }
    match fixture {
        Fixture::ValidAbsentKey => Ok(Cell::Absent),
        Fixture::ValidTombstone => Ok(Cell::Tombstone),
        Fixture::ValidNull => Ok(Cell::Null),
        Fixture::ValidValue => Ok(Cell::Value),
        Fixture::MissingCommitCatalog
        | Fixture::MissingRootObject
        | Fixture::WrongKindRoot
        | Fixture::MalformedCatalog
        | Fixture::MalformedRoot => Err(ErrorKind::Corruption),
    }
}

#[cfg(test)]
mod tests {
    use super::{resolve, Cell, ErrorKind, Fixture, ReadTrace};

    #[test]
    fn only_valid_root_can_report_absence() {
        let trace = ReadTrace::one_read(7);
        assert_eq!(resolve(Fixture::ValidAbsentKey, &trace), Ok(Cell::Absent));
        for fixture in [
            Fixture::MissingCommitCatalog,
            Fixture::MissingRootObject,
            Fixture::WrongKindRoot,
            Fixture::MalformedCatalog,
            Fixture::MalformedRoot,
        ] {
            assert_eq!(resolve(fixture, &trace), Err(ErrorKind::Corruption));
        }
    }

    #[test]
    fn cells_remain_distinct() {
        let trace = ReadTrace::one_read(8);
        assert_eq!(resolve(Fixture::ValidTombstone, &trace), Ok(Cell::Tombstone));
        assert_eq!(resolve(Fixture::ValidNull, &trace), Ok(Cell::Null));
        assert_eq!(resolve(Fixture::ValidValue, &trace), Ok(Cell::Value));
    }

    #[test]
    fn a_second_read_or_fallback_is_not_a_repair() {
        let mut trace = ReadTrace::one_read(9);
        trace.operation_read_ids.push(10);
        assert_eq!(
            resolve(Fixture::ValidAbsentKey, &trace),
            Err(ErrorKind::ReadProtocol)
        );
        let mut trace = ReadTrace::one_read(11);
        trace.fallback_count = 1;
        assert_eq!(
            resolve(Fixture::MissingCommitCatalog, &trace),
            Err(ErrorKind::ReadProtocol)
        );
    }
}
