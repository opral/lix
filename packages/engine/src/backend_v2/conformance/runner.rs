use crate::backend_v2::conformance::{
    baseline, projection, pushdown, scan, write, BackendFactory,
};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConformanceReport {
    pub tests: Vec<ConformanceTest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConformanceTest {
    pub name: &'static str,
}

pub fn run_backend_conformance<F>(factory: &F) -> ConformanceReport
where
    F: BackendFactory,
{
    let mut report = ConformanceReport::default();

    baseline::register(&mut report);
    scan::register(&mut report, factory);
    write::register(&mut report, factory);
    projection::register(&mut report, factory);
    pushdown::register(&mut report, factory);

    report
}

impl ConformanceReport {
    pub(crate) fn add(&mut self, name: &'static str) {
        self.tests.push(ConformanceTest { name });
    }
}
