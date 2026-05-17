use crate::backend::conformance::{
    baseline, model_based, persistence, projection, pushdown, scan, write, BackendFactory,
};

pub type ConformanceResult = Result<(), String>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ConformanceReport {
    pub tests: Vec<ConformanceTest>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConformanceTest {
    pub name: &'static str,
    pub status: ConformanceStatus,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConformanceStatus {
    Passed,
    Failed(String),
    Pending,
}

pub fn run_backend_conformance<F>(factory: &F) -> ConformanceReport
where
    F: BackendFactory,
{
    let mut report = ConformanceReport::default();

    baseline::register(&mut report, factory);
    model_based::register(&mut report, factory);
    if !factory.config().ephemeral {
        persistence::register(&mut report, factory);
    }
    scan::register(&mut report, factory);
    write::register(&mut report, factory);
    projection::register(&mut report, factory);
    pushdown::register(&mut report, factory);

    report
}

impl ConformanceReport {
    pub(crate) fn add_pending(&mut self, name: &'static str) {
        self.tests.push(ConformanceTest {
            name,
            status: ConformanceStatus::Pending,
        });
    }

    pub(crate) fn run(&mut self, name: &'static str, test: impl FnOnce() -> ConformanceResult) {
        let status = match test() {
            Ok(()) => ConformanceStatus::Passed,
            Err(error) => ConformanceStatus::Failed(error),
        };
        self.tests.push(ConformanceTest { name, status });
    }

    pub fn failed(&self) -> impl Iterator<Item = &ConformanceTest> {
        self.tests
            .iter()
            .filter(|test| matches!(test.status, ConformanceStatus::Failed(_)))
    }

    pub fn pending(&self) -> impl Iterator<Item = &ConformanceTest> {
        self.tests
            .iter()
            .filter(|test| matches!(test.status, ConformanceStatus::Pending))
    }

    pub fn assert_no_failures(&self) {
        let problems = self
            .failed()
            .map(|test| match &test.status {
                ConformanceStatus::Failed(error) => format!("{}: {error}", test.name),
                _ => unreachable!("failed iterator only returns failed tests"),
            })
            .chain(
                self.pending()
                    .map(|test| format!("{}: pending capability conformance", test.name)),
            )
            .collect::<Vec<_>>();
        assert!(
            problems.is_empty(),
            "backend conformance problems:\n{}",
            problems.join("\n")
        );
    }
}
