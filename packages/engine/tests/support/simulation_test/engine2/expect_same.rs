use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use super::mode::Engine2SimulationMode;

#[derive(Clone)]
pub(super) struct Engine2SimulationAssertions {
    inner: Engine2SimulationAssertionsInner,
}

#[derive(Clone)]
enum Engine2SimulationAssertionsInner {
    Local(LocalExpectSameHandle),
    Shared(SharedExpectSameRun),
}

impl Engine2SimulationAssertions {
    pub(super) fn new_local() -> Self {
        Self {
            inner: Engine2SimulationAssertionsInner::Local(LocalExpectSameHandle::default()),
        }
    }

    pub(super) fn shared(run: SharedExpectSameRun) -> Self {
        Self {
            inner: Engine2SimulationAssertionsInner::Shared(run),
        }
    }

    pub(super) fn start_mode(&self, mode: Engine2SimulationMode) {
        match &self.inner {
            Engine2SimulationAssertionsInner::Local(local) => local.lock().start_mode(mode),
            Engine2SimulationAssertionsInner::Shared(shared) => shared.start_mode(),
        }
    }

    pub(super) fn assert_same(&self, mode: Engine2SimulationMode, label: &str, actual: String) {
        match &self.inner {
            Engine2SimulationAssertionsInner::Local(local) => {
                local.lock().assert_same(mode, label, actual)
            }
            Engine2SimulationAssertionsInner::Shared(shared) => shared.assert_same(label, actual),
        }
    }

    pub(super) fn finish_mode(&self, mode: Engine2SimulationMode) {
        match &self.inner {
            Engine2SimulationAssertionsInner::Local(local) => local.lock().finish_mode(mode),
            Engine2SimulationAssertionsInner::Shared(shared) => shared.finish_mode(),
        }
    }
}

#[derive(Clone, Default)]
struct LocalExpectSameHandle {
    inner: Arc<Mutex<LocalExpectSame>>,
}

impl LocalExpectSameHandle {
    fn lock(&self) -> std::sync::MutexGuard<'_, LocalExpectSame> {
        self.inner
            .lock()
            .expect("engine2 simulation assertions lock poisoned")
    }
}

#[derive(Default)]
struct LocalExpectSame {
    expected: Vec<(String, String)>,
    current_index: usize,
}

impl LocalExpectSame {
    fn start_mode(&mut self, _mode: Engine2SimulationMode) {
        self.current_index = 0;
    }

    fn assert_same(&mut self, mode: Engine2SimulationMode, label: &str, actual: String) {
        match mode {
            Engine2SimulationMode::Base => {
                self.expected.push((label.to_string(), actual));
                self.current_index += 1;
            }
            Engine2SimulationMode::TrackedStateRebuild => {
                let Some((expected_label, expected_value)) = self.expected.get(self.current_index)
                else {
                    panic!(
                        "engine2 simulation assertion '{label}' has no base value at index {}",
                        self.current_index
                    );
                };
                assert_eq!(
                    expected_label, label,
                    "engine2 simulation assertion order changed"
                );
                assert_eq!(
                    expected_value, &actual,
                    "engine2 simulation assertion '{label}' differed"
                );
                self.current_index += 1;
            }
        }
    }

    fn finish_mode(&mut self, mode: Engine2SimulationMode) {
        if mode == Engine2SimulationMode::Base {
            return;
        }
        assert_eq!(
            self.current_index,
            self.expected.len(),
            "engine2 simulation mode did not execute all assert_same checks"
        );
    }
}

#[derive(Clone)]
pub(crate) struct SharedExpectSameRun {
    case_id: String,
    mode: Engine2SimulationMode,
    call_index: Arc<Mutex<usize>>,
    case: Arc<SharedExpectSameCase>,
}

struct SharedExpectSameCase {
    state: Mutex<SharedExpectSameState>,
    condvar: Condvar,
}

#[derive(Default)]
struct SharedExpectSameState {
    base_finished: bool,
    base_failed: bool,
    expected: Vec<(String, String)>,
}

pub(crate) struct SharedExpectSameRunGuard {
    run: SharedExpectSameRun,
    finished: bool,
}

impl SharedExpectSameRun {
    pub(crate) fn new(case_id: &str, mode: Engine2SimulationMode) -> Self {
        static CASES: OnceLock<Mutex<HashMap<String, Arc<SharedExpectSameCase>>>> = OnceLock::new();
        let cases = CASES.get_or_init(|| Mutex::new(HashMap::new()));
        let case = {
            let mut guard = cases
                .lock()
                .expect("engine2 shared expectation registry lock poisoned");
            guard
                .entry(case_id.to_string())
                .or_insert_with(|| {
                    Arc::new(SharedExpectSameCase {
                        state: Mutex::new(SharedExpectSameState::default()),
                        condvar: Condvar::new(),
                    })
                })
                .clone()
        };
        Self {
            case_id: case_id.to_string(),
            mode,
            call_index: Arc::new(Mutex::new(0)),
            case,
        }
    }

    fn start_mode(&self) {}

    fn next_index(&self) -> usize {
        let mut guard = self
            .call_index
            .lock()
            .expect("engine2 shared expectation call index lock poisoned");
        let index = *guard;
        *guard += 1;
        index
    }

    fn call_count(&self) -> usize {
        *self
            .call_index
            .lock()
            .expect("engine2 shared expectation call index lock poisoned")
    }

    fn assert_same(&self, label: &str, actual: String) {
        let index = self.next_index();
        match self.mode {
            Engine2SimulationMode::Base => {
                let mut state = self
                    .case
                    .state
                    .lock()
                    .expect("engine2 shared expectation lock poisoned");
                state.expected.push((label.to_string(), actual));
                self.case.condvar.notify_all();
            }
            Engine2SimulationMode::TrackedStateRebuild => {
                let expected = self.wait_for_expected(index, label);
                assert_eq!(
                    expected.0,
                    label,
                    "simulation_test2 assertion order changed for case `{}` mode `{}` at call #{}",
                    self.case_id,
                    self.mode.name(),
                    index
                );
                assert_eq!(
                    expected.1,
                    actual,
                    "simulation_test2 assert_same `{label}` differed for case `{}` mode `{}`",
                    self.case_id,
                    self.mode.name()
                );
            }
        }
    }

    fn wait_for_expected(&self, index: usize, label: &str) -> (String, String) {
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut state = self
            .case
            .state
            .lock()
            .expect("engine2 shared expectation lock poisoned");
        loop {
            if state.base_failed {
                panic!(
                    "simulation_test2 case `{}` base failed before `{}` could compare call #{}",
                    self.case_id, label, index
                );
            }
            if let Some(expected) = state.expected.get(index) {
                return expected.clone();
            }
            if state.base_finished {
                panic!(
                    "simulation_test2 case `{}` mode `{}` called assert_same one extra time at call #{} ({label})",
                    self.case_id,
                    self.mode.name(),
                    index
                );
            }

            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!(
                    "simulation_test2 timed out waiting for base assert_same call #{} in case `{}`",
                    index, self.case_id
                );
            }
            let (next_state, timeout) = self
                .case
                .condvar
                .wait_timeout(state, remaining)
                .expect("engine2 shared expectation condvar wait poisoned");
            state = next_state;
            if timeout.timed_out() {
                panic!(
                    "simulation_test2 timed out waiting for base assert_same call #{} in case `{}`",
                    index, self.case_id
                );
            }
        }
    }

    fn finish_mode(&self) {
        match self.mode {
            Engine2SimulationMode::Base => self.finish_base(std::thread::panicking()),
            Engine2SimulationMode::TrackedStateRebuild => self.finish_compare(),
        }
    }

    fn finish_base(&self, failed: bool) {
        let mut state = self
            .case
            .state
            .lock()
            .expect("engine2 shared expectation lock poisoned");
        state.base_finished = true;
        state.base_failed = failed;
        self.case.condvar.notify_all();
    }

    fn finish_compare(&self) {
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut state = self
            .case
            .state
            .lock()
            .expect("engine2 shared expectation lock poisoned");
        while !state.base_finished && !state.base_failed {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                panic!(
                    "simulation_test2 timed out waiting for base completion in case `{}`",
                    self.case_id
                );
            }
            let (next_state, timeout) = self
                .case
                .condvar
                .wait_timeout(state, remaining)
                .expect("engine2 shared expectation condvar wait poisoned");
            state = next_state;
            if timeout.timed_out() {
                panic!(
                    "simulation_test2 timed out waiting for base completion in case `{}`",
                    self.case_id
                );
            }
        }
        if state.base_failed {
            panic!(
                "simulation_test2 case `{}` base failed before mode `{}` completed",
                self.case_id,
                self.mode.name()
            );
        }
        assert_eq!(
            self.call_count(),
            state.expected.len(),
            "simulation_test2 mode `{}` for case `{}` did not execute all assert_same checks",
            self.mode.name(),
            self.case_id
        );
    }
}

impl SharedExpectSameRunGuard {
    pub(crate) fn new(run: SharedExpectSameRun) -> Self {
        Self {
            run,
            finished: false,
        }
    }
}

impl Drop for SharedExpectSameRunGuard {
    fn drop(&mut self) {
        if self.finished || self.run.mode != Engine2SimulationMode::Base {
            return;
        }
        self.run.finish_base(std::thread::panicking());
        self.finished = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_expect_same_compares_rebuild_against_base() {
        let mut expect = LocalExpectSame::default();
        expect.start_mode(Engine2SimulationMode::Base);
        expect.assert_same(Engine2SimulationMode::Base, "value", "1".to_string());
        expect.finish_mode(Engine2SimulationMode::Base);

        expect.start_mode(Engine2SimulationMode::TrackedStateRebuild);
        expect.assert_same(
            Engine2SimulationMode::TrackedStateRebuild,
            "value",
            "1".to_string(),
        );
        expect.finish_mode(Engine2SimulationMode::TrackedStateRebuild);
    }

    #[test]
    #[should_panic(expected = "assertion `left == right` failed")]
    fn local_expect_same_panics_on_different_value() {
        let mut expect = LocalExpectSame::default();
        expect.start_mode(Engine2SimulationMode::Base);
        expect.assert_same(Engine2SimulationMode::Base, "value", "1".to_string());
        expect.finish_mode(Engine2SimulationMode::Base);

        expect.start_mode(Engine2SimulationMode::TrackedStateRebuild);
        expect.assert_same(
            Engine2SimulationMode::TrackedStateRebuild,
            "value",
            "2".to_string(),
        );
    }

    #[test]
    fn shared_expect_same_compares_against_base_run() {
        let case_id = "engine2_expect_same_unit_shared";
        let base = SharedExpectSameRun::new(case_id, Engine2SimulationMode::Base);
        base.assert_same("value", "1".to_string());
        base.finish_mode();

        let rebuild = SharedExpectSameRun::new(case_id, Engine2SimulationMode::TrackedStateRebuild);
        rebuild.assert_same("value", "1".to_string());
        rebuild.finish_mode();
    }
}
