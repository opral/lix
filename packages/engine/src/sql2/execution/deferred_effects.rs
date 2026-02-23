use std::collections::VecDeque;

use crate::LixError;

#[derive(Debug, Clone, Copy)]
pub(crate) struct RetryPolicy {
    pub(crate) max_attempts: usize,
}

impl RetryPolicy {
    pub(crate) const fn at_most(max_attempts: usize) -> Self {
        Self { max_attempts }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

type DeferredEffectFn = dyn FnMut() -> Result<(), LixError> + 'static;

struct DeferredEffect {
    id: String,
    apply: Box<DeferredEffectFn>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PostCommitFlushReport {
    pub(crate) applied_effect_ids: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct PostCommitFlushError {
    pub(crate) effect_id: String,
    pub(crate) attempts: usize,
    pub(crate) error: LixError,
}

#[derive(Default)]
pub(crate) struct DeferredPostCommitEffects {
    effects: VecDeque<DeferredEffect>,
}

impl DeferredPostCommitEffects {
    pub(crate) fn new() -> Self {
        Self {
            effects: VecDeque::new(),
        }
    }

    pub(crate) fn enqueue<F>(&mut self, id: impl Into<String>, effect: F)
    where
        F: FnMut() -> Result<(), LixError> + 'static,
    {
        self.effects.push_back(DeferredEffect {
            id: id.into(),
            apply: Box::new(effect),
        });
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.effects.is_empty()
    }

    pub(crate) fn flush(
        &mut self,
        retry_policy: RetryPolicy,
    ) -> Result<PostCommitFlushReport, PostCommitFlushError> {
        let max_attempts = retry_policy.max_attempts.max(1);
        let mut applied_effect_ids = Vec::new();

        while let Some(mut effect) = self.effects.pop_front() {
            let mut attempts = 0;
            loop {
                attempts += 1;
                match (effect.apply)() {
                    Ok(()) => {
                        applied_effect_ids.push(effect.id.clone());
                        break;
                    }
                    Err(error) if attempts < max_attempts => {
                        continue;
                    }
                    Err(error) => {
                        let effect_id = effect.id.clone();
                        self.effects.push_front(effect);
                        return Err(PostCommitFlushError {
                            effect_id,
                            attempts,
                            error,
                        });
                    }
                }
            }
        }

        Ok(PostCommitFlushReport { applied_effect_ids })
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use super::{DeferredPostCommitEffects, RetryPolicy};
    use crate::LixError;

    #[test]
    fn flush_retries_until_success_within_policy() {
        let attempts = Rc::new(RefCell::new(0usize));
        let attempts_for_effect = Rc::clone(&attempts);
        let mut queue = DeferredPostCommitEffects::new();
        queue.enqueue("effect-a", move || {
            let mut guard = attempts_for_effect.borrow_mut();
            *guard += 1;
            if *guard < 3 {
                Err(LixError {
                    message: "transient failure".to_string(),
                })
            } else {
                Ok(())
            }
        });

        let report = queue
            .flush(RetryPolicy::at_most(3))
            .expect("effect should eventually succeed within retry policy");
        assert_eq!(report.applied_effect_ids, vec!["effect-a".to_string()]);
        assert_eq!(*attempts.borrow(), 3);
        assert!(queue.is_empty(), "successful flush should empty the queue");
    }

    #[test]
    fn flush_surfaces_failure_after_max_attempts_and_keeps_effect_queued() {
        let attempts = Rc::new(RefCell::new(0usize));
        let attempts_for_effect = Rc::clone(&attempts);
        let mut queue = DeferredPostCommitEffects::new();
        queue.enqueue("effect-b", move || {
            *attempts_for_effect.borrow_mut() += 1;
            Err(LixError {
                message: "persistent failure".to_string(),
            })
        });

        let error = queue
            .flush(RetryPolicy::at_most(2))
            .expect_err("flush should fail after retry budget is exhausted");
        assert_eq!(error.effect_id, "effect-b");
        assert_eq!(error.attempts, 2);
        assert!(
            error.error.message.contains("persistent failure"),
            "unexpected error message: {}",
            error.error.message
        );
        assert_eq!(*attempts.borrow(), 2);
        assert!(
            !queue.is_empty(),
            "failed effects should remain queued for a future retry"
        );
    }

    #[test]
    fn successful_effect_is_not_reapplied_on_subsequent_flushes() {
        let attempts = Rc::new(RefCell::new(0usize));
        let attempts_for_effect = Rc::clone(&attempts);
        let mut queue = DeferredPostCommitEffects::new();
        queue.enqueue("effect-c", move || {
            *attempts_for_effect.borrow_mut() += 1;
            Ok(())
        });

        queue
            .flush(RetryPolicy::at_most(2))
            .expect("first flush should succeed");
        let second = queue
            .flush(RetryPolicy::at_most(2))
            .expect("second flush should be a no-op");

        assert!(second.applied_effect_ids.is_empty());
        assert_eq!(
            *attempts.borrow(),
            1,
            "effect should only run once after it has succeeded"
        );
    }
}
