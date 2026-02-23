use super::super::ast::nodes::Statement;
use super::rewrite::predicates;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HistoryRelation {
    State,
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HistoryRewriteRequest {
    pub(crate) relation: HistoryRelation,
    pub(crate) statement_index: usize,
}

pub(crate) fn collect_history_rewrite_requests(
    statements: &[Statement],
) -> Vec<HistoryRewriteRequest> {
    let mut requests = Vec::new();

    for (statement_index, statement) in statements.iter().enumerate() {
        if predicates::statement_targets_state_history(statement) {
            requests.push(HistoryRewriteRequest {
                relation: HistoryRelation::State,
                statement_index,
            });
        }
        if predicates::statement_targets_file_history(statement) {
            requests.push(HistoryRewriteRequest {
                relation: HistoryRelation::File,
                statement_index,
            });
        }
        if predicates::statement_targets_directory_history(statement) {
            requests.push(HistoryRewriteRequest {
                relation: HistoryRelation::Directory,
                statement_index,
            });
        }
    }

    requests
}
