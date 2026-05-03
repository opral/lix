use std::collections::BTreeSet;

use crate::live_state::LiveStateRow;
use crate::live_state::{LiveStateReader, LiveStateScanRequest};
use crate::transaction::staging::{StagedStateRowIdentity, StagedStateRowOverlay};
use crate::LixError;

pub(crate) async fn overlay_scan_rows(
    base: &dyn LiveStateReader,
    staged: &StagedStateRowOverlay,
    request: &LiveStateScanRequest,
) -> Result<Vec<LiveStateRow>, LixError> {
    let mut rows = staged.scan(request);
    let hidden_identities = staged.identities_matching_scan(request);
    let mut visible_identities = rows
        .iter()
        .map(StagedStateRowIdentity::from)
        .collect::<BTreeSet<_>>();

    for row in base.scan_rows(request).await? {
        let identity = StagedStateRowIdentity::from(&row);
        if hidden_identities.contains(&identity) {
            continue;
        }
        if visible_identities.insert(identity) {
            rows.push(row);
        }
    }

    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}
