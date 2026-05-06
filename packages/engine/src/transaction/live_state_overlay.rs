use std::collections::BTreeSet;

use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateReader, LiveStateScanRequest};
use crate::transaction::staging::{PreparedStateRowIdentity, PreparedStateRowOverlay};
use crate::LixError;

pub(crate) async fn overlay_scan_rows(
    base: &dyn LiveStateReader,
    staged: &PreparedStateRowOverlay,
    request: &LiveStateScanRequest,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let staged_parts = staged.scan_parts(request)?;
    let hidden_identities = staged_parts.hidden_identities;
    let mut rows = staged_parts.rows;
    let mut visible_identities = rows
        .iter()
        .map(PreparedStateRowIdentity::from)
        .collect::<BTreeSet<_>>();

    for row in base.scan_rows(request).await? {
        let identity = PreparedStateRowIdentity::from(&row);
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
