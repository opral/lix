use crate::live_state::visibility;
use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateReader, LiveStateScanRequest};
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::LixError;

pub(crate) async fn overlay_scan_rows(
    base: &dyn LiveStateReader,
    staged: &PreparedStateRowOverlay,
    request: &LiveStateScanRequest,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let staged_parts = staged.scan_parts(request)?;
    let mut rows = base.scan_rows(request).await?;
    rows.extend(staged_parts.rows);
    rows = visibility::resolve_scan_rows(
        rows,
        &request.filter.version_ids,
        request.filter.include_tombstones,
    );

    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}
