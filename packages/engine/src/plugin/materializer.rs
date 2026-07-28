use crate::live_state::LiveStateProjection;

pub(crate) fn plugin_state_live_state_projection() -> LiveStateProjection {
    LiveStateProjection {
        columns: vec!["snapshot_content".to_string(), "metadata".to_string()],
    }
}
