use crate::hot_state::HotStateProjection;

pub(crate) fn plugin_state_hot_state_projection() -> HotStateProjection {
    HotStateProjection {
        columns: vec!["snapshot_content".to_string(), "metadata".to_string()],
    }
}
