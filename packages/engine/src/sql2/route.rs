const SQL2_ROUTE_ORIGIN_KEY: &str = "engine:sql2";

pub(crate) fn should_route_selected_read(
    origin_key: Option<&str>,
    resolved_relations: &[String],
) -> bool {
    origin_key == Some(SQL2_ROUTE_ORIGIN_KEY)
        && resolved_relations.len() == 1
        && resolved_relations[0] == "lix_state"
}

#[cfg(test)]
mod tests {
    use super::should_route_selected_read;

    #[test]
    fn routes_only_sql2_bringup_origin_for_lix_state() {
        assert!(should_route_selected_read(
            Some("engine:sql2"),
            &[String::from("lix_state")]
        ));
        assert!(!should_route_selected_read(
            Some("engine:sql2"),
            &[String::from("lix_state_history")]
        ));
        assert!(!should_route_selected_read(
            Some("something-else"),
            &[String::from("lix_state")]
        ));
    }
}
