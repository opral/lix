pub(crate) fn maybe_print_io_report() {
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_IO").is_some() {
        eprintln!(
            "LIX_TRACKED_STATE_CRUD_IO is reserved for the tracked_state_crud logical I/O report."
        );
    }
}
