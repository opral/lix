// The buffered write runner boundary remains under execution/write/.
//
// SQL statement preparation and retry orchestration moved upward into
// `crate::session::write_preparation` so write_runtime only consumes prepared
// commands.
