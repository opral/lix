use std::fs;
use std::path::PathBuf;

fn read_engine_source(relative: &str) -> String {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src")
        .join(relative);
    fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()))
}

#[test]
fn engine_open_session_is_the_workspace_root_entrypoint() {
    let source = read_engine_source("engine.rs");

    assert!(
        source.contains(
            "pub async fn open_session(self: &Arc<Self>) -> Result<crate::Session, LixError>"
        ),
        "engine.rs should expose open_session() as the workspace-root entrypoint",
    );
    assert!(
        source.contains("crate::Session::open_workspace(Arc::clone(self)).await"),
        "engine.rs should open the workspace-backed root session directly",
    );

    for forbidden in [
        "pub async fn open_workspace_session(",
        "options: crate::OpenSessionOptions",
        ".open_child_session(",
    ] {
        assert!(
            !source.contains(forbidden),
            "engine.rs should not reintroduce the old root/child ambiguity\nforbidden: {}",
            forbidden,
        );
    }
}

#[test]
fn child_session_entrypoints_are_explicit_and_session_scoped() {
    let session_source = read_engine_source("session/mod.rs");
    let lix_source = read_engine_source("lix.rs");

    assert!(
        session_source.contains(
            "pub async fn open_child_session(&self, options: OpenSessionOptions) -> Result<Self, LixError>"
        ),
        "session/mod.rs should expose explicit child-session construction",
    );
    assert!(
        !session_source.contains(
            "pub async fn open_session(&self, options: OpenSessionOptions) -> Result<Self, LixError>"
        ),
        "session/mod.rs should not expose the old ambiguous child-session name",
    );
    assert!(
        session_source.contains(
            "pub(crate) async fn open_workspace(engine: Arc<Engine>) -> Result<Self, LixError>"
        ),
        "workspace-root construction should remain crate-private under Session",
    );
    assert!(
        !session_source.contains("pub async fn open_workspace("),
        "this plan should not broaden Session with a new public workspace-root constructor",
    );

    assert!(
        lix_source.contains(
            "pub async fn open_child_session(&self, options: OpenSessionOptions) -> Result<Self, LixError>"
        ),
        "lix.rs should mirror the explicit child-session API",
    );
    assert!(
        !lix_source.contains(
            "pub async fn open_session(&self, options: OpenSessionOptions) -> Result<Self, LixError>"
        ),
        "lix.rs should not expose the old ambiguous child-session name",
    );
    assert!(
        lix_source.contains("let session = engine.open_session().await?;"),
        "Lix::open should use Engine::open_session() as the default root-session entrypoint",
    );
}
