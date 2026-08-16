#![recursion_limit = "256"]

use std::convert::Infallible;
use std::io::{Cursor, Write as _};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use http::{Request, Response};
use http_body_util::{BodyExt as _, Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use lix::server_protocol::{LixServerProtocol, ServerProtocolBody, ServerProtocolContext};
use lix::storage::Storage;
use lix::{Lix, LixError, ServerOptions, Value, open_lix};
use lix_storage_filesystem::FilesystemStorage;
use tempfile::TempDir;
use tokio::net::TcpListener;

const ROW_SCHEMA: &str = r#"{
  "$schema": "https://lix.dev/schema-v1.json",
  "key": "sync_mode_row",
  "columns": [
    { "name": "row_id", "type": "text", "nullable": false },
    { "name": "value", "type": "text", "nullable": false }
  ],
  "primary_key": ["row_id"]
}"#;

#[tokio::test]
async fn open_execute_observe_syncs_two_filesystem_replicas_and_server_writes() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;

    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice_storage = FilesystemStorage::new(alice_dir.path())
        .open()
        .expect("open alice filesystem storage");
    let bob_storage = FilesystemStorage::new(bob_dir.path())
        .open()
        .expect("open bob filesystem storage");
    let alice = open_lix()
        .with_storage(alice_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(bob_storage)
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");

    let mut bob_observation = bob
        .observe(
            "SELECT value FROM sync_mode_row WHERE row_id = 'shared'",
            &[],
        )
        .expect("observe bob replica");
    bob_observation
        .next()
        .await
        .expect("initial observation event")
        .expect("initial observation remains open");

    alice
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('shared', 'from-alice')",
            &[],
        )
        .await
        .expect("alice local execute");

    let bob_event = tokio::time::timeout(Duration::from_secs(10), bob_observation.next())
        .await
        .expect("bob observes alice before timeout")
        .expect("bob observation event")
        .expect("bob observation remains open");
    assert_eq!(
        bob_event.rows.rows()[0].get::<String>("value").unwrap(),
        "from-alice"
    );

    wait_for_value(&server_lix, "from-alice").await;
    server_lix
        .execute(
            "UPDATE sync_mode_row SET value = 'from-server' WHERE row_id = 'shared'",
            &[],
        )
        .await
        .expect("ordinary server execute");
    wait_for_value(&alice, "from-server").await;
    wait_for_value(&bob, "from-server").await;

    bob_observation.close();
    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_created_branch_becomes_visible_on_sync_replicas() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    // Establish a canonical commit that is present in both local commit
    // graphs. The branch catalog can then use that identity as its source.
    alice
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('branch-seed', 'seed')",
            &[],
        )
        .await
        .expect("seed canonical row");
    wait_for_named_value(&bob, "branch-seed", "seed").await;
    wait_for_named_value(server_lix.as_ref(), "branch-seed", "seed").await;

    let branch_id = "0198a000-0000-7000-8000-0000000000c1";
    let server_branch = server_lix
        .create_branch(lix::CreateBranchOptions {
            id: Some(branch_id.to_owned()),
            name: "server-feature".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create branch on server");

    wait_for_branch(&alice, branch_id).await;
    wait_for_branch(&bob, branch_id).await;
    let server_branch_row = server_lix
        .execute(
            "SELECT name, commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("read server branch")
        .rows()
        .first()
        .cloned()
        .expect("server branch row");
    for replica in [&alice, &bob] {
        let row = replica
            .execute(
                "SELECT name, commit_id FROM lix_branch WHERE id = $1",
                &[Value::Text(branch_id.to_owned())],
            )
            .await
            .expect("read replicated branch")
            .rows()
            .first()
            .cloned()
            .expect("replicated branch row");
        assert_eq!(
            row.get::<String>("name").unwrap(),
            server_branch_row.get::<String>("name").unwrap()
        );
        assert_eq!(
            row.get::<String>("commit_id").unwrap(),
            server_branch_row.get::<String>("commit_id").unwrap()
        );
    }
    assert_eq!(server_branch.id, branch_id);

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn binary_file_bytes_follow_sync_mode_between_filesystem_replicas() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('seed', 'seed')",
            &[],
        )
        .await
        .expect("seed canonical sync event");

    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice filesystem storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice sync replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob filesystem storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob sync replica");

    let payload = b"\0lix-sync-binary\xff";
    alice
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/binary.bin', $1)",
            &[Value::Blob(payload.to_vec().into())],
        )
        .await
        .expect("write binary file on alice");

    wait_for_binary_file(&bob, payload).await;
    wait_for_binary_file(server_lix.as_ref(), payload).await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn initialized_filesystem_replica_reopens_and_accepts_writes_offline() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("bootstrap replica online");
    replica
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('offline', 'online')",
            &[],
        )
        .await
        .expect("write before disconnect");
    wait_for_named_value(&server_lix, "offline", "online").await;
    replica.close().await.expect("close online replica");
    drop(replica);
    server_task.abort();

    let offline = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("reopen replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("reopen initialized replica offline");
    wait_for_named_value(&offline, "offline", "online").await;
    offline
        .execute(
            "UPDATE sync_mode_row SET value = 'queued-offline' WHERE row_id = 'offline'",
            &[],
        )
        .await
        .expect("commit local row and outbox while offline");
    wait_for_named_value(&offline, "offline", "queued-offline").await;
    offline.close().await.expect("close offline replica");
    server_lix.close().await.expect("close server");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_replica_rejects_an_uncertified_pre_sync_server_history() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    server_lix
        .execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register schema before sync authority starts");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('existing', 'server-only')",
            &[],
        )
        .await
        .expect("write pre-sync server row");
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let opened = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open fresh replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await;
    let error = match opened {
        Ok(lix) => {
            lix.close().await.expect("close unexpected replica");
            panic!("uncertified bootstrap must fail closed");
        }
        Err(error) => error,
    };
    assert_eq!(error.code, LixError::CODE_INVALID_PARAM);

    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plugin_rows_sync_and_each_replica_renders_its_local_file() {
    let template_dir = TempDir::new().expect("template tempdir");
    let (first_id, second_id) = {
        let storage = FilesystemStorage::new(template_dir.path())
            .open()
            .expect("open template storage");
        let seed = open_lix()
            .with_storage(storage)
            .await
            .expect("open template repository");
        install_markdown_plugin(&seed).await;
        write_file(
            &seed,
            "/shared.md",
            b"First paragraph.\n\nSecond paragraph.\n",
        )
        .await;
        let paragraphs = seed
            .execute(
                "SELECT id, payload_json FROM markdown_node WHERE kind = 'paragraph'",
                &[],
            )
            .await
            .expect("read template paragraphs");
        let paragraph_id = |needle: &str| {
            paragraphs
                .rows()
                .iter()
                .find(|row| row.get::<String>("payload_json").unwrap().contains(needle))
                .expect("paragraph exists")
                .get::<String>("id")
                .unwrap()
        };
        let ids = (
            paragraph_id("First paragraph."),
            paragraph_id("Second paragraph."),
        );
        seed.close().await.expect("close template repository");
        ids
    };

    let server_dir = TempDir::new().expect("server tempdir");
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    copy_directory(template_dir.path(), server_dir.path());
    copy_directory(template_dir.path(), alice_dir.path());
    copy_directory(template_dir.path(), bob_dir.path());

    let server_storage = FilesystemStorage::new(server_dir.path())
        .open()
        .expect("open server filesystem storage");
    let server_lix = Arc::new(
        open_lix()
            .with_storage(server_storage)
            .await
            .expect("open server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");

    update_markdown_paragraph(&alice, &first_id, "First from Alice.").await;
    update_markdown_paragraph(&bob, &second_id, "Second from Bob.").await;

    let expected = b"First from Alice.\n\nSecond from Bob.\n";
    wait_for_file(server_lix.as_ref(), expected).await;
    wait_for_file(&alice, expected).await;
    wait_for_file(&bob, expected).await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fresh_replica_file_observe_hydrates_only_file_view_semantics() {
    let server_lix = Arc::new(open_lix().await.expect("open server repository"));
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    install_markdown_plugin(server_lix.as_ref()).await;
    write_file(
        server_lix.as_ref(),
        "/shared.md",
        b"First paragraph.\n\nSecond paragraph.\n",
    )
    .await;

    let replica_dir = TempDir::new().expect("replica tempdir");
    let replica = open_lix()
        .with_storage(
            FilesystemStorage::new(replica_dir.path())
                .open()
                .expect("open replica storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open fresh lazy replica");
    let mut observation = replica
        .observe(
            "SELECT content FROM lix_file WHERE path = '/shared.md'",
            &[],
        )
        .expect("observe demanded file view");
    let event = tokio::time::timeout(Duration::from_secs(10), observation.next())
        .await
        .expect("lazy file hydration before timeout")
        .expect("lazy file observation event")
        .expect("lazy file observation remains open");
    assert_eq!(
        event.rows.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"First paragraph.\n\nSecond paragraph.\n"
    );

    observation.close();
    replica.close().await.expect("close replica");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn switching_sync_branches_rebinds_each_replica_to_that_branch() {
    // Build one identical repository image first. This gives every replica a
    // certified local copy of both branch heads before sync authority starts.
    let template_dir = TempDir::new().expect("template tempdir");
    let main_branch_id = {
        let seed = open_lix()
            .with_storage(
                FilesystemStorage::new(template_dir.path())
                    .open()
                    .expect("open template storage"),
            )
            .await
            .expect("open template repository");
        seed.execute(
            "INSERT INTO lix_registered_schema (value) VALUES (CAST($1 AS JSONB))",
            &[Value::Text(ROW_SCHEMA.to_owned())],
        )
        .await
        .expect("register synchronized schema");
        let main_branch_id = seed.active_branch_id().await.expect("main branch id");
        seed.create_branch(lix::CreateBranchOptions {
            id: Some("0198a000-0000-7000-8000-0000000000b1".to_owned()),
            name: "feature".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create feature branch");
        seed.close().await.expect("close template repository");
        main_branch_id
    };

    let server_dir = TempDir::new().expect("server tempdir");
    let alice_dir = TempDir::new().expect("alice tempdir");
    let bob_dir = TempDir::new().expect("bob tempdir");
    copy_directory(template_dir.path(), server_dir.path());
    copy_directory(template_dir.path(), alice_dir.path());
    copy_directory(template_dir.path(), bob_dir.path());

    let server_lix = Arc::new(
        open_lix()
            .with_storage(
                FilesystemStorage::new(server_dir.path())
                    .open()
                    .expect("open server storage"),
            )
            .await
            .expect("open server repository"),
    );
    let protocol = LixServerProtocol::new(Arc::clone(&server_lix));
    let (server_url, server_task) = serve_protocol(protocol).await;
    let alice = open_lix()
        .with_storage(
            FilesystemStorage::new(alice_dir.path())
                .open()
                .expect("open alice storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open alice replica");
    let bob = open_lix()
        .with_storage(
            FilesystemStorage::new(bob_dir.path())
                .open()
                .expect("open bob storage"),
        )
        .with_server(ServerOptions::sync(&server_url))
        .await
        .expect("open bob replica");
    let feature_branch_id = "0198a000-0000-7000-8000-0000000000b1";

    alice
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: feature_branch_id.to_owned(),
        })
        .await
        .expect("switch alice to feature branch");
    bob.switch_branch(lix::SwitchBranchOptions {
        branch_id: feature_branch_id.to_owned(),
    })
    .await
    .expect("switch bob to feature branch");
    server_lix
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: feature_branch_id.to_owned(),
        })
        .await
        .expect("switch server to feature branch");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('feature', 'from-feature')",
            &[],
        )
        .await
        .expect("write feature branch");
    wait_for_named_value(&alice, "feature", "from-feature").await;
    wait_for_named_value(&bob, "feature", "from-feature").await;

    alice
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: main_branch_id.clone(),
        })
        .await
        .expect("switch alice back to main branch");
    server_lix
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: main_branch_id,
        })
        .await
        .expect("switch server back to main branch");
    server_lix
        .execute(
            "INSERT INTO sync_mode_row (row_id, value) VALUES ('main-only', 'from-main')",
            &[],
        )
        .await
        .expect("write main branch");
    wait_for_named_value(&alice, "main-only", "from-main").await;

    alice.close().await.expect("close alice");
    bob.close().await.expect("close bob");
    server_lix.close().await.expect("close server");
    server_task.abort();
}

async fn wait_for_value<S>(lix: &Lix<S>, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = lix
                .execute(
                    "SELECT value FROM sync_mode_row WHERE row_id = 'shared'",
                    &[],
                )
                .await
                .expect("read synchronized row");
            if result
                .rows()
                .first()
                .and_then(|row| row.get::<String>("value").ok())
                .as_deref()
                == Some(expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for synchronized value '{expected}'"));
}

async fn wait_for_named_value<S>(lix: &Lix<S>, row_id: &str, expected: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = lix
                .execute(
                    "SELECT value FROM sync_mode_row WHERE row_id = $1",
                    &[Value::Text(row_id.to_owned())],
                )
                .await
                .expect("read named synchronized row");
            if result
                .rows()
                .first()
                .and_then(|row| row.get::<String>("value").ok())
                .as_deref()
                == Some(expected)
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for row '{row_id}' value '{expected}'"));
}

async fn wait_for_branch<S>(lix: &Lix<S>, branch_id: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = lix
                .execute(
                    "SELECT id FROM lix_branch WHERE id = $1",
                    &[Value::Text(branch_id.to_owned())],
                )
                .await
                .expect("read synchronized branch catalog");
            if !result.rows().is_empty() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for branch '{branch_id}'"));
}

async fn wait_for_file<S>(lix: &Lix<S>, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = lix
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/shared.md'",
                    &[],
                )
                .await
                .expect("read rendered file");
            let content = result.rows()[0].get::<Vec<u8>>("content").unwrap();
            if content == expected {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for rendered plugin file");
}

async fn wait_for_binary_file<S>(lix: &Lix<S>, expected: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let result = lix
                .execute(
                    "SELECT content FROM lix_file WHERE path = '/binary.bin'",
                    &[],
                )
                .await
                .expect("read synchronized binary file");
            let content = result
                .rows()
                .first()
                .and_then(|row| row.get::<Vec<u8>>("content").ok());
            if content.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("timed out waiting for synchronized binary file");
}

async fn install_markdown_plugin<S>(lix: &Lix<S>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ('/.lix/plugins/plugin_markdown.lixplugin', $1)",
        &[Value::Blob(markdown_plugin_archive().into())],
    )
    .await
    .expect("install markdown plugin");
}

async fn write_file<S>(lix: &Lix<S>, path: &str, content: &[u8])
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
        &[
            Value::Text(path.to_owned()),
            Value::Blob(content.to_vec().into()),
        ],
    )
    .await
    .expect("write plugin-backed file");
}

async fn update_markdown_paragraph<S>(lix: &Lix<S>, id: &str, text: &str)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    lix.execute(
        "UPDATE markdown_node SET payload_json = $1 WHERE id = $2",
        &[
            Value::Text(serde_json::json!({"inline":[{"type":"text","value":text}]}).to_string()),
            Value::Text(id.to_owned()),
        ],
    )
    .await
    .expect("update markdown semantic row");
}

fn markdown_plugin_archive() -> Vec<u8> {
    let wasm = std::fs::read(Path::new(env!(
        "CARGO_CDYLIB_FILE_PLUGIN_MARKDOWN_plugin_markdown"
    )))
    .expect("read markdown plugin component");
    let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
    let options =
        zip::write::SimpleFileOptions::default().compression_method(zip::CompressionMethod::Stored);
    writer.start_file("manifest.json", options).unwrap();
    writer
        .write_all(include_bytes!("../../../plugins/markdown/manifest.json"))
        .unwrap();
    writer
        .start_file("schema/markdown_node.json", options)
        .unwrap();
    writer
        .write_all(include_bytes!(
            "../../../plugins/markdown/schema/markdown_node.json"
        ))
        .unwrap();
    writer.start_file("plugin.wasm", options).unwrap();
    writer.write_all(&wasm).unwrap();
    writer.finish().unwrap().into_inner()
}

fn copy_directory(source: &Path, target: &Path) {
    for entry in std::fs::read_dir(source).expect("read template directory") {
        let entry = entry.expect("template directory entry");
        let destination = target.join(entry.file_name());
        if entry.file_type().expect("template entry type").is_dir() {
            std::fs::create_dir_all(&destination).expect("create cloned directory");
            copy_directory(&entry.path(), &destination);
        } else {
            std::fs::copy(entry.path(), destination).expect("copy template file");
        }
    }
}

async fn serve_protocol<S>(protocol: LixServerProtocol<S>) -> (String, tokio::task::JoinHandle<()>)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind protocol listener");
    let address = listener.local_addr().expect("protocol listener address");
    let task = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.expect("accept protocol client");
            let protocol = protocol.clone();
            tokio::spawn(async move {
                let service = service_fn(move |request| serve_request(protocol.clone(), request));
                let _ = http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (format!("http://{address}"), task)
}

async fn serve_request<S>(
    protocol: LixServerProtocol<S>,
    request: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (parts, body) = request.into_parts();
    let body = body
        .collect()
        .await
        .expect("collect request body")
        .to_bytes();
    let response = protocol
        .handle(
            Request::from_parts(parts, ServerProtocolBody::full(body)),
            ServerProtocolContext::anonymous(),
        )
        .await;
    let (parts, body) = response.into_parts();
    let body = body
        .collect()
        .await
        .expect("collect protocol response")
        .to_bytes();
    Ok(Response::from_parts(parts, Full::new(body)))
}
