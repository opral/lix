#[tokio::test]
async fn scoped_file_index_publication_prepares_renamed_ancestors_and_new_matches() {
    let authority = open_lix().await.unwrap();
    authority.set_sync_role(crate::sync::SyncRole::Authority).unwrap();
    authority.execute(
        "INSERT INTO lix_file (id,path,content) VALUES ('aaaaaaaa-0000-8000-8000-000000000001','/old/nested/selected.bin',CAST('one' AS BYTEA)), ('cccccccc-0000-8000-8000-000000000003','/unrelated/file.bin',CAST('other' AS BYTEA))",
        &[],
    ).await.unwrap();
    let (authority, engine, session, old) = fixture_from_authority(authority, None).await;
    let storage = engine.storage();
    let ids = vec!["aaaaaaaa-0000-8000-8000-000000000001".to_owned(), "bbbbbbbb-0000-8000-8000-000000000002".to_owned()];
    let sql = "SELECT id,path FROM lix_file WHERE id IN ($1,$2) ORDER BY id";
    let params = ids.iter().cloned().map(Value::Text).collect::<Vec<_>>();
    let initial = execute_hydrating(&session, &storage, &old, &authority, sql, &params, &mut Fetches::default()).await.unwrap();
    assert_eq!(initial.rows().len(), 1);
    assert_eq!(initial.rows()[0].get::<String>("path").unwrap(), "/old/nested/selected.bin");
    let interests = engine.sync_mode().read_interests().unwrap().snapshot().unwrap();
    let recipes = interests.interests.iter().filter_map(|interest| match interest.as_ref() {
        crate::hot_state::LogicalReadInterest::FilesystemPaths { file_ids, .. } => Some(file_ids),
        _ => None,
    }).collect::<Vec<_>>();
    assert!(!recipes.is_empty());
    assert!(recipes.iter().all(|scope| scope.as_ref() == Some(&ids)), "selected and negative IDs must survive the retained recipe without widening");
    authority.execute("UPDATE lix_directory SET path='/moved' WHERE path='/old'", &[]).await.unwrap();
    authority.execute("INSERT INTO lix_file (id,path,content) VALUES ('bbbbbbbb-0000-8000-8000-000000000002','/new/appeared.bin',CAST('two' AS BYTEA))", &[]).await.unwrap();
    let next = Arc::new(old.with_descriptor_and_fresh_generations(authority.partial_replica_descriptor(None).await.unwrap()).unwrap());
    let prepared = prepare_hydrating(&engine, &old, next.clone(), &authority).await;
    assert_eq!(session.execute(sql, &params).await.unwrap().rows()[0].get::<String>("path").unwrap(), "/old/nested/selected.bin", "old cached view remains stable before publication");
    publish_prepared_partial(engine.clone(), prepared).await.unwrap();
    let result = session.execute(sql, &params).await.expect("publication prepares new scoped paths before switching generations");
    assert_eq!(result.rows().iter().map(|row| row.get::<String>("path").unwrap()).collect::<Vec<_>>(), vec!["/moved/nested/selected.bin", "/new/appeared.bin"]);
    let (_, reopened) = Engine::new_partial_replica(storage, EngineOptions::new(), &next).await.unwrap();
    assert_eq!(reopened.execute(sql, &params).await.unwrap().rows(), result.rows(), "reopening needs no additional hydration");
}
