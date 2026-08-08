//! Standalone negative-fixture runner for the W1b-2 stateful model.
//!
//! This intentionally includes the model so the negative controls compile
//! against the same state/authority implementation without importing Lix.

mod model {
    include!("stale_reconciliation_oracle.rs");

    #[test]
    fn second_begin_read_is_not_one_retained_read() {
        let opening = snapshot(1);
        let mut current = current_snapshot(2);
        current.view.begin_reads = 1;
        assert_eq!(
            reconcile(&opening, &current, &[]),
            Err(Corruption::ReadView)
        );
    }

    #[test]
    fn wrong_revision_and_change_id_fail_before_replay() {
        let opening = snapshot(1);
        let mut current = current_snapshot(2);
        let write_a = write("op-a", "file-a", "row", Value::Tombstone);
        current
            .idempotency
            .insert("op-a".into(), write_a.fingerprint());
        current.owners.insert(
            "file-a".into(),
            Proof::Valid {
                file_id: "file-a".into(),
                plugin_key: "plugin-a".into(),
                generation: "generation-a".into(),
                revision: 99,
                change_id: "forged-change".into(),
            },
        );
        assert_eq!(
            reconcile(&opening, &current, &[write_a]),
            Err(Corruption::OwnerProof)
        );
    }

    #[test]
    fn idempotency_payload_mismatch_is_conflict_not_replay() {
        let opening = snapshot(1);
        let mut current = current_snapshot(2);
        let original = write("op-a", "file-a", "row", Value::Null);
        let conflicting = write("op-a", "file-a", "row", Value::Json("new".into()));
        current
            .idempotency
            .insert("op-a".into(), original.fingerprint());
        assert_eq!(
            reconcile(&opening, &current, &[conflicting]),
            Ok(Err(Conflict::IdempotencyMismatch))
        );
    }

    #[test]
    fn reversed_input_order_cannot_change_reconciled_winner() {
        let opening = snapshot(1);
        let mut current = current_snapshot(2);
        current.changed_keys.insert(("file-a".into(), "row".into()));
        let low = write_with_rank("op-low", "file-a", "row", Value::Null, 1);
        let high = write_with_rank("op-high", "file-a", "row", Value::Json("high".into()), 2);
        assert_eq!(
            reconcile(&opening, &current, &[high.clone(), low.clone()]),
            reconcile(&opening, &current, &[low, high])
        );
    }
}
