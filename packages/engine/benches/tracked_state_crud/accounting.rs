use crate::backends::{BACKEND_PROFILES, BackendProfile};
use crate::kv_layout::{self, KvLayoutAccounting, KvWriteAccounting};
use crate::transaction_api::{self, TransactionLayoutAccounting, TransactionWriteAccounting};
use crate::workload::{WorkloadRow, row_label};

pub(crate) fn maybe_print_accounting_report(
    runtime: &tokio::runtime::Runtime,
    rows: &[WorkloadRow],
) {
    if std::env::var_os("LIX_TRACKED_STATE_CRUD_ACCOUNTING").is_none() {
        return;
    }

    println!();
    println!(
        "## tracked_state_crud accounting ({})",
        row_label(rows.len())
    );
    println!();
    print_write_accounting(runtime, rows);
    println!();
    print_layout_accounting(runtime, rows);
    println!();
}

fn print_write_accounting(runtime: &tokio::runtime::Runtime, rows: &[WorkloadRow]) {
    println!("### Write amplification");
    println!();
    println!(
        "| Layer | Backend | Operation | Logical rows | Puts | Point deletes | Range deletes | Touched spaces | Backend calls | Put batches | Delete batches | Written bytes | Put amp | Delete amp |"
    );
    println!(
        "| ----- | ------- | --------- | -----------: | ---: | ------------: | ------------: | -------------: | ------------: | ----------: | -------------: | ------------: | ------: | ---------: |"
    );

    for profile in BACKEND_PROFILES {
        let mut kv_insert = runtime.block_on(kv_layout::empty_fixture(profile, rows));
        print_kv_write(
            profile,
            "kv_layout",
            "insert_all",
            runtime.block_on(kv_insert.insert_all_accounting()),
        );

        let mut kv_update = runtime.block_on(kv_layout::seeded_fixture(profile, rows));
        print_kv_write(
            profile,
            "kv_layout",
            "update_all",
            runtime.block_on(kv_update.update_all_accounting()),
        );

        let mut kv_update_one = runtime.block_on(kv_layout::seeded_fixture(profile, rows));
        print_kv_write(
            profile,
            "kv_layout",
            "update_one_by_pk",
            runtime.block_on(kv_update_one.update_one_by_pk_accounting()),
        );

        let mut kv_delete = runtime.block_on(kv_layout::seeded_fixture(profile, rows));
        print_kv_write(
            profile,
            "kv_layout",
            "delete_all",
            runtime.block_on(kv_delete.delete_all_accounting()),
        );

        let mut kv_delete_one = runtime.block_on(kv_layout::seeded_fixture(profile, rows));
        print_kv_write(
            profile,
            "kv_layout",
            "delete_one_by_pk",
            runtime.block_on(kv_delete_one.delete_one_by_pk_accounting()),
        );

        let mut transaction_insert =
            runtime.block_on(transaction_api::empty_fixture(profile, rows));
        print_transaction_write(
            profile,
            "transaction",
            "insert_all",
            runtime.block_on(transaction_insert.insert_all_accounting()),
        );

        let mut transaction_update =
            runtime.block_on(transaction_api::seeded_fixture(profile, rows));
        print_transaction_write(
            profile,
            "transaction",
            "update_all",
            runtime.block_on(transaction_update.update_all_accounting()),
        );

        let mut transaction_update_one =
            runtime.block_on(transaction_api::seeded_fixture(profile, rows));
        print_transaction_write(
            profile,
            "transaction",
            "update_one_by_pk",
            runtime.block_on(transaction_update_one.update_one_by_pk_accounting()),
        );

        let mut transaction_delete =
            runtime.block_on(transaction_api::seeded_fixture(profile, rows));
        print_transaction_write(
            profile,
            "transaction",
            "delete_all",
            runtime.block_on(transaction_delete.delete_all_accounting()),
        );

        let mut transaction_delete_one =
            runtime.block_on(transaction_api::seeded_fixture(profile, rows));
        print_transaction_write(
            profile,
            "transaction",
            "delete_one_by_pk",
            runtime.block_on(transaction_delete_one.delete_one_by_pk_accounting()),
        );
    }
}

fn print_layout_accounting(runtime: &tokio::runtime::Runtime, rows: &[WorkloadRow]) {
    println!("### Layout footprint after insert_all");
    println!();
    println!("| Layer | Backend | Space id | Space | Rows | Key bytes | Value bytes |");
    println!("| ----- | ------- | -------: | ----- | ---: | --------: | ----------: |");

    for profile in BACKEND_PROFILES {
        let kv = runtime.block_on(kv_layout::seeded_fixture(profile, rows));
        for row in runtime.block_on(kv.layout_accounting()) {
            print_kv_layout(profile, "kv_layout", row);
        }

        let transaction = runtime.block_on(transaction_api::seeded_fixture(profile, rows));
        for row in runtime.block_on(transaction.layout_accounting()) {
            print_transaction_layout(profile, "transaction", row);
        }
    }
}

fn print_kv_write(
    profile: BackendProfile,
    layer: &str,
    operation: &str,
    accounting: KvWriteAccounting,
) {
    print_write_row(
        profile,
        layer,
        operation,
        WriteRow {
            logical_rows: accounting.logical_rows,
            staged_puts: accounting.staged_puts,
            staged_deletes: accounting.staged_deletes,
            range_deletes: accounting.range_deletes,
            touched_spaces: accounting.touched_spaces,
            backend_calls: accounting.backend_calls,
            put_batches: accounting.put_batches,
            delete_batches: accounting.delete_batches,
            written_bytes: accounting.written_bytes,
        },
    );
}

fn print_transaction_write(
    profile: BackendProfile,
    layer: &str,
    operation: &str,
    accounting: TransactionWriteAccounting,
) {
    print_write_row(
        profile,
        layer,
        operation,
        WriteRow {
            logical_rows: accounting.logical_rows,
            staged_puts: accounting.staged_puts,
            staged_deletes: accounting.staged_deletes,
            range_deletes: 0,
            touched_spaces: accounting.touched_spaces,
            backend_calls: accounting.backend_calls,
            put_batches: accounting.put_batches,
            delete_batches: accounting.delete_batches,
            written_bytes: accounting.written_bytes,
        },
    );
}

struct WriteRow {
    logical_rows: usize,
    staged_puts: u64,
    staged_deletes: u64,
    range_deletes: u64,
    touched_spaces: u64,
    backend_calls: u64,
    put_batches: u64,
    delete_batches: u64,
    written_bytes: u64,
}

fn print_write_row(profile: BackendProfile, layer: &str, operation: &str, row: WriteRow) {
    println!(
        "| {layer} | {} | {operation} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        profile.name(),
        row.logical_rows,
        row.staged_puts,
        row.staged_deletes,
        row.range_deletes,
        row.touched_spaces,
        row.backend_calls,
        row.put_batches,
        row.delete_batches,
        row.written_bytes,
        amp(row.staged_puts, row.logical_rows),
        amp(row.staged_deletes + row.range_deletes, row.logical_rows),
    );
}

fn print_kv_layout(profile: BackendProfile, layer: &str, row: KvLayoutAccounting) {
    print_layout_row(
        profile,
        layer,
        LayoutRow {
            space_id: row.space_id,
            space: row.space,
            rows: row.rows,
            key_bytes: row.key_bytes,
            value_bytes: row.value_bytes,
        },
    );
}

fn print_transaction_layout(
    profile: BackendProfile,
    layer: &str,
    row: TransactionLayoutAccounting,
) {
    print_layout_row(
        profile,
        layer,
        LayoutRow {
            space_id: row.space_id,
            space: row.space,
            rows: row.rows,
            key_bytes: row.key_bytes,
            value_bytes: row.value_bytes,
        },
    );
}

struct LayoutRow {
    space_id: u32,
    space: &'static str,
    rows: u64,
    key_bytes: u64,
    value_bytes: u64,
}

fn print_layout_row(profile: BackendProfile, layer: &str, row: LayoutRow) {
    println!(
        "| {layer} | {} | `0x{:08x}` | `{}` | {} | {} | {} |",
        profile.name(),
        row.space_id,
        row.space,
        row.rows,
        row.key_bytes,
        row.value_bytes
    );
}

#[expect(clippy::cast_precision_loss)]
fn amp(count: u64, logical_rows: usize) -> String {
    if logical_rows == 0 {
        return "-".to_string();
    }
    let value = count as f64 / logical_rows as f64;
    format!("{value:.2}x")
}
