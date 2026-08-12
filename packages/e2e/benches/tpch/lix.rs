use std::fmt::Write as _;

use lix::integration::{Engine, SessionContext};
use lix::storage::Storage;
use lix::{ExecuteResult, SqlReadProfile, Value};
use lix_storage_rocksdb::RocksDB;
#[cfg(feature = "slatedb")]
use lix_storage_slatedb::SlateDB;
use tempfile::TempDir;

use crate::data;

const REGION_INSERT_COLUMNS: &str = r#"
    INSERT INTO region (r_rowkey, r_regionkey, r_name, r_comment) VALUES
"#;
const NATION_INSERT_COLUMNS: &str = r#"
    INSERT INTO nation (
        n_rowkey, n_nationkey, n_name, n_regionkey, n_comment
    ) VALUES
"#;
const SUPPLIER_INSERT_COLUMNS: &str = r#"
    INSERT INTO supplier (
        s_rowkey, s_suppkey, s_name, s_address, s_nationkey, s_phone,
        s_acctbal, s_comment
    ) VALUES
"#;
const PART_INSERT_COLUMNS: &str = r#"
    INSERT INTO part (
        p_rowkey, p_partkey, p_name, p_mfgr, p_brand, p_type, p_size,
        p_container, p_retailprice, p_comment
    ) VALUES
"#;
const PARTSUPP_INSERT_COLUMNS: &str = r#"
    INSERT INTO partsupp (
        ps_rowkey, ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
    ) VALUES
"#;
const CUSTOMER_INSERT_COLUMNS: &str = r#"
    INSERT INTO customer (
        c_rowkey, c_custkey, c_name, c_address, c_nationkey, c_phone,
        c_acctbal, c_mktsegment, c_comment
    ) VALUES
"#;
const ORDERS_INSERT_COLUMNS: &str = r#"
    INSERT INTO orders (
        o_rowkey, o_orderkey, o_custkey, o_orderstatus, o_totalprice,
        o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment
    ) VALUES
"#;
const LINEITEM_INSERT_COLUMNS: &str = r#"
    INSERT INTO lineitem (
        l_rowkey, l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity,
        l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus,
        l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode,
        l_comment
    ) VALUES
"#;
const INSERT_ROWS_PER_STATEMENT: usize = 2_048;

const REGION_SCHEMA: &str = r#"{
    "x-lix-key":"region",
    "x-lix-primary-key":["/r_rowkey"],
    "type":"object",
    "properties":{
        "r_rowkey":{"type":"string"},
        "r_regionkey":{"type":"integer"},
        "r_name":{"type":"string"},
        "r_comment":{"type":"string"}
    },
    "required":["r_rowkey","r_regionkey","r_name","r_comment"],
    "additionalProperties":false
}"#;

const NATION_SCHEMA: &str = r#"{
    "x-lix-key":"nation",
    "x-lix-primary-key":["/n_rowkey"],
    "type":"object",
    "properties":{
        "n_rowkey":{"type":"string"},
        "n_nationkey":{"type":"integer"},
        "n_name":{"type":"string"},
        "n_regionkey":{"type":"integer"},
        "n_comment":{"type":"string"}
    },
    "required":["n_rowkey","n_nationkey","n_name","n_regionkey","n_comment"],
    "additionalProperties":false
}"#;

const SUPPLIER_SCHEMA: &str = r#"{
    "x-lix-key":"supplier",
    "x-lix-primary-key":["/s_rowkey"],
    "type":"object",
    "properties":{
        "s_rowkey":{"type":"string"},
        "s_suppkey":{"type":"integer"},
        "s_name":{"type":"string"},
        "s_address":{"type":"string"},
        "s_nationkey":{"type":"integer"},
        "s_phone":{"type":"string"},
        "s_acctbal":{"type":"number"},
        "s_comment":{"type":"string"}
    },
    "required":[
        "s_rowkey","s_suppkey","s_name","s_address","s_nationkey",
        "s_phone","s_acctbal","s_comment"
    ],
    "additionalProperties":false
}"#;

const PART_SCHEMA: &str = r#"{
    "x-lix-key":"part",
    "x-lix-primary-key":["/p_rowkey"],
    "type":"object",
    "properties":{
        "p_rowkey":{"type":"string"},
        "p_partkey":{"type":"integer"},
        "p_name":{"type":"string"},
        "p_mfgr":{"type":"string"},
        "p_brand":{"type":"string"},
        "p_type":{"type":"string"},
        "p_size":{"type":"integer"},
        "p_container":{"type":"string"},
        "p_retailprice":{"type":"number"},
        "p_comment":{"type":"string"}
    },
    "required":[
        "p_rowkey","p_partkey","p_name","p_mfgr","p_brand","p_type",
        "p_size","p_container","p_retailprice","p_comment"
    ],
    "additionalProperties":false
}"#;

const PARTSUPP_SCHEMA: &str = r#"{
    "x-lix-key":"partsupp",
    "x-lix-primary-key":["/ps_rowkey"],
    "type":"object",
    "properties":{
        "ps_rowkey":{"type":"string"},
        "ps_partkey":{"type":"integer"},
        "ps_suppkey":{"type":"integer"},
        "ps_availqty":{"type":"integer"},
        "ps_supplycost":{"type":"number"},
        "ps_comment":{"type":"string"}
    },
    "required":[
        "ps_rowkey","ps_partkey","ps_suppkey","ps_availqty",
        "ps_supplycost","ps_comment"
    ],
    "additionalProperties":false
}"#;

const CUSTOMER_SCHEMA: &str = r#"{
    "x-lix-key":"customer",
    "x-lix-primary-key":["/c_rowkey"],
    "type":"object",
    "properties":{
        "c_rowkey":{"type":"string"},
        "c_custkey":{"type":"integer"},
        "c_name":{"type":"string"},
        "c_address":{"type":"string"},
        "c_nationkey":{"type":"integer"},
        "c_phone":{"type":"string"},
        "c_acctbal":{"type":"number"},
        "c_mktsegment":{"type":"string"},
        "c_comment":{"type":"string"}
    },
    "required":[
        "c_rowkey","c_custkey","c_name","c_address","c_nationkey",
        "c_phone","c_acctbal","c_mktsegment","c_comment"
    ],
    "additionalProperties":false
}"#;

const ORDERS_SCHEMA: &str = r#"{
    "x-lix-key":"orders",
    "x-lix-primary-key":["/o_rowkey"],
    "type":"object",
    "properties":{
        "o_rowkey":{"type":"string"},
        "o_orderkey":{"type":"integer"},
        "o_custkey":{"type":"integer"},
        "o_orderstatus":{"type":"string"},
        "o_totalprice":{"type":"number"},
        "o_orderdate":{"type":"string"},
        "o_orderpriority":{"type":"string"},
        "o_clerk":{"type":"string"},
        "o_shippriority":{"type":"integer"},
        "o_comment":{"type":"string"}
    },
    "required":[
        "o_rowkey","o_orderkey","o_custkey","o_orderstatus","o_totalprice",
        "o_orderdate","o_orderpriority","o_clerk","o_shippriority","o_comment"
    ],
    "additionalProperties":false
}"#;

const LINEITEM_SCHEMA: &str = r#"{
    "x-lix-key":"lineitem",
    "x-lix-primary-key":["/l_rowkey"],
    "type":"object",
    "properties":{
        "l_rowkey":{"type":"string"},
        "l_orderkey":{"type":"integer"},
        "l_partkey":{"type":"integer"},
        "l_suppkey":{"type":"integer"},
        "l_linenumber":{"type":"integer"},
        "l_quantity":{"type":"integer"},
        "l_extendedprice":{"type":"number"},
        "l_discount":{"type":"number"},
        "l_tax":{"type":"number"},
        "l_returnflag":{"type":"string"},
        "l_linestatus":{"type":"string"},
        "l_shipdate":{"type":"string"},
        "l_commitdate":{"type":"string"},
        "l_receiptdate":{"type":"string"},
        "l_shipinstruct":{"type":"string"},
        "l_shipmode":{"type":"string"},
        "l_comment":{"type":"string"}
    },
    "required":[
        "l_rowkey","l_orderkey","l_partkey","l_suppkey","l_linenumber","l_quantity",
        "l_extendedprice","l_discount","l_tax","l_returnflag","l_linestatus",
        "l_shipdate","l_commitdate","l_receiptdate","l_shipinstruct","l_shipmode",
        "l_comment"
    ],
    "additionalProperties":false
}"#;

pub(crate) enum Fixture {
    RocksDB {
        session: SessionContext<RocksDB>,
        _dir: TempDir,
    },
    #[cfg(feature = "slatedb")]
    SlateDB {
        session: SessionContext<SlateDB>,
        _dir: TempDir,
    },
}

impl Fixture {
    pub(crate) async fn rocksdb(scale_factor: f64, overlay_rowkeys: &[String]) -> Self {
        let dir = TempDir::new().expect("create TPC-H RocksDB directory");
        let storage = RocksDB::open(dir.path().join("tpch.rocksdb")).expect("open TPC-H RocksDB");
        let session = prepare(storage, scale_factor, overlay_rowkeys).await;
        Self::RocksDB { session, _dir: dir }
    }

    #[cfg(feature = "slatedb")]
    pub(crate) async fn slatedb(scale_factor: f64, overlay_rowkeys: &[String]) -> Self {
        let dir = TempDir::new().expect("create TPC-H SlateDB directory");
        let storage = SlateDB::open(dir.path().join("tpch.slatedb")).expect("open TPC-H SlateDB");
        let session = prepare(storage, scale_factor, overlay_rowkeys).await;
        Self::SlateDB { session, _dir: dir }
    }

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::RocksDB { .. } => "rocksdb",
            #[cfg(feature = "slatedb")]
            Self::SlateDB { .. } => "slatedb",
        }
    }

    pub(crate) async fn query(&self, sql: &str) -> ExecuteResult {
        match self {
            Self::RocksDB { session, .. } => execute(session, sql).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { session, .. } => execute(session, sql).await,
        }
    }

    pub(crate) async fn query_profiled(&self, sql: &str) -> (ExecuteResult, SqlReadProfile) {
        match self {
            Self::RocksDB { session, .. } => execute_profiled(session, sql).await,
            #[cfg(feature = "slatedb")]
            Self::SlateDB { session, .. } => execute_profiled(session, sql).await,
        }
    }

    pub(crate) async fn explain_analyze(&self, sql: &str) -> String {
        let result = self.query(&format!("EXPLAIN ANALYZE VERBOSE {sql}")).await;
        result
            .rows()
            .iter()
            .map(|row| {
                row.values()
                    .iter()
                    .map(|value| match value {
                        Value::Text(value) => value.clone(),
                        value => format!("{value:?}"),
                    })
                    .collect::<Vec<_>>()
                    .join("\t")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

async fn execute_profiled<S>(
    session: &SessionContext<S>,
    sql: &str,
) -> (ExecuteResult, SqlReadProfile)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute_profiled(sql, &[])
        .await
        .expect("execute profiled Lix TPC-H query")
}

async fn prepare<S>(storage: S, scale_factor: f64, overlay_rowkeys: &[String]) -> SessionContext<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Engine::initialize(storage.clone())
        .await
        .expect("initialize Lix TPC-H storage");
    let engine = Engine::new(storage).await.expect("open Lix TPC-H engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("open Lix TPC-H session");
    for schema in [
        REGION_SCHEMA,
        NATION_SCHEMA,
        SUPPLIER_SCHEMA,
        PART_SCHEMA,
        PARTSUPP_SCHEMA,
        CUSTOMER_SCHEMA,
        ORDERS_SCHEMA,
        LINEITEM_SCHEMA,
    ] {
        let registered = session
            .execute(
                "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked) VALUES (lix_json($1), false, false)",
                &[Value::Text(schema.to_string())],
            )
            .await
            .expect("register TPC-H schema")
            .rows_affected();
        assert_eq!(registered, 1);
    }

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix region seed");
    let rows = data::regions(scale_factor).collect::<Vec<_>>();
    let affected = transaction
        .execute(&region_insert_sql(&rows), &[])
        .await
        .expect("seed Lix region")
        .rows_affected();
    transaction.commit().await.expect("commit Lix region seed");
    assert_eq!(affected, rows.len() as u64);

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix nation seed");
    let rows = data::nations(scale_factor).collect::<Vec<_>>();
    let affected = transaction
        .execute(&nation_insert_sql(&rows), &[])
        .await
        .expect("seed Lix nation")
        .rows_affected();
    transaction.commit().await.expect("commit Lix nation seed");
    assert_eq!(affected, rows.len() as u64);

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix supplier seed");
    let mut rows = data::suppliers(scale_factor);
    let mut affected = 0_u64;
    let mut attempted = 0_u64;
    loop {
        let chunk = rows
            .by_ref()
            .take(INSERT_ROWS_PER_STATEMENT)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        attempted += chunk.len() as u64;
        affected += transaction
            .execute(&supplier_insert_sql(&chunk), &[])
            .await
            .expect("seed Lix supplier chunk")
            .rows_affected();
    }
    transaction
        .commit()
        .await
        .expect("commit Lix supplier seed");
    assert_eq!(affected, attempted, "incomplete TPC-H supplier seed");

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix part seed");
    let mut rows = data::parts(scale_factor);
    let mut affected = 0_u64;
    let mut attempted = 0_u64;
    loop {
        let chunk = rows
            .by_ref()
            .take(INSERT_ROWS_PER_STATEMENT)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        attempted += chunk.len() as u64;
        affected += transaction
            .execute(&part_insert_sql(&chunk), &[])
            .await
            .expect("seed Lix part chunk")
            .rows_affected();
    }
    transaction.commit().await.expect("commit Lix part seed");
    assert_eq!(affected, attempted, "incomplete TPC-H part seed");

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix partsupp seed");
    let mut rows = data::partsupps(scale_factor);
    let mut affected = 0_u64;
    let mut attempted = 0_u64;
    loop {
        let chunk = rows
            .by_ref()
            .take(INSERT_ROWS_PER_STATEMENT)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        attempted += chunk.len() as u64;
        affected += transaction
            .execute(&partsupp_insert_sql(&chunk), &[])
            .await
            .expect("seed Lix partsupp chunk")
            .rows_affected();
    }
    transaction
        .commit()
        .await
        .expect("commit Lix partsupp seed");
    assert_eq!(affected, attempted, "incomplete TPC-H partsupp seed");

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix customer seed");
    let mut rows = data::customers(scale_factor);
    let mut affected = 0_u64;
    let mut attempted = 0_u64;
    loop {
        let chunk = rows
            .by_ref()
            .take(INSERT_ROWS_PER_STATEMENT)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        attempted += chunk.len() as u64;
        affected += transaction
            .execute(&customer_insert_sql(&chunk), &[])
            .await
            .expect("seed Lix customer chunk")
            .rows_affected();
    }
    transaction
        .commit()
        .await
        .expect("commit Lix customer seed");
    assert_eq!(affected, attempted, "incomplete TPC-H customer seed");

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix orders seed");
    let mut rows = data::orders(scale_factor);
    let mut affected = 0_u64;
    let mut attempted = 0_u64;
    loop {
        let chunk = rows
            .by_ref()
            .take(INSERT_ROWS_PER_STATEMENT)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        attempted += chunk.len() as u64;
        affected += transaction
            .execute(&orders_insert_sql(&chunk), &[])
            .await
            .expect("seed Lix orders chunk")
            .rows_affected();
    }
    transaction.commit().await.expect("commit Lix orders seed");
    assert_eq!(affected, attempted, "incomplete TPC-H orders seed");

    let mut transaction = session
        .begin_transaction()
        .await
        .expect("begin Lix lineitem seed");
    let mut rows = data::lineitems(scale_factor);
    let mut affected = 0_u64;
    let mut attempted = 0_u64;
    loop {
        let chunk = rows
            .by_ref()
            .take(INSERT_ROWS_PER_STATEMENT)
            .collect::<Vec<_>>();
        if chunk.is_empty() {
            break;
        }
        attempted += chunk.len() as u64;
        affected += transaction
            .execute(&lineitem_insert_sql(&chunk), &[])
            .await
            .expect("seed Lix lineitem chunk")
            .rows_affected();
    }
    transaction
        .commit()
        .await
        .expect("commit Lix lineitem seed");
    assert_eq!(affected, attempted, "incomplete TPC-H lineitem seed");
    if !overlay_rowkeys.is_empty() {
        let mut transaction = session
            .begin_transaction()
            .await
            .expect("begin Lix TPC-H overlay");
        let mut affected = 0_u64;
        for chunk in overlay_rowkeys.chunks(crate::overlay::ROWS_PER_STATEMENT) {
            affected += transaction
                .execute(&crate::overlay::lineitem_update_sql(chunk), &[])
                .await
                .expect("apply Lix TPC-H overlay chunk")
                .rows_affected();
        }
        transaction
            .commit()
            .await
            .expect("commit Lix TPC-H overlay");
        assert_eq!(
            affected,
            overlay_rowkeys.len() as u64,
            "incomplete Lix TPC-H overlay"
        );
    }
    session
}

fn region_insert_sql(rows: &[data::Region]) -> String {
    let mut sql = REGION_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, '{}', '{}')",
            sql_string(&row.rowkey),
            row.regionkey,
            sql_string(&row.name),
            sql_string(&row.comment),
        )
        .expect("write region SQL");
    }
    sql
}

fn nation_insert_sql(rows: &[data::Nation]) -> String {
    let mut sql = NATION_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, '{}', {}, '{}')",
            sql_string(&row.rowkey),
            row.nationkey,
            sql_string(&row.name),
            row.regionkey,
            sql_string(&row.comment),
        )
        .expect("write nation SQL");
    }
    sql
}

fn supplier_insert_sql(rows: &[data::Supplier]) -> String {
    let mut sql = SUPPLIER_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, '{}', '{}', {}, '{}', {}, '{}')",
            sql_string(&row.rowkey),
            row.suppkey,
            sql_string(&row.name),
            sql_string(&row.address),
            row.nationkey,
            sql_string(&row.phone),
            row.acctbal,
            sql_string(&row.comment),
        )
        .expect("write supplier SQL");
    }
    sql
}

fn part_insert_sql(rows: &[data::Part]) -> String {
    let mut sql = PART_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, '{}', '{}', '{}', '{}', {}, '{}', {}, '{}')",
            sql_string(&row.rowkey),
            row.partkey,
            sql_string(&row.name),
            sql_string(&row.mfgr),
            sql_string(&row.brand),
            sql_string(&row.part_type),
            row.size,
            sql_string(&row.container),
            row.retailprice,
            sql_string(&row.comment),
        )
        .expect("write part SQL");
    }
    sql
}

fn partsupp_insert_sql(rows: &[data::PartSupp]) -> String {
    let mut sql = PARTSUPP_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, {}, {}, {}, '{}')",
            sql_string(&row.rowkey),
            row.partkey,
            row.suppkey,
            row.availqty,
            row.supplycost,
            sql_string(&row.comment),
        )
        .expect("write partsupp SQL");
    }
    sql
}

fn customer_insert_sql(rows: &[data::Customer]) -> String {
    let mut sql = CUSTOMER_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, '{}', '{}', {}, '{}', {}, '{}', '{}')",
            sql_string(&row.rowkey),
            row.custkey,
            sql_string(&row.name),
            sql_string(&row.address),
            row.nationkey,
            sql_string(&row.phone),
            row.acctbal,
            sql_string(&row.mktsegment),
            sql_string(&row.comment),
        )
        .expect("write customer SQL");
    }
    sql
}

fn orders_insert_sql(rows: &[data::Order]) -> String {
    let mut sql = ORDERS_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, {}, '{}', {}, '{}', '{}', '{}', {}, '{}')",
            sql_string(&row.rowkey),
            row.orderkey,
            row.custkey,
            sql_string(&row.orderstatus),
            row.totalprice,
            sql_string(&row.orderdate),
            sql_string(&row.orderpriority),
            sql_string(&row.clerk),
            row.shippriority,
            sql_string(&row.comment),
        )
        .expect("write orders SQL");
    }
    sql
}

fn lineitem_insert_sql(rows: &[data::LineItem]) -> String {
    let mut sql = LINEITEM_INSERT_COLUMNS.to_string();
    for (index, row) in rows.iter().enumerate() {
        if index != 0 {
            sql.push(',');
        }
        write!(
            sql,
            "('{}', {}, {}, {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}')",
            sql_string(&row.rowkey),
            row.orderkey,
            row.partkey,
            row.suppkey,
            row.linenumber,
            row.quantity,
            row.extendedprice,
            row.discount,
            row.tax,
            sql_string(&row.returnflag),
            sql_string(&row.linestatus),
            sql_string(&row.shipdate),
            sql_string(&row.commitdate),
            sql_string(&row.receiptdate),
            sql_string(&row.shipinstruct),
            sql_string(&row.shipmode),
            sql_string(&row.comment),
        )
        .expect("write lineitem SQL");
    }
    sql
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

async fn execute<S>(session: &SessionContext<S>, sql: &str) -> ExecuteResult
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(sql, &[])
        .await
        .expect("execute Lix TPC-H query")
}
