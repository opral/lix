use datafusion::arrow::record_batch::RecordBatch;
use duckdb::{Connection, params};
use std::time::{Duration, Instant};

use crate::data;

pub(crate) fn seeded(scale_factor: f64, overlay_rowkeys: &[String]) -> Connection {
    let mut connection = Connection::open_in_memory().expect("open DuckDB TPC-H control");
    connection
        .execute_batch(
            r#"
            SET threads = 1;
            CREATE TABLE region (
                r_rowkey VARCHAR NOT NULL PRIMARY KEY,
                r_regionkey BIGINT NOT NULL,
                r_name VARCHAR NOT NULL,
                r_comment VARCHAR NOT NULL
            );
            CREATE TABLE nation (
                n_rowkey VARCHAR NOT NULL PRIMARY KEY,
                n_nationkey BIGINT NOT NULL,
                n_name VARCHAR NOT NULL,
                n_regionkey BIGINT NOT NULL,
                n_comment VARCHAR NOT NULL
            );
            CREATE TABLE supplier (
                s_rowkey VARCHAR NOT NULL PRIMARY KEY,
                s_suppkey BIGINT NOT NULL,
                s_name VARCHAR NOT NULL,
                s_address VARCHAR NOT NULL,
                s_nationkey BIGINT NOT NULL,
                s_phone VARCHAR NOT NULL,
                s_acctbal DOUBLE NOT NULL,
                s_comment VARCHAR NOT NULL
            );
            CREATE TABLE part (
                p_rowkey VARCHAR NOT NULL PRIMARY KEY,
                p_partkey BIGINT NOT NULL,
                p_name VARCHAR NOT NULL,
                p_mfgr VARCHAR NOT NULL,
                p_brand VARCHAR NOT NULL,
                p_type VARCHAR NOT NULL,
                p_size BIGINT NOT NULL,
                p_container VARCHAR NOT NULL,
                p_retailprice DOUBLE NOT NULL,
                p_comment VARCHAR NOT NULL
            );
            CREATE TABLE partsupp (
                ps_rowkey VARCHAR NOT NULL PRIMARY KEY,
                ps_partkey BIGINT NOT NULL,
                ps_suppkey BIGINT NOT NULL,
                ps_availqty BIGINT NOT NULL,
                ps_supplycost DOUBLE NOT NULL,
                ps_comment VARCHAR NOT NULL
            );
            CREATE TABLE customer (
                c_rowkey VARCHAR NOT NULL PRIMARY KEY,
                c_custkey BIGINT NOT NULL,
                c_name VARCHAR NOT NULL,
                c_address VARCHAR NOT NULL,
                c_nationkey BIGINT NOT NULL,
                c_phone VARCHAR NOT NULL,
                c_acctbal DOUBLE NOT NULL,
                c_mktsegment VARCHAR NOT NULL,
                c_comment VARCHAR NOT NULL
            );
            CREATE TABLE orders (
                o_rowkey VARCHAR NOT NULL PRIMARY KEY,
                o_orderkey BIGINT NOT NULL,
                o_custkey BIGINT NOT NULL,
                o_orderstatus VARCHAR NOT NULL,
                o_totalprice DOUBLE NOT NULL,
                o_orderdate VARCHAR NOT NULL,
                o_orderpriority VARCHAR NOT NULL,
                o_clerk VARCHAR NOT NULL,
                o_shippriority BIGINT NOT NULL,
                o_comment VARCHAR NOT NULL
            );
            CREATE TABLE lineitem (
                l_rowkey VARCHAR NOT NULL PRIMARY KEY,
                l_orderkey BIGINT NOT NULL,
                l_partkey BIGINT NOT NULL,
                l_suppkey BIGINT NOT NULL,
                l_linenumber BIGINT NOT NULL,
                l_quantity BIGINT NOT NULL,
                l_extendedprice DOUBLE NOT NULL,
                l_discount DOUBLE NOT NULL,
                l_tax DOUBLE NOT NULL,
                l_returnflag VARCHAR NOT NULL,
                l_linestatus VARCHAR NOT NULL,
                l_shipdate VARCHAR NOT NULL,
                l_commitdate VARCHAR NOT NULL,
                l_receiptdate VARCHAR NOT NULL,
                l_shipinstruct VARCHAR NOT NULL,
                l_shipmode VARCHAR NOT NULL,
                l_comment VARCHAR NOT NULL
            );
            "#,
        )
        .expect("create DuckDB TPC-H tables");

    let transaction = connection.transaction().expect("begin DuckDB seed");
    {
        let mut appender = transaction.appender("region").expect("DuckDB appender");
        for row in data::regions(scale_factor) {
            appender
                .append_row(params![row.rowkey, row.regionkey, row.name, row.comment,])
                .expect("append DuckDB region");
        }
        appender.flush().expect("flush DuckDB region appender");
    }
    {
        let mut appender = transaction.appender("nation").expect("DuckDB appender");
        for row in data::nations(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.nationkey,
                    row.name,
                    row.regionkey,
                    row.comment,
                ])
                .expect("append DuckDB nation");
        }
        appender.flush().expect("flush DuckDB nation appender");
    }
    {
        let mut appender = transaction.appender("supplier").expect("DuckDB appender");
        for row in data::suppliers(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.suppkey,
                    row.name,
                    row.address,
                    row.nationkey,
                    row.phone,
                    row.acctbal,
                    row.comment,
                ])
                .expect("append DuckDB supplier");
        }
        appender.flush().expect("flush DuckDB supplier appender");
    }
    {
        let mut appender = transaction.appender("part").expect("DuckDB appender");
        for row in data::parts(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.partkey,
                    row.name,
                    row.mfgr,
                    row.brand,
                    row.part_type,
                    row.size,
                    row.container,
                    row.retailprice,
                    row.comment,
                ])
                .expect("append DuckDB part");
        }
        appender.flush().expect("flush DuckDB part appender");
    }
    {
        let mut appender = transaction.appender("partsupp").expect("DuckDB appender");
        for row in data::partsupps(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.partkey,
                    row.suppkey,
                    row.availqty,
                    row.supplycost,
                    row.comment,
                ])
                .expect("append DuckDB partsupp");
        }
        appender.flush().expect("flush DuckDB partsupp appender");
    }
    {
        let mut appender = transaction.appender("customer").expect("DuckDB appender");
        for row in data::customers(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.custkey,
                    row.name,
                    row.address,
                    row.nationkey,
                    row.phone,
                    row.acctbal,
                    row.mktsegment,
                    row.comment,
                ])
                .expect("append DuckDB customer");
        }
        appender.flush().expect("flush DuckDB customer appender");
    }
    {
        let mut appender = transaction.appender("orders").expect("DuckDB appender");
        for row in data::orders(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.orderkey,
                    row.custkey,
                    row.orderstatus,
                    row.totalprice,
                    row.orderdate,
                    row.orderpriority,
                    row.clerk,
                    row.shippriority,
                    row.comment,
                ])
                .expect("append DuckDB orders");
        }
        appender.flush().expect("flush DuckDB orders appender");
    }
    {
        let mut appender = transaction.appender("lineitem").expect("DuckDB appender");
        for row in data::lineitems(scale_factor) {
            appender
                .append_row(params![
                    row.rowkey,
                    row.orderkey,
                    row.partkey,
                    row.suppkey,
                    row.linenumber,
                    row.quantity,
                    row.extendedprice,
                    row.discount,
                    row.tax,
                    row.returnflag,
                    row.linestatus,
                    row.shipdate,
                    row.commitdate,
                    row.receiptdate,
                    row.shipinstruct,
                    row.shipmode,
                    row.comment,
                ])
                .expect("append DuckDB lineitem");
        }
        appender.flush().expect("flush DuckDB lineitem appender");
    }
    transaction.commit().expect("commit DuckDB seed");
    if !overlay_rowkeys.is_empty() {
        let transaction = connection.transaction().expect("begin DuckDB overlay");
        let mut affected = 0_usize;
        for chunk in overlay_rowkeys.chunks(crate::overlay::ROWS_PER_STATEMENT) {
            affected += transaction
                .execute(&crate::overlay::lineitem_update_sql(chunk), [])
                .expect("apply DuckDB TPC-H overlay chunk");
        }
        transaction.commit().expect("commit DuckDB overlay");
        assert_eq!(
            affected,
            overlay_rowkeys.len(),
            "incomplete DuckDB TPC-H overlay"
        );
    }
    connection
}

pub(crate) fn query(connection: &Connection, sql: &str) -> Vec<RecordBatch> {
    query_profiled(connection, sql).batches
}

pub(crate) struct ProfiledQuery {
    pub(crate) batches: Vec<RecordBatch>,
    pub(crate) prepare: Duration,
    /// DuckDB's `query_arrow` executes the query and constructs its internal
    /// result. Its public API does not expose a truthful split between those.
    pub(crate) query_arrow: Duration,
    pub(crate) arrow_collection: Duration,
}

pub(crate) fn query_profiled(connection: &Connection, sql: &str) -> ProfiledQuery {
    let started = Instant::now();
    let mut statement = connection.prepare(sql).expect("prepare DuckDB TPC-H query");
    let prepare = started.elapsed();

    let started = Instant::now();
    let arrow = statement
        .query_arrow([])
        .expect("execute DuckDB TPC-H query");
    let query_arrow = started.elapsed();

    let started = Instant::now();
    let batches = arrow.collect();
    let arrow_collection = started.elapsed();
    ProfiledQuery {
        batches,
        prepare,
        query_arrow,
        arrow_collection,
    }
}
