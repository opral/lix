use tpchgen::generators::{
    CustomerGenerator, LineItemGenerator, NationGenerator, OrderGenerator, PartGenerator,
    PartSuppGenerator, RegionGenerator, SupplierGenerator,
};

#[derive(Debug)]
pub(crate) struct Region {
    pub(crate) rowkey: String,
    pub(crate) regionkey: i64,
    pub(crate) name: String,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct Nation {
    pub(crate) rowkey: String,
    pub(crate) nationkey: i64,
    pub(crate) name: String,
    pub(crate) regionkey: i64,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct Supplier {
    pub(crate) rowkey: String,
    pub(crate) suppkey: i64,
    pub(crate) name: String,
    pub(crate) address: String,
    pub(crate) nationkey: i64,
    pub(crate) phone: String,
    pub(crate) acctbal: f64,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct Part {
    pub(crate) rowkey: String,
    pub(crate) partkey: i64,
    pub(crate) name: String,
    pub(crate) mfgr: String,
    pub(crate) brand: String,
    pub(crate) part_type: String,
    pub(crate) size: i64,
    pub(crate) container: String,
    pub(crate) retailprice: f64,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct PartSupp {
    pub(crate) rowkey: String,
    pub(crate) partkey: i64,
    pub(crate) suppkey: i64,
    pub(crate) availqty: i64,
    pub(crate) supplycost: f64,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct Customer {
    pub(crate) rowkey: String,
    pub(crate) custkey: i64,
    pub(crate) name: String,
    pub(crate) address: String,
    pub(crate) nationkey: i64,
    pub(crate) phone: String,
    pub(crate) acctbal: f64,
    pub(crate) mktsegment: String,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct Order {
    pub(crate) rowkey: String,
    pub(crate) orderkey: i64,
    pub(crate) custkey: i64,
    pub(crate) orderstatus: String,
    pub(crate) totalprice: f64,
    pub(crate) orderdate: String,
    pub(crate) orderpriority: String,
    pub(crate) clerk: String,
    pub(crate) shippriority: i64,
    pub(crate) comment: String,
}

#[derive(Debug)]
pub(crate) struct LineItem {
    pub(crate) rowkey: String,
    pub(crate) orderkey: i64,
    pub(crate) partkey: i64,
    pub(crate) suppkey: i64,
    pub(crate) linenumber: i64,
    pub(crate) quantity: i64,
    pub(crate) extendedprice: f64,
    pub(crate) discount: f64,
    pub(crate) tax: f64,
    pub(crate) returnflag: String,
    pub(crate) linestatus: String,
    pub(crate) shipdate: String,
    pub(crate) commitdate: String,
    pub(crate) receiptdate: String,
    pub(crate) shipinstruct: String,
    pub(crate) shipmode: String,
    pub(crate) comment: String,
}

pub(crate) fn regions(scale_factor: f64) -> impl Iterator<Item = Region> {
    RegionGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| Region {
            rowkey: format!("{:012}", row.r_regionkey),
            regionkey: row.r_regionkey,
            name: row.r_name.to_string(),
            comment: row.r_comment.to_string(),
        })
}

pub(crate) fn nations(scale_factor: f64) -> impl Iterator<Item = Nation> {
    NationGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| Nation {
            rowkey: format!("{:012}", row.n_nationkey),
            nationkey: row.n_nationkey,
            name: row.n_name.to_string(),
            regionkey: row.n_regionkey,
            comment: row.n_comment.to_string(),
        })
}

pub(crate) fn suppliers(scale_factor: f64) -> impl Iterator<Item = Supplier> {
    SupplierGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| Supplier {
            rowkey: format!("{:012}", row.s_suppkey),
            suppkey: row.s_suppkey,
            name: row.s_name.to_string(),
            address: row.s_address.to_string(),
            nationkey: row.s_nationkey,
            phone: row.s_phone.to_string(),
            acctbal: row.s_acctbal.as_f64(),
            comment: row.s_comment,
        })
}

pub(crate) fn customers(scale_factor: f64) -> impl Iterator<Item = Customer> {
    CustomerGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| Customer {
            rowkey: format!("{:012}", row.c_custkey),
            custkey: row.c_custkey,
            name: row.c_name.to_string(),
            address: row.c_address.to_string(),
            nationkey: row.c_nationkey,
            phone: row.c_phone.to_string(),
            acctbal: row.c_acctbal.as_f64(),
            mktsegment: row.c_mktsegment.to_string(),
            comment: row.c_comment.to_string(),
        })
}

pub(crate) fn parts(scale_factor: f64) -> impl Iterator<Item = Part> {
    PartGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| Part {
            rowkey: format!("{:012}", row.p_partkey),
            partkey: row.p_partkey,
            name: row.p_name.to_string(),
            mfgr: row.p_mfgr.to_string(),
            brand: row.p_brand.to_string(),
            part_type: row.p_type.to_string(),
            size: i64::from(row.p_size),
            container: row.p_container.to_string(),
            retailprice: row.p_retailprice.as_f64(),
            comment: row.p_comment.to_string(),
        })
}

pub(crate) fn partsupps(scale_factor: f64) -> impl Iterator<Item = PartSupp> {
    PartSuppGenerator::new(scale_factor, 1, 1)
        .iter()
        .enumerate()
        .map(|(ordinal, row)| PartSupp {
            // tpchgen emits each part's suppliers in generator order rather
            // than numeric supplier-key order. The ordinal keeps the shared
            // physical identity strictly increasing for both engines.
            rowkey: format!("{ordinal:016}"),
            partkey: row.ps_partkey,
            suppkey: row.ps_suppkey,
            availqty: i64::from(row.ps_availqty),
            supplycost: row.ps_supplycost.as_f64(),
            comment: row.ps_comment.to_string(),
        })
}

pub(crate) fn orders(scale_factor: f64) -> impl Iterator<Item = Order> {
    OrderGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| Order {
            rowkey: format!("{:012}", row.o_orderkey),
            orderkey: row.o_orderkey,
            custkey: row.o_custkey,
            orderstatus: row.o_orderstatus.to_string(),
            totalprice: row.o_totalprice.as_f64(),
            orderdate: row.o_orderdate.to_string(),
            orderpriority: row.o_orderpriority.to_string(),
            clerk: row.o_clerk.to_string(),
            shippriority: i64::from(row.o_shippriority),
            comment: row.o_comment.to_string(),
        })
}

pub(crate) fn lineitems(scale_factor: f64) -> impl Iterator<Item = LineItem> {
    LineItemGenerator::new(scale_factor, 1, 1)
        .iter()
        .map(|row| LineItem {
            // Lix's initial columnar base is published only when fresh rows
            // arrive in physical identity order. Fixed-width components make
            // lexicographic identity order equal TPC-H order/line order.
            rowkey: format!("{:012}:{:02}", row.l_orderkey, row.l_linenumber),
            orderkey: row.l_orderkey,
            partkey: row.l_partkey,
            suppkey: row.l_suppkey,
            linenumber: i64::from(row.l_linenumber),
            quantity: row.l_quantity,
            extendedprice: row.l_extendedprice.as_f64(),
            discount: row.l_discount.as_f64(),
            tax: row.l_tax.as_f64(),
            returnflag: row.l_returnflag.to_string(),
            linestatus: row.l_linestatus.to_string(),
            shipdate: row.l_shipdate.to_string(),
            commitdate: row.l_commitdate.to_string(),
            receiptdate: row.l_receiptdate.to_string(),
            shipinstruct: row.l_shipinstruct.to_string(),
            shipmode: row.l_shipmode.to_string(),
            comment: row.l_comment.to_string(),
        })
}
