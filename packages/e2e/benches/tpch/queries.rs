//! Standard-substitution TPC-H queries adapted to Lix's current common types.
//!
//! Dates are ISO strings, so interval endpoints are precomputed and year
//! extraction uses `substr`. Q15 uses a CTE rather than session-mutating views.

#[derive(Clone, Debug)]
pub(crate) struct Query {
    pub(crate) number: u8,
    pub(crate) sql: String,
}

const SQL: [&str; 22] = [
    r#"SELECT l_returnflag, l_linestatus,
              SUM(l_quantity) AS sum_qty,
              SUM(l_extendedprice) AS sum_base_price,
              SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
              SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
              AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price,
              AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
       FROM lineitem WHERE l_shipdate <= '1998-09-02'
       GROUP BY l_returnflag, l_linestatus
       ORDER BY l_returnflag, l_linestatus"#,
    r#"SELECT s_acctbal, s_name, n_name, p_partkey, p_mfgr,
              s_address, s_phone, s_comment
       FROM part, supplier, partsupp, nation, region
       WHERE p_partkey = ps_partkey AND s_suppkey = ps_suppkey
         AND p_size = 15 AND p_type LIKE '%BRASS'
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
         AND r_name = 'EUROPE'
         AND ps_supplycost = (
             SELECT MIN(ps2.ps_supplycost)
             FROM partsupp ps2, supplier s2, nation n2, region r2
             WHERE p_partkey = ps2.ps_partkey
               AND s2.s_suppkey = ps2.ps_suppkey
               AND s2.s_nationkey = n2.n_nationkey
               AND n2.n_regionkey = r2.r_regionkey AND r2.r_name = 'EUROPE')
       ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
       LIMIT 100"#,
    r#"SELECT l_orderkey,
              SUM(l_extendedprice * (1 - l_discount)) AS revenue,
              o_orderdate, o_shippriority
       FROM customer, orders, lineitem
       WHERE c_mktsegment = 'BUILDING' AND c_custkey = o_custkey
         AND l_orderkey = o_orderkey AND o_orderdate < '1995-03-15'
         AND l_shipdate > '1995-03-15'
       GROUP BY l_orderkey, o_orderdate, o_shippriority
       ORDER BY revenue DESC, o_orderdate, l_orderkey
       LIMIT 10"#,
    r#"SELECT o_orderpriority, COUNT(*) AS order_count
       FROM orders
       WHERE o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'
         AND EXISTS (SELECT 1 FROM lineitem
                     WHERE l_orderkey = o_orderkey
                       AND l_commitdate < l_receiptdate)
       GROUP BY o_orderpriority ORDER BY o_orderpriority"#,
    r#"SELECT n_name, SUM(l_extendedprice * (1 - l_discount)) AS revenue
       FROM customer, orders, lineitem, supplier, nation, region
       WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
         AND l_suppkey = s_suppkey AND c_nationkey = s_nationkey
         AND s_nationkey = n_nationkey AND n_regionkey = r_regionkey
         AND r_name = 'ASIA' AND o_orderdate >= '1994-01-01'
         AND o_orderdate < '1995-01-01'
       GROUP BY n_name ORDER BY revenue DESC"#,
    r#"SELECT SUM(l_extendedprice * l_discount) AS revenue
       FROM lineitem
       WHERE l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'
         AND l_discount BETWEEN 0.05 AND 0.07 AND l_quantity < 24"#,
    r#"SELECT supp_nation, cust_nation, l_year, SUM(volume) AS revenue
       FROM (SELECT n1.n_name AS supp_nation, n2.n_name AS cust_nation,
                    substr(l_shipdate, 1, 4) AS l_year,
                    l_extendedprice * (1 - l_discount) AS volume
             FROM supplier, lineitem, orders, customer, nation n1, nation n2
             WHERE s_suppkey = l_suppkey AND o_orderkey = l_orderkey
               AND c_custkey = o_custkey AND s_nationkey = n1.n_nationkey
               AND c_nationkey = n2.n_nationkey
               AND ((n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY')
                 OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE'))
               AND l_shipdate BETWEEN '1995-01-01' AND '1996-12-31') shipping
       GROUP BY supp_nation, cust_nation, l_year
       ORDER BY supp_nation, cust_nation, l_year"#,
    r#"SELECT o_year,
              SUM(CASE WHEN nation = 'BRAZIL' THEN volume ELSE 0 END)
                / SUM(volume) AS mkt_share
       FROM (SELECT substr(o_orderdate, 1, 4) AS o_year,
                    l_extendedprice * (1 - l_discount) AS volume,
                    n2.n_name AS nation
             FROM part, supplier, lineitem, orders, customer,
                  nation n1, nation n2, region
             WHERE p_partkey = l_partkey AND s_suppkey = l_suppkey
               AND l_orderkey = o_orderkey AND o_custkey = c_custkey
               AND c_nationkey = n1.n_nationkey
               AND n1.n_regionkey = r_regionkey AND r_name = 'AMERICA'
               AND s_nationkey = n2.n_nationkey
               AND o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'
               AND p_type = 'ECONOMY ANODIZED STEEL') all_nations
       GROUP BY o_year ORDER BY o_year"#,
    r#"SELECT nation, o_year, SUM(amount) AS sum_profit
       FROM (SELECT n_name AS nation, substr(o_orderdate, 1, 4) AS o_year,
                    l_extendedprice * (1 - l_discount)
                      - ps_supplycost * l_quantity AS amount
             FROM part, supplier, lineitem, partsupp, orders, nation
             WHERE s_suppkey = l_suppkey AND ps_suppkey = l_suppkey
               AND ps_partkey = l_partkey AND p_partkey = l_partkey
               AND o_orderkey = l_orderkey AND s_nationkey = n_nationkey
               AND p_name LIKE '%green%') profit
       GROUP BY nation, o_year ORDER BY nation, o_year DESC"#,
    r#"SELECT c_custkey, c_name,
              SUM(l_extendedprice * (1 - l_discount)) AS revenue,
              c_acctbal, n_name, c_address, c_phone, c_comment
       FROM customer, orders, lineitem, nation
       WHERE c_custkey = o_custkey AND l_orderkey = o_orderkey
         AND o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'
         AND l_returnflag = 'R' AND c_nationkey = n_nationkey
       GROUP BY c_custkey, c_name, c_acctbal, c_phone, n_name,
                c_address, c_comment
       ORDER BY revenue DESC, c_custkey
       LIMIT 20"#,
    r#"SELECT ps_partkey, SUM(ps_supplycost * ps_availqty) AS value
       FROM partsupp, supplier, nation
       WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
         AND n_name = 'GERMANY'
       GROUP BY ps_partkey
       HAVING SUM(ps_supplycost * ps_availqty) >
         (SELECT SUM(ps_supplycost * ps_availqty) * {Q11_FRACTION}
          FROM partsupp, supplier, nation
          WHERE ps_suppkey = s_suppkey AND s_nationkey = n_nationkey
            AND n_name = 'GERMANY')
       ORDER BY value DESC"#,
    r#"SELECT l_shipmode,
              SUM(CASE WHEN o_orderpriority = '1-URGENT'
                         OR o_orderpriority = '2-HIGH' THEN 1 ELSE 0 END)
                AS high_line_count,
              SUM(CASE WHEN o_orderpriority <> '1-URGENT'
                         AND o_orderpriority <> '2-HIGH' THEN 1 ELSE 0 END)
                AS low_line_count
       FROM orders, lineitem
       WHERE o_orderkey = l_orderkey AND l_shipmode IN ('MAIL', 'SHIP')
         AND l_commitdate < l_receiptdate AND l_shipdate < l_commitdate
         AND l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'
       GROUP BY l_shipmode ORDER BY l_shipmode"#,
    r#"SELECT c_count, COUNT(*) AS custdist
       FROM (SELECT c_custkey, COUNT(o_orderkey) AS c_count
             FROM customer LEFT OUTER JOIN orders
               ON c_custkey = o_custkey
              AND o_comment NOT LIKE '%special%requests%'
             GROUP BY c_custkey) c_orders
       GROUP BY c_count ORDER BY custdist DESC, c_count DESC"#,
    r#"SELECT 100.0 * SUM(CASE WHEN p_type LIKE 'PROMO%'
                                THEN l_extendedprice * (1 - l_discount)
                                ELSE 0 END)
                       / SUM(l_extendedprice * (1 - l_discount)) AS promo_revenue
       FROM lineitem, part
       WHERE l_partkey = p_partkey AND l_shipdate >= '1995-09-01'
         AND l_shipdate < '1995-10-01'"#,
    r#"WITH revenue AS (
         SELECT l_suppkey AS supplier_no,
                SUM(l_extendedprice * (1 - l_discount)) AS total_revenue
         FROM lineitem
         WHERE l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'
         GROUP BY l_suppkey)
       SELECT s_suppkey, s_name, s_address, s_phone, total_revenue
       FROM supplier, revenue
       WHERE s_suppkey = supplier_no
         AND total_revenue = (SELECT MAX(total_revenue) FROM revenue)
       ORDER BY s_suppkey"#,
    r#"SELECT p_brand, p_type, p_size,
              COUNT(DISTINCT ps_suppkey) AS supplier_cnt
       FROM partsupp, part
       WHERE p_partkey = ps_partkey AND p_brand <> 'Brand#45'
         AND p_type NOT LIKE 'MEDIUM POLISHED%'
         AND p_size IN (49, 14, 23, 45, 19, 3, 36, 9)
         AND ps_suppkey NOT IN
           (SELECT s_suppkey FROM supplier
            WHERE s_comment LIKE '%Customer%Complaints%')
       GROUP BY p_brand, p_type, p_size
       ORDER BY supplier_cnt DESC, p_brand, p_type, p_size"#,
    r#"SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
       FROM lineitem, part
       WHERE p_partkey = l_partkey AND p_brand = 'Brand#23'
         AND p_container = 'MED BOX'
         AND l_quantity < (SELECT 0.2 * AVG(l2.l_quantity)
                           FROM lineitem l2
                           WHERE l2.l_partkey = p_partkey)"#,
    r#"SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
              SUM(l_quantity) AS sum_quantity
       FROM customer, orders, lineitem
       WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem
                            GROUP BY l_orderkey
                            HAVING SUM(l_quantity) > 300)
         AND c_custkey = o_custkey AND o_orderkey = l_orderkey
       GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
       ORDER BY o_totalprice DESC, o_orderdate, o_orderkey
       LIMIT 100"#,
    r#"SELECT SUM(l_extendedprice * (1 - l_discount)) AS revenue
       FROM lineitem, part
       WHERE (p_partkey = l_partkey AND p_brand = 'Brand#12'
              AND p_container IN ('SM CASE','SM BOX','SM PACK','SM PKG')
              AND l_quantity BETWEEN 1 AND 11 AND p_size BETWEEN 1 AND 5
              AND l_shipmode IN ('AIR','AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')
          OR (p_partkey = l_partkey AND p_brand = 'Brand#23'
              AND p_container IN ('MED BAG','MED BOX','MED PKG','MED PACK')
              AND l_quantity BETWEEN 10 AND 20 AND p_size BETWEEN 1 AND 10
              AND l_shipmode IN ('AIR','AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')
          OR (p_partkey = l_partkey AND p_brand = 'Brand#34'
              AND p_container IN ('LG CASE','LG BOX','LG PACK','LG PKG')
              AND l_quantity BETWEEN 20 AND 30 AND p_size BETWEEN 1 AND 15
              AND l_shipmode IN ('AIR','AIR REG')
              AND l_shipinstruct = 'DELIVER IN PERSON')"#,
    r#"SELECT s_name, s_address
       FROM supplier, nation
       WHERE s_suppkey IN (
         SELECT ps_suppkey FROM partsupp
         WHERE ps_partkey IN
           (SELECT p_partkey FROM part WHERE p_name LIKE 'forest%')
           AND ps_availqty >
             (SELECT 0.5 * SUM(l_quantity) FROM lineitem
              WHERE l_partkey = ps_partkey AND l_suppkey = ps_suppkey
                AND l_shipdate >= '1994-01-01'
                AND l_shipdate < '1995-01-01'))
         AND s_nationkey = n_nationkey AND n_name = 'CANADA'
       ORDER BY s_name"#,
    r#"SELECT s_name, COUNT(*) AS numwait
       FROM supplier, lineitem l1, orders, nation
       WHERE s_suppkey = l1.l_suppkey AND o_orderkey = l1.l_orderkey
         AND o_orderstatus = 'F' AND l1.l_receiptdate > l1.l_commitdate
         AND EXISTS (SELECT 1 FROM lineitem l2
                     WHERE l2.l_orderkey = l1.l_orderkey
                       AND l2.l_suppkey <> l1.l_suppkey)
         AND NOT EXISTS (SELECT 1 FROM lineitem l3
                         WHERE l3.l_orderkey = l1.l_orderkey
                           AND l3.l_suppkey <> l1.l_suppkey
                           AND l3.l_receiptdate > l3.l_commitdate)
         AND s_nationkey = n_nationkey AND n_name = 'SAUDI ARABIA'
       GROUP BY s_name ORDER BY numwait DESC, s_name
       LIMIT 100"#,
    r#"SELECT cntrycode, COUNT(*) AS numcust, SUM(c_acctbal) AS totacctbal
       FROM (SELECT substr(c_phone, 1, 2) AS cntrycode, c_acctbal
             FROM customer
             WHERE substr(c_phone, 1, 2) IN
                   ('13','31','23','29','30','18','17')
               AND c_acctbal >
                 (SELECT AVG(c2.c_acctbal) FROM customer c2
                  WHERE c2.c_acctbal > 0.0
                    AND substr(c2.c_phone, 1, 2) IN
                        ('13','31','23','29','30','18','17'))
               AND NOT EXISTS (SELECT 1 FROM orders
                               WHERE o_custkey = c_custkey)) custsale
       GROUP BY cntrycode ORDER BY cntrycode"#,
];

pub(crate) fn queries(scale_factor: f64) -> Vec<Query> {
    let q11_fraction = 0.0001 / scale_factor;
    SQL.iter()
        .enumerate()
        .map(|(index, sql)| Query {
            number: u8::try_from(index + 1).expect("TPC-H query number fits in u8"),
            sql: sql.replace("{Q11_FRACTION}", &format!("{q11_fraction:.17}")),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #[test]
    fn exposes_every_standard_query_in_order() {
        let queries = super::queries(1.0);
        assert_eq!(queries.len(), 22);
        assert_eq!(
            queries.iter().map(|query| query.number).collect::<Vec<_>>(),
            (1..=22).collect::<Vec<_>>()
        );
    }

    #[test]
    fn q11_fraction_scales_with_the_dataset() {
        assert!(
            super::queries(0.01)[10]
                .sql
                .contains("* 0.01000000000000000")
        );
        assert!(
            super::queries(1.0)[10]
                .sql
                .contains("* 0.00010000000000000")
        );
    }
}
