use std::collections::BTreeMap;

pub const LANES: usize = 32;
pub const WIDE_COLUMNS: usize = 16;
pub const WIDE_PAYLOAD_BYTES: usize = 256;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Query {
    NarrowScan,
    WideScan,
    Filter,
    Group,
    OrderLimit,
    Join,
    Projection,
}

impl Query {
    pub const ALL: [Self; 7] = [
        Self::NarrowScan,
        Self::WideScan,
        Self::Filter,
        Self::Group,
        Self::OrderLimit,
        Self::Join,
        Self::Projection,
    ];

    pub const fn label(self) -> &'static str {
        match self {
            Self::NarrowScan => "narrow_scan",
            Self::WideScan => "wide_scan",
            Self::Filter => "filtered_scan",
            Self::Group => "group_by",
            Self::OrderLimit => "order_limit",
            Self::Join => "simple_join",
            Self::Projection => "column_projection",
        }
    }

    pub const fn sql(self) -> &'static str {
        match self {
            Self::NarrowScan => {
                "SELECT id, ordinal, lane, score, active FROM forktree_olap_narrow ORDER BY ordinal"
            }
            Self::WideScan => {
                "SELECT id, ordinal, lane, score, active, c00, c01, c02, c03, c04, c05, c06, c07, c08, c09, c10, c11, c12, c13, c14, c15, payload FROM forktree_olap_wide ORDER BY ordinal"
            }
            Self::Filter => {
                "SELECT ordinal, lane, score FROM forktree_olap_narrow WHERE active = TRUE AND lane IN (7, 19) ORDER BY ordinal"
            }
            Self::Group => {
                "SELECT lane, COUNT(*) AS rows, SUM(ordinal) AS ordinal_sum, MIN(score) AS score_min, MAX(score) AS score_max FROM forktree_olap_narrow WHERE active = TRUE GROUP BY lane ORDER BY lane"
            }
            Self::OrderLimit => {
                "SELECT id, ordinal, score FROM forktree_olap_narrow WHERE active = TRUE ORDER BY score DESC, ordinal ASC LIMIT 1000"
            }
            Self::Join => {
                "SELECT n.id, n.ordinal, n.score, d.label FROM forktree_olap_narrow AS n JOIN forktree_olap_dim AS d ON n.lane = d.lane WHERE n.active = TRUE ORDER BY n.ordinal"
            }
            Self::Projection => "SELECT id, score FROM forktree_olap_wide ORDER BY ordinal",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Cell {
    Null,
    Integer(i64),
    Text(String),
    Boolean(bool),
}

#[derive(Clone, Debug)]
pub struct NarrowRow {
    pub id: String,
    pub ordinal: i64,
    pub lane: i64,
    pub score: i64,
    pub active: bool,
}

#[derive(Clone, Debug)]
pub struct WideRow {
    pub base: NarrowRow,
    pub columns: [i64; WIDE_COLUMNS],
    pub payload: String,
}

pub fn narrow_row(ordinal: usize) -> NarrowRow {
    let ordinal = i64::try_from(ordinal).expect("OLAP ordinal fits i64");
    NarrowRow {
        id: format!("/~forktree-olap/{ordinal:09}"),
        ordinal,
        lane: ordinal % LANES as i64,
        score: (ordinal * 97 + 13) % 100_003,
        active: ordinal % 3 != 0,
    }
}

pub fn wide_row(ordinal: usize) -> WideRow {
    let base = narrow_row(ordinal);
    let columns = std::array::from_fn(|column| {
        (base.ordinal * (column as i64 + 17) + column as i64 * 31) % 1_000_003
    });
    let seed = format!("wide/{ordinal:09}/");
    let payload = seed
        .chars()
        .cycle()
        .take(WIDE_PAYLOAD_BYTES)
        .collect::<String>();
    WideRow {
        base,
        columns,
        payload,
    }
}

pub fn dimension_rows() -> Vec<(i64, String)> {
    (0..LANES)
        .map(|lane| (lane as i64, format!("dimension-{lane:02}")))
        .collect()
}

pub fn encode_narrow(row: &NarrowRow) -> Vec<u8> {
    let mut encoded = Vec::with_capacity(25);
    encoded.extend_from_slice(&row.ordinal.to_be_bytes());
    encoded.extend_from_slice(&row.lane.to_be_bytes());
    encoded.extend_from_slice(&row.score.to_be_bytes());
    encoded.push(u8::from(row.active));
    encoded
}

pub fn decode_narrow(id: String, encoded: &[u8]) -> NarrowRow {
    assert_eq!(encoded.len(), 25, "canonical narrow row width");
    NarrowRow {
        id,
        ordinal: i64::from_be_bytes(encoded[0..8].try_into().expect("ordinal width")),
        lane: i64::from_be_bytes(encoded[8..16].try_into().expect("lane width")),
        score: i64::from_be_bytes(encoded[16..24].try_into().expect("score width")),
        active: match encoded[24] {
            0 => false,
            1 => true,
            other => panic!("noncanonical boolean {other}"),
        },
    }
}

pub fn encode_wide(row: &WideRow) -> Vec<u8> {
    let mut encoded = encode_narrow(&row.base);
    for value in row.columns {
        encoded.extend_from_slice(&value.to_be_bytes());
    }
    encoded.extend_from_slice(row.payload.as_bytes());
    encoded
}

pub fn decode_wide(id: String, encoded: &[u8]) -> WideRow {
    let fixed = 25 + WIDE_COLUMNS * 8;
    assert_eq!(encoded.len(), fixed + WIDE_PAYLOAD_BYTES);
    let base = decode_narrow(id, &encoded[..25]);
    let columns = std::array::from_fn(|column| {
        let start = 25 + column * 8;
        i64::from_be_bytes(
            encoded[start..start + 8]
                .try_into()
                .expect("wide column width"),
        )
    });
    let payload = String::from_utf8(encoded[fixed..].to_vec()).expect("wide payload UTF-8");
    WideRow {
        base,
        columns,
        payload,
    }
}

pub fn evaluate(
    query: Query,
    narrow: &[NarrowRow],
    wide: &[WideRow],
    dimensions: &[(i64, String)],
) -> Vec<Vec<Cell>> {
    match query {
        Query::NarrowScan => narrow.iter().map(narrow_cells).collect(),
        Query::WideScan => wide.iter().map(wide_cells).collect(),
        Query::Filter => narrow
            .iter()
            .filter(|row| row.active && matches!(row.lane, 7 | 19))
            .map(|row| {
                vec![
                    Cell::Integer(row.ordinal),
                    Cell::Integer(row.lane),
                    Cell::Integer(row.score),
                ]
            })
            .collect(),
        Query::Group => {
            let mut groups = BTreeMap::<i64, (i64, i64, i64, i64)>::new();
            for row in narrow.iter().filter(|row| row.active) {
                let group = groups.entry(row.lane).or_insert((0, 0, i64::MAX, i64::MIN));
                group.0 += 1;
                group.1 += row.ordinal;
                group.2 = group.2.min(row.score);
                group.3 = group.3.max(row.score);
            }
            groups
                .into_iter()
                .map(|(lane, (rows, ordinal_sum, score_min, score_max))| {
                    vec![
                        Cell::Integer(lane),
                        Cell::Integer(rows),
                        Cell::Integer(ordinal_sum),
                        Cell::Integer(score_min),
                        Cell::Integer(score_max),
                    ]
                })
                .collect()
        }
        Query::OrderLimit => {
            let mut selected = narrow.iter().filter(|row| row.active).collect::<Vec<_>>();
            selected.sort_unstable_by_key(|row| (std::cmp::Reverse(row.score), row.ordinal));
            selected
                .into_iter()
                .take(1_000)
                .map(|row| {
                    vec![
                        Cell::Text(row.id.clone()),
                        Cell::Integer(row.ordinal),
                        Cell::Integer(row.score),
                    ]
                })
                .collect()
        }
        Query::Join => {
            let dimensions = dimensions.iter().cloned().collect::<BTreeMap<_, _>>();
            narrow
                .iter()
                .filter(|row| row.active)
                .map(|row| {
                    vec![
                        Cell::Text(row.id.clone()),
                        Cell::Integer(row.ordinal),
                        Cell::Integer(row.score),
                        Cell::Text(
                            dimensions
                                .get(&row.lane)
                                .expect("dimension row exists")
                                .clone(),
                        ),
                    ]
                })
                .collect()
        }
        Query::Projection => wide
            .iter()
            .map(|row| {
                vec![
                    Cell::Text(row.base.id.clone()),
                    Cell::Integer(row.base.score),
                ]
            })
            .collect(),
    }
}

pub fn digest(rows: &[Vec<Cell>]) -> [u8; 32] {
    let mut hasher = blake3::Hasher::new();
    hasher.update(&(rows.len() as u64).to_be_bytes());
    for row in rows {
        hasher.update(&(row.len() as u64).to_be_bytes());
        for cell in row {
            match cell {
                Cell::Null => {
                    hasher.update(&[0]);
                }
                Cell::Integer(value) => {
                    hasher.update(&[1]);
                    hasher.update(&value.to_be_bytes());
                }
                Cell::Text(value) => {
                    hasher.update(&[2]);
                    hasher.update(&(value.len() as u64).to_be_bytes());
                    hasher.update(value.as_bytes());
                }
                Cell::Boolean(value) => {
                    hasher.update(&[3, u8::from(*value)]);
                }
            }
        }
    }
    *hasher.finalize().as_bytes()
}

fn narrow_cells(row: &NarrowRow) -> Vec<Cell> {
    vec![
        Cell::Text(row.id.clone()),
        Cell::Integer(row.ordinal),
        Cell::Integer(row.lane),
        Cell::Integer(row.score),
        Cell::Boolean(row.active),
    ]
}

fn wide_cells(row: &WideRow) -> Vec<Cell> {
    let mut cells = narrow_cells(&row.base);
    cells.extend(row.columns.into_iter().map(Cell::Integer));
    cells.push(Cell::Text(row.payload.clone()));
    cells
}

pub fn key(prefix: u8, id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(2 + id.len());
    key.push(prefix);
    key.push(b'/');
    key.extend_from_slice(id.as_bytes());
    key
}

pub fn strip_key(prefix: u8, key: &[u8]) -> String {
    assert_eq!(key.get(..2), Some([prefix, b'/'].as_slice()));
    String::from_utf8(key[2..].to_vec()).expect("OLAP key UTF-8")
}
