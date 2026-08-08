use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum KeyPart {
    Text(String),
    Integer(i64),
    Uuid(String),
}
#[derive(Clone, Debug, Eq, PartialEq)]
enum Value {
    Null,
    Text(String),
    Integer(i64),
    Uuid(String),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum KeyKind {
    Text,
    Integer,
    Uuid,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Schema {
    key_fields: Vec<(String, KeyKind)>,
    columns: Vec<String>,
}

impl Schema {
    fn new(key_fields: &[(&str, KeyKind)], columns: &[&str]) -> Self {
        Self {
            key_fields: key_fields
                .iter()
                .map(|(name, kind)| ((*name).to_owned(), *kind))
                .collect(),
            columns: columns.iter().map(|column| (*column).to_owned()).collect(),
        }
    }

    fn validate_pk(&self, row: &Row) -> Result<(), Error> {
        if row.pk.len() != self.key_fields.len() {
            return Err(Error::MalformedRow("primary-key arity"));
        }
        for ((field, kind), part) in self.key_fields.iter().zip(&row.pk) {
            let Some(value) = row.values.get(field) else {
                return Err(Error::MalformedRow("primary-key value missing"));
            };
            match (kind, part, value) {
                (KeyKind::Text, KeyPart::Text(expected), Value::Text(actual))
                    if expected == actual => {}
                (KeyKind::Integer, KeyPart::Integer(expected), Value::Integer(actual))
                    if expected == actual => {}
                (KeyKind::Uuid, KeyPart::Uuid(expected), Value::Uuid(actual)) => {
                    if parse_canonical_uuid(expected).is_none()
                        || parse_canonical_uuid(actual).is_none()
                    {
                        return Err(Error::MalformedRow(
                            "noncanonical UUID primary-key encoding",
                        ));
                    }
                    if expected != actual {
                        return Err(Error::MalformedRow("typed primary-key mismatch"));
                    }
                }
                _ => return Err(Error::MalformedRow("typed primary-key mismatch")),
            }
        }
        Ok(())
    }

    fn validate_projection(&self, projection: Option<&[String]>) -> Result<Vec<String>, Error> {
        let columns = projection
            .map(|columns| columns.to_vec())
            .unwrap_or_else(|| self.columns.clone());
        if columns
            .iter()
            .any(|column| !self.columns.iter().any(|known| known == column))
        {
            return Err(Error::InvalidProjection);
        }
        Ok(columns)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Branch {
    Global,
    Local(String),
}

impl Branch {
    fn local(name: &str) -> Self {
        Self::Local(name.to_owned())
    }

    fn rank_for(&self, requested: &str) -> Option<u8> {
        match self {
            Self::Local(name) if name == requested => Some(0),
            Self::Global => Some(1),
            Self::Local(_) => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PhysicalKind {
    EntitySnapshot,
    ObsoleteRowGroup,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Row {
    schema: String,
    branch: Branch,
    pk: Vec<KeyPart>,
    values: BTreeMap<String, Value>,
    tombstone: bool,
    kind: PhysicalKind,
}

impl Row {
    fn entity(schema: &str, branch: Branch, pk: Vec<KeyPart>, values: &[(&str, Value)]) -> Self {
        Self {
            schema: schema.to_owned(),
            branch,
            pk,
            values: values
                .iter()
                .map(|(key, value)| ((*key).to_owned(), value.clone()))
                .collect(),
            tombstone: false,
            kind: PhysicalKind::EntitySnapshot,
        }
    }

    fn tombstone(mut self) -> Self {
        self.tombstone = true;
        self
    }

    fn wrong_kind(mut self) -> Self {
        self.kind = PhysicalKind::ObsoleteRowGroup;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Query {
    schema: String,
    branch: String,
    exact_pks: Option<Vec<Vec<KeyPart>>>,
    projection: Option<Vec<String>>,
    include_tombstones: bool,
    limit: Option<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct OutputRow {
    pk: Vec<KeyPart>,
    tombstone: bool,
    columns: Vec<(String, Value)>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Error {
    MissingSchema,
    MissingBranch,
    MalformedRow(&'static str),
    WrongKind,
    InvalidPrimaryKey,
    InvalidProjection,
    ConflictingDuplicate,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

#[derive(Default)]
struct Database {
    schemas: BTreeMap<String, Schema>,
    branches: BTreeSet<String>,
    rows: Vec<Row>,
}

impl Database {
    fn with_schema(schema_key: &str, schema: Schema) -> Self {
        let mut schemas = BTreeMap::new();
        schemas.insert(schema_key.to_owned(), schema);
        let mut branches = BTreeSet::new();
        branches.insert("branch".to_owned());
        Self {
            schemas,
            branches,
            rows: Vec::new(),
        }
    }

    fn push(&mut self, row: Row) {
        self.rows.push(row);
    }

    fn validate_query(&self, query: &Query) -> Result<(&Schema, Vec<String>), Error> {
        let schema = self
            .schemas
            .get(&query.schema)
            .ok_or(Error::MissingSchema)?;
        if !self.branches.contains(&query.branch) {
            return Err(Error::MissingBranch);
        }
        let projection = schema.validate_projection(query.projection.as_deref())?;
        if let Some(pks) = &query.exact_pks {
            if pks.iter().any(|pk| pk.len() != schema.key_fields.len()) {
                return Err(Error::InvalidPrimaryKey);
            }
            for pk in pks {
                for ((_, kind), part) in schema.key_fields.iter().zip(pk) {
                    if !key_part_matches_kind(part, *kind) {
                        return Err(Error::InvalidPrimaryKey);
                    }
                }
            }
        }
        Ok((schema, projection))
    }

    fn selected_rows(&self, query: &Query) -> Result<(Schema, Vec<String>, Vec<Row>), Error> {
        let (schema, projection) = self.validate_query(query)?;
        let mut candidates = Vec::new();
        for row in self.rows.iter().filter(|row| {
            row.schema == query.schema && row.branch.rank_for(&query.branch).is_some()
        }) {
            if row.kind != PhysicalKind::EntitySnapshot {
                return Err(Error::WrongKind);
            }
            schema.validate_pk(row)?;
            if let Some(pks) = &query.exact_pks {
                if !pks.iter().any(|pk| pk == &row.pk) {
                    continue;
                }
            }
            candidates.push(row.clone());
        }
        Ok((schema.clone(), projection, candidates))
    }

    fn execute_direct(&self, query: &Query) -> Result<Vec<OutputRow>, Error> {
        let (_schema, projection, candidates) = self.selected_rows(query)?;
        let mut winners = BTreeMap::<Vec<KeyPart>, (u8, Row)>::new();
        for row in candidates {
            let rank = row.branch.rank_for(&query.branch).expect("filtered branch");
            match winners.entry(row.pk.clone()) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert((rank, row));
                }
                std::collections::btree_map::Entry::Occupied(mut entry) => {
                    let (existing_rank, existing) = entry.get_mut();
                    if *existing_rank == rank {
                        if !same_payload(existing, &row) {
                            return Err(Error::ConflictingDuplicate);
                        }
                    } else if rank < *existing_rank {
                        *existing_rank = rank;
                        *existing = row;
                    }
                }
            }
        }
        materialize_output(
            winners.into_values().map(|(_, row)| row),
            &projection,
            query,
        )
    }

    /// Independent reference for the pre-deletion row-group planner. It uses
    /// a sorted vector/grouping algorithm rather than the direct map above.
    fn execute_deleted_row_group_reference(&self, query: &Query) -> Result<Vec<OutputRow>, Error> {
        let (_schema, projection, mut candidates) = self.selected_rows(query)?;
        candidates.sort_by(|left, right| {
            left.pk.cmp(&right.pk).then_with(|| {
                left.branch
                    .rank_for(&query.branch)
                    .cmp(&right.branch.rank_for(&query.branch))
            })
        });
        let mut winners = Vec::new();
        let mut index = 0;
        while index < candidates.len() {
            let pk = candidates[index].pk.clone();
            let first = candidates[index].clone();
            let first_rank = first
                .branch
                .rank_for(&query.branch)
                .expect("filtered branch");
            index += 1;
            while index < candidates.len() && candidates[index].pk == pk {
                let duplicate = &candidates[index];
                let duplicate_rank = duplicate
                    .branch
                    .rank_for(&query.branch)
                    .expect("filtered branch");
                if duplicate_rank == first_rank && !same_payload(&first, duplicate) {
                    return Err(Error::ConflictingDuplicate);
                }
                index += 1;
            }
            winners.push(first);
        }
        materialize_output(winners, &projection, query)
    }
}

fn key_part_matches_kind(part: &KeyPart, kind: KeyKind) -> bool {
    match (part, kind) {
        (KeyPart::Text(_), KeyKind::Text) | (KeyPart::Integer(_), KeyKind::Integer) => true,
        (KeyPart::Uuid(value), KeyKind::Uuid) => parse_canonical_uuid(value).is_some(),
        _ => false,
    }
}

/// Parses the only UUID spelling accepted by the public EntityPk contract:
/// lowercase hexadecimal, canonical 8-4-4-4-12 grouping.  Keeping this
/// parser dependency-free makes the standalone `rustc -D warnings` model
/// enforce the same lexical boundary as the production typed identity layer.
fn parse_canonical_uuid(value: &str) -> Option<[u8; 16]> {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return None;
    }
    let mut parsed = [0_u8; 16];
    let mut nibble_index = 0_usize;
    for (index, byte) in bytes.iter().copied().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if byte != b'-' {
                return None;
            }
            continue;
        }
        let nibble = match byte {
            b'0'..=b'9' => byte - b'0',
            b'a'..=b'f' => byte - b'a' + 10,
            _ => return None,
        };
        let output = nibble_index / 2;
        if nibble_index % 2 == 0 {
            parsed[output] = nibble << 4;
        } else {
            parsed[output] |= nibble;
        }
        nibble_index += 1;
    }
    (nibble_index == 32).then_some(parsed)
}

fn same_payload(left: &Row, right: &Row) -> bool {
    left.tombstone == right.tombstone && left.values == right.values && left.kind == right.kind
}

fn materialize_output(
    rows: impl IntoIterator<Item = Row>,
    projection: &[String],
    query: &Query,
) -> Result<Vec<OutputRow>, Error> {
    let mut output = rows
        .into_iter()
        .filter(|row| query.include_tombstones || !row.tombstone)
        .map(|row| OutputRow {
            pk: row.pk,
            tombstone: row.tombstone,
            columns: projection
                .iter()
                .map(|column| {
                    (
                        column.clone(),
                        row.values.get(column).cloned().unwrap_or(Value::Null),
                    )
                })
                .collect(),
        })
        .collect::<Vec<_>>();
    if let Some(limit) = query.limit {
        output.truncate(limit);
    }
    Ok(output)
}

fn text(value: &str) -> Value {
    Value::Text(value.to_owned())
}

fn pk(value: &str) -> Vec<KeyPart> {
    vec![KeyPart::Text(value.to_owned())]
}

fn query(projection: Option<&[&str]>) -> Query {
    Query {
        schema: "users".to_owned(),
        branch: "branch".to_owned(),
        exact_pks: None,
        projection: projection
            .map(|columns| columns.iter().map(|column| (*column).to_owned()).collect()),
        include_tombstones: false,
        limit: None,
    }
}

fn fixture() -> Database {
    let schema = Schema::new(&[("id", KeyKind::Text)], &["id", "name", "note", "age"]);
    let mut database = Database::with_schema("users", schema);
    database.push(Row::entity(
        "users",
        Branch::Global,
        pk("a"),
        &[
            ("id", text("a")),
            ("name", text("global-a")),
            ("note", Value::Null),
        ],
    ));
    database.push(Row::entity(
        "users",
        Branch::Global,
        pk("b"),
        &[("id", text("b")), ("name", text("global-b"))],
    ));
    database.push(Row::entity(
        "users",
        Branch::Global,
        pk("d"),
        &[("id", text("d")), ("name", text("global-d"))],
    ));
    database.push(Row::entity(
        "users",
        Branch::local("branch"),
        pk("a"),
        &[("id", text("a")), ("name", text("local-a"))],
    ));
    database.push(
        Row::entity(
            "users",
            Branch::local("branch"),
            pk("b"),
            &[("id", text("b")), ("name", text("deleted-b"))],
        )
        .tombstone(),
    );
    database.push(Row::entity(
        "users",
        Branch::local("branch"),
        pk("c"),
        &[
            ("id", text("c")),
            ("name", text("local-c")),
            ("note", Value::Null),
        ],
    ));
    database
}

#[test]
fn exact_identity_overlay_and_null_are_distinct_from_tombstone() {
    let database = fixture();
    let output = database.execute_direct(&query(None)).unwrap();
    assert_eq!(
        output.iter().map(|row| row.pk.clone()).collect::<Vec<_>>(),
        vec![pk("a"), pk("c"), pk("d")]
    );
    assert_eq!(output[0].columns[1], ("name".to_owned(), text("local-a")));
    assert_eq!(output[1].columns[2], ("note".to_owned(), Value::Null));
    assert!(!output.iter().any(|row| row.pk == pk("b")));

    let mut include = query(None);
    include.include_tombstones = true;
    let output = database.execute_direct(&include).unwrap();
    assert!(output.iter().any(|row| row.pk == pk("b") && row.tombstone));
}

#[test]
fn ordering_projection_and_limit_apply_after_overlay_canonicalization() {
    let mut database = fixture();
    database.push(Row::entity(
        "users",
        Branch::Global,
        pk("e"),
        &[("id", text("e")), ("name", text("global-e"))],
    ));
    let mut request = query(Some(&["name", "id"]));
    request.limit = Some(2);
    let output = database.execute_direct(&request).unwrap();
    assert_eq!(output.len(), 2);
    assert_eq!(output[0].pk, pk("a"));
    assert_eq!(output[1].pk, pk("c"));
    assert_eq!(output[0].columns[0], ("name".to_owned(), text("local-a")));
    assert_eq!(output[0].columns[1], ("id".to_owned(), text("a")));
}

#[test]
fn exact_primary_key_filter_is_typed_and_does_not_change_ordering() {
    let mut database = Database::with_schema(
        "orders",
        Schema::new(&[("id", KeyKind::Integer)], &["id", "label"]),
    );
    database.push(Row::entity(
        "orders",
        Branch::Global,
        vec![KeyPart::Integer(2)],
        &[("id", Value::Integer(2)), ("label", text("two"))],
    ));
    database.push(Row::entity(
        "orders",
        Branch::Global,
        vec![KeyPart::Integer(1)],
        &[("id", Value::Integer(1)), ("label", text("one"))],
    ));
    let mut request = Query {
        schema: "orders".to_owned(),
        branch: "branch".to_owned(),
        exact_pks: Some(vec![vec![KeyPart::Integer(2)]]),
        projection: Some(vec!["id".to_owned()]),
        include_tombstones: false,
        limit: None,
    };
    assert_eq!(
        database.execute_direct(&request).unwrap()[0].pk,
        vec![KeyPart::Integer(2)]
    );
    request.exact_pks = Some(vec![vec![KeyPart::Text("2".to_owned())]]);
    assert_eq!(
        database.execute_direct(&request),
        Err(Error::InvalidPrimaryKey)
    );

    let mut accounts = Database::with_schema(
        "accounts",
        Schema::new(&[("id", KeyKind::Uuid)], &["id", "label"]),
    );
    let uuid = "01920000-0000-7000-8000-000000000001".to_owned();
    accounts.push(Row::entity(
        "accounts",
        Branch::Global,
        vec![KeyPart::Uuid(uuid.clone())],
        &[
            ("id", Value::Uuid(uuid.clone())),
            ("label", text("account")),
        ],
    ));
    let uuid_request = Query {
        schema: "accounts".to_owned(),
        branch: "branch".to_owned(),
        exact_pks: Some(vec![vec![KeyPart::Uuid(uuid)]]),
        projection: Some(vec!["id".to_owned()]),
        include_tombstones: false,
        limit: None,
    };
    assert_eq!(accounts.execute_direct(&uuid_request).unwrap().len(), 1);

    let mut noncanonical_query = uuid_request.clone();
    noncanonical_query.exact_pks = Some(vec![vec![KeyPart::Uuid(
        "01920000-0000-7000-8000-00000000000A".to_owned(),
    )]]);
    assert_eq!(
        accounts.execute_direct(&noncanonical_query),
        Err(Error::InvalidPrimaryKey)
    );

    let malformed_uuid = "not-a-uuid".to_owned();
    let mut malformed_accounts = Database::with_schema(
        "accounts",
        Schema::new(&[("id", KeyKind::Uuid)], &["id", "label"]),
    );
    malformed_accounts.push(Row::entity(
        "accounts",
        Branch::Global,
        vec![KeyPart::Uuid(malformed_uuid.clone())],
        &[
            ("id", Value::Uuid(malformed_uuid)),
            ("label", text("malformed")),
        ],
    ));
    assert_eq!(
        malformed_accounts.execute_direct(&uuid_request),
        Err(Error::MalformedRow("noncanonical UUID primary-key encoding"))
    );
}

#[test]
fn schema_projection_and_missing_scope_fail_closed() {
    let database = fixture();
    let mut wrong_projection = query(Some(&["not_a_column"]));
    assert_eq!(
        database.execute_direct(&wrong_projection),
        Err(Error::InvalidProjection)
    );
    wrong_projection.schema = "missing".to_owned();
    assert_eq!(
        database.execute_direct(&wrong_projection),
        Err(Error::MissingSchema)
    );
    wrong_projection.schema = "users".to_owned();
    wrong_projection.branch = "missing-branch".to_owned();
    assert_eq!(
        database.execute_direct(&wrong_projection),
        Err(Error::MissingBranch)
    );
}

#[test]
fn malformed_missing_and_wrong_kind_rows_fail_closed() {
    let mut malformed = fixture();
    malformed.push(Row::entity(
        "users",
        Branch::local("branch"),
        pk("malformed"),
        &[("id", text("different")), ("name", text("bad"))],
    ));
    assert_eq!(
        malformed.execute_direct(&query(None)),
        Err(Error::MalformedRow("typed primary-key mismatch"))
    );

    let mut wrong_kind = fixture();
    wrong_kind.push(
        Row::entity(
            "users",
            Branch::local("branch"),
            pk("wrong-kind"),
            &[("id", text("wrong-kind")), ("name", text("bad"))],
        )
        .wrong_kind(),
    );
    assert_eq!(
        wrong_kind.execute_direct(&query(None)),
        Err(Error::WrongKind)
    );
}

#[test]
fn identical_duplicates_collapse_but_conflicting_authority_fails_closed() {
    let mut identical = fixture();
    let duplicate = identical
        .rows
        .iter()
        .find(|row| row.pk == pk("a"))
        .unwrap()
        .clone();
    identical.push(duplicate);
    assert!(identical.execute_direct(&query(None)).is_ok());

    let mut conflicting = fixture();
    conflicting.push(Row::entity(
        "users",
        Branch::local("branch"),
        pk("a"),
        &[("id", text("a")), ("name", text("conflict"))],
    ));
    assert_eq!(
        conflicting.execute_direct(&query(None)),
        Err(Error::ConflictingDuplicate)
    );
}

#[test]
fn direct_snapshot_execution_matches_deleted_row_group_reference_for_public_shapes() {
    let database = fixture();
    let requests = [
        query(None),
        query(Some(&["id", "name"])),
        {
            let mut request = query(None);
            request.limit = Some(1);
            request
        },
        {
            let mut request = query(Some(&["note", "id"]));
            request.include_tombstones = true;
            request
        },
        {
            let mut request = query(None);
            request.exact_pks = Some(vec![pk("a"), pk("d")]);
            request
        },
    ];
    for request in requests {
        assert_eq!(
            database.execute_direct(&request),
            database.execute_deleted_row_group_reference(&request),
            "direct execution diverged from the obsolete row-group reference for {request:?}"
        );
    }
}

#[test]
fn all_adapter_controls_share_the_same_public_result_and_read_only_contract() {
    let database = fixture();
    let request = query(Some(&["id", "name", "note"]));
    let expected = database.execute_direct(&request).unwrap();
    for backend in ["memory", "rocksdb", "slatedb"] {
        assert_eq!(
            database.execute_direct(&request).unwrap(),
            expected,
            "backend={backend}"
        );
        assert_eq!(
            database
                .execute_deleted_row_group_reference(&request)
                .unwrap(),
            expected
        );
    }
}
