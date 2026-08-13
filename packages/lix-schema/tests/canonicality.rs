//! Canonicality of the Schema v1 typed-row body encoding.
//!
//! The property under test: **one logical value produces exactly one byte
//! string**. Every test below encodes the same logical value by *two different
//! construction paths* and asserts the bytes are equal. Each such test is
//! paired with a **positive control** that differs, so a test cannot pass by
//! producing an empty or constant encoding — a null is not self-validating.
//!
//! Run nonce: vlayout-8813.

use lix_schema::value_layout::{
    BodyColumn, BodyKind, BodyValue, canonical_float8_bits, decode_body, encode_body, encode_one,
};
use serde_json::json;

fn column(kind: BodyKind) -> BodyColumn {
    BodyColumn {
        kind,
        nullable: true,
    }
}

/// Encode `value` through the single-column body path.
fn bytes(kind: BodyKind, value: &BodyValue) -> Vec<u8> {
    encode_one(kind, value).expect("value must encode")
}

/// Encode `value` through the multi-column body path, then project the body
/// back out by encoding the same value alone. Two *structurally different*
/// encoder invocations that must agree on the value's own bytes.
fn bytes_via_row(kind: BodyKind, value: &BodyValue) -> Vec<u8> {
    let mut output = Vec::new();
    encode_body(&[column(kind)], std::slice::from_ref(value), &mut output)
        .expect("value must encode");
    output
}

// ---------------------------------------------------------------------------
// boolean
// ---------------------------------------------------------------------------

#[test]
fn canonicality_boolean_has_one_image_per_value() {
    // Path A: Rust literal. Path B: through serde_json.
    let literal = BodyValue::Boolean(true);
    let parsed = match serde_json::from_str::<serde_json::Value>("true").unwrap() {
        serde_json::Value::Bool(flag) => BodyValue::Boolean(flag),
        other => panic!("expected a bool, got {other:?}"),
    };
    assert_eq!(
        bytes(BodyKind::Boolean, &literal),
        bytes(BodyKind::Boolean, &parsed),
        "the two construction paths for `true` must agree"
    );
    assert_eq!(
        bytes(BodyKind::Boolean, &literal),
        bytes_via_row(BodyKind::Boolean, &parsed),
        "single-value and row encoders must agree"
    );

    // POSITIVE CONTROL: true and false must differ, so equality above is not
    // the trivial equality of two empty encodings.
    assert_ne!(
        bytes(BodyKind::Boolean, &BodyValue::Boolean(true)),
        bytes(BodyKind::Boolean, &BodyValue::Boolean(false)),
        "positive control: true and false must encode differently"
    );
}

#[test]
fn canonicality_boolean_admits_exactly_two_bytes() {
    let plan = [BodyColumn {
        kind: BodyKind::Boolean,
        nullable: false,
    }];
    // Header byte, no bitmap (column is NOT NULL), then the one payload byte.
    let mut body = encode_one_not_null(BodyKind::Boolean, &BodyValue::Boolean(true));
    assert_eq!(*body.last().unwrap(), 0x01);
    for illegal in [0x02u8, 0x7f, 0x80, 0xff] {
        *body.last_mut().unwrap() = illegal;
        assert!(
            decode_body(&plan, &body).is_err(),
            "byte {illegal:#04x} must not decode as a boolean; if it did, \
             two byte strings would mean one value"
        );
    }
    *body.last_mut().unwrap() = 0x00;
    assert!(
        decode_body(&plan, &body).is_ok(),
        "0x00 must decode as false"
    );
}

fn encode_one_not_null(kind: BodyKind, value: &BodyValue) -> Vec<u8> {
    let mut output = Vec::new();
    encode_body(
        &[BodyColumn {
            kind,
            nullable: false,
        }],
        std::slice::from_ref(value),
        &mut output,
    )
    .expect("value must encode");
    output
}

// ---------------------------------------------------------------------------
// int8
// ---------------------------------------------------------------------------

#[test]
fn canonicality_int8_has_one_image_per_value() {
    // Path A: literal. Path B: arithmetic. Path C: JSON parse.
    let literal = BodyValue::Int8(-9_007_199_254_740_993);
    let computed = BodyValue::Int8(-9_007_199_254_740_992 - 1);
    let parsed = BodyValue::Int8(
        serde_json::from_str::<serde_json::Value>("-9007199254740993")
            .unwrap()
            .as_i64()
            .expect("int8 must round-trip through JSON as i64"),
    );
    assert_eq!(
        bytes(BodyKind::Int8, &literal),
        bytes(BodyKind::Int8, &computed)
    );
    assert_eq!(
        bytes(BodyKind::Int8, &literal),
        bytes(BodyKind::Int8, &parsed)
    );

    // Endianness is pinned, not merely self-consistent: big-endian two's
    // complement, matching `typed_slots`' `i64::from_be_bytes`.
    let one = encode_one_not_null(BodyKind::Int8, &BodyValue::Int8(1));
    assert_eq!(&one[one.len() - 8..], &[0, 0, 0, 0, 0, 0, 0, 1]);

    // POSITIVE CONTROL
    assert_ne!(
        bytes(BodyKind::Int8, &BodyValue::Int8(1)),
        bytes(BodyKind::Int8, &BodyValue::Int8(-1)),
        "positive control: 1 and -1 must encode differently"
    );
    assert_ne!(
        bytes(BodyKind::Int8, &BodyValue::Int8(1)),
        bytes(BodyKind::Int8, &BodyValue::Int8(1 << 56)),
        "positive control: a byte-swapped pair must not collide"
    );
}

#[test]
fn canonicality_timestamptz_has_one_image_per_instant() {
    let plan = [BodyColumn {
        kind: BodyKind::Timestamptz,
        nullable: false,
    }];
    let instant = 1_786_647_721_123_456_i64;
    let mut first = Vec::new();
    let mut second = Vec::new();
    encode_body(&plan, &[BodyValue::Timestamptz(instant)], &mut first).unwrap();
    encode_body(&plan, &[BodyValue::Timestamptz(instant)], &mut second).unwrap();
    assert_eq!(first, second);

    let mut different = Vec::new();
    encode_body(
        &plan,
        &[BodyValue::Timestamptz(instant + 1)],
        &mut different,
    )
    .unwrap();
    assert_ne!(first, different);
    assert_eq!(
        decode_body(&plan, &first).unwrap(),
        vec![BodyValue::Timestamptz(instant)]
    );
}

// ---------------------------------------------------------------------------
// float8  --  the -0.0 and NaN questions
// ---------------------------------------------------------------------------

#[test]
fn canonicality_double_erases_the_sign_of_zero() {
    // -0.0 == 0.0 under IEEE-754 and under Rust's PartialEq, so they are ONE
    // logical value. Their bit patterns differ. If the encoder emitted both,
    // one value would have two content addresses.
    assert_eq!(-0.0_f64, 0.0_f64, "premise: -0.0 and +0.0 are one value");
    assert_ne!(
        (-0.0_f64).to_be_bytes(),
        0.0_f64.to_be_bytes(),
        "premise: their raw IEEE-754 images differ, so this needs canonicalising"
    );

    // Path A: literal +0.0. Path B: literal -0.0. Path C: computed -1.0 * 0.0.
    // Path D: parsed from the JSON text "-0.0".
    let paths = [
        BodyValue::Float8(0.0),
        BodyValue::Float8(-0.0),
        BodyValue::Float8(-1.0 * 0.0),
        BodyValue::Float8(
            serde_json::from_str::<serde_json::Value>("-0.0")
                .unwrap()
                .as_f64()
                .unwrap(),
        ),
    ];
    let expected = bytes(BodyKind::Float8, &paths[0]);
    for path in &paths {
        assert_eq!(
            bytes(BodyKind::Float8, path),
            expected,
            "every construction path for zero must produce one byte string"
        );
    }
    assert_eq!(canonical_float8_bits(-0.0).unwrap(), [0; 8]);

    // POSITIVE CONTROL: the canonicalisation must not be a constant function.
    assert_ne!(
        bytes(BodyKind::Float8, &BodyValue::Float8(1.0)),
        bytes(BodyKind::Float8, &BodyValue::Float8(-1.0)),
        "positive control: 1.0 and -1.0 must still differ"
    );
    assert_ne!(
        bytes(BodyKind::Float8, &BodyValue::Float8(1.0)),
        bytes(
            BodyKind::Float8,
            &BodyValue::Float8(1.000_000_000_000_000_2)
        ),
        "positive control: adjacent doubles must differ"
    );
}

#[test]
fn canonicality_double_cannot_carry_a_nan_payload() {
    // IEEE-754 has ~2^53 distinct NaN bit patterns (quiet/signalling x payload).
    // They all compare unequal to themselves, so "one value, one byte string"
    // is not even well-defined for them. Schema v1 therefore makes the type
    // unable to carry them: `lix_schema`'s row validation requires
    // `f64::is_finite`, and the encoder rejects non-finite values outright.
    let quiet = f64::from_bits(0x7ff8_0000_0000_0001);
    let signalling = f64::from_bits(0x7ff0_0000_0000_0001);
    let negative = f64::from_bits(0xfff8_0000_0000_0000);
    assert!(quiet.is_nan() && signalling.is_nan() && negative.is_nan());
    assert_ne!(
        quiet.to_bits(),
        signalling.to_bits(),
        "premise: distinct NaN payloads exist"
    );

    for value in [
        quiet,
        signalling,
        negative,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ] {
        assert!(
            canonical_float8_bits(value).is_err(),
            "{value:?} must be rejected, not encoded"
        );
        assert!(
            encode_one(BodyKind::Float8, &BodyValue::Float8(value)).is_err(),
            "{value:?} must not reach the body"
        );
    }

    // And the decoder refuses non-canonical fixed slots, so a hand-built or
    // corrupted body cannot smuggle one back in.
    let plan = [BodyColumn {
        kind: BodyKind::Float8,
        nullable: false,
    }];
    let mut body = encode_one_not_null(BodyKind::Float8, &BodyValue::Float8(1.0));
    let len = body.len();
    body[len - 8..].copy_from_slice(&quiet.to_be_bytes());
    assert!(decode_body(&plan, &body).is_err(), "NaN must not decode");
    body[len - 8..].copy_from_slice(&(-0.0_f64).to_be_bytes());
    assert!(
        decode_body(&plan, &body).is_err(),
        "-0.0 must not decode: it is not canonical"
    );

    // POSITIVE CONTROL: the decoder is not simply rejecting everything.
    body[len - 8..].copy_from_slice(&0.0_f64.to_be_bytes());
    assert!(
        decode_body(&plan, &body).is_ok(),
        "positive control: +0.0 must decode"
    );
}

// ---------------------------------------------------------------------------
// uuid
// ---------------------------------------------------------------------------

#[test]
fn canonicality_uuid_is_sixteen_bytes_in_one_order() {
    // Five textual spellings of one UUID, all accepted by `Uuid::parse_str`.
    let spellings = [
        "0191b7e4-1f2c-7c3a-9d4e-5f6a7b8c9d0e",
        "0191B7E4-1F2C-7C3A-9D4E-5F6A7B8C9D0E",
        "0191b7e41f2c7c3a9d4e5f6a7b8c9d0e",
        "{0191b7e4-1f2c-7c3a-9d4e-5f6a7b8c9d0e}",
        "urn:uuid:0191b7e4-1f2c-7c3a-9d4e-5f6a7b8c9d0e",
    ];
    let images = spellings
        .iter()
        .map(|text| {
            bytes(
                BodyKind::Uuid,
                &BodyValue::Uuid(uuid::Uuid::parse_str(text).expect("must parse")),
            )
        })
        .collect::<Vec<_>>();
    for (spelling, image) in spellings.iter().zip(&images) {
        assert_eq!(
            image, &images[0],
            "spelling {spelling} must produce the same 16 bytes"
        );
    }

    // Exactly 16 bytes, in RFC 4122 field order (most significant byte first).
    let body = encode_one_not_null(
        BodyKind::Uuid,
        &BodyValue::Uuid(uuid::Uuid::parse_str(spellings[0]).unwrap()),
    );
    assert_eq!(
        body.len(),
        1 + 16,
        "header byte plus exactly 16 payload bytes"
    );
    assert_eq!(body[1], 0x01, "first payload byte is the leading hex octet");
    assert_eq!(
        body[16], 0x0e,
        "last payload byte is the trailing hex octet"
    );

    // POSITIVE CONTROL
    assert_ne!(
        images[0],
        bytes(BodyKind::Uuid, &BodyValue::Uuid(uuid::Uuid::nil())),
        "positive control: distinct UUIDs must encode differently"
    );
}

// ---------------------------------------------------------------------------
// text
// ---------------------------------------------------------------------------

#[test]
fn canonicality_text_has_one_image_per_string() {
    // Path A: literal. Path B: built character by character. Path C: JSON
    // parse of an escaped spelling of the same string.
    let literal = BodyValue::Text("caf\u{e9} \u{1f600}".to_owned());
    let built = {
        let mut text = String::new();
        for ch in "caf\u{e9} \u{1f600}".chars() {
            text.push(ch);
        }
        BodyValue::Text(text)
    };
    let parsed = BodyValue::Text(
        serde_json::from_str::<serde_json::Value>(r#""café 😀""#)
            .unwrap()
            .as_str()
            .unwrap()
            .to_owned(),
    );
    assert_eq!(
        bytes(BodyKind::Text, &literal),
        bytes(BodyKind::Text, &built)
    );
    assert_eq!(
        bytes(BodyKind::Text, &literal),
        bytes(BodyKind::Text, &parsed),
        "escaped and literal JSON spellings of one string must agree"
    );

    // POSITIVE CONTROL
    assert_ne!(
        bytes(BodyKind::Text, &BodyValue::Text("a".to_owned())),
        bytes(BodyKind::Text, &BodyValue::Text("b".to_owned())),
        "positive control: distinct strings must encode differently"
    );
}

#[test]
fn canonicality_text_is_not_unicode_normalised() {
    // This documents a DECISION, not a gap. NFC "é" (U+00E9) and NFD "é"
    // (U+0065 U+0301) render identically but are distinct PostgreSQL `text`
    // values. Normalising would make the encoding non-injective with respect to
    // what the user stored, so the encoding preserves the bytes as given and
    // the two forms get two content addresses.
    let nfc = BodyValue::Text("\u{e9}".to_owned());
    let nfd = BodyValue::Text("e\u{301}".to_owned());
    assert_ne!(
        bytes(BodyKind::Text, &nfc),
        bytes(BodyKind::Text, &nfd),
        "NFC and NFD are different text values and must stay distinguishable"
    );
    // If a later change introduces normalisation upstream, this assertion is
    // where it will surface, rather than silently in dedup behaviour.
}

#[test]
fn canonicality_text_rejects_interior_nul() {
    assert!(encode_one(BodyKind::Text, &BodyValue::Text("a\0b".to_owned())).is_err());
    // POSITIVE CONTROL
    assert!(encode_one(BodyKind::Text, &BodyValue::Text("ab".to_owned())).is_ok());
}

// ---------------------------------------------------------------------------
// jsonb  --  the type where JSON survives
// ---------------------------------------------------------------------------

#[test]
fn canonicality_jsonb_has_one_image_per_semantic_value() {
    // Six construction paths for one semantic JSON value, differing in key
    // order, whitespace, number spelling, and escape spelling.
    let texts = [
        r#"{"a":2,"b":[1,{"z":true,"y":null}],"c":"x"}"#,
        r#"{"c":"x","b":[1,{"y":null,"z":true}],"a":2}"#,
        "{ \"a\" : 2 , \"c\" : \"x\" , \"b\" : [ 1 , { \"z\" : true , \"y\" : null } ] }",
        r#"{"a":2.0,"b":[1.0,{"z":true,"y":null}],"c":"x"}"#,
        r#"{"a":2e0,"b":[1E0,{"z":true,"y":null}],"c":"x"}"#,
        r#"{"b":[1,{"y":null,"z":true}],"a":2,"c":"x","a":2}"#,
    ];
    let images = texts
        .iter()
        .map(|text| {
            let value: serde_json::Value = serde_json::from_str(text).expect("must parse");
            bytes(BodyKind::Jsonb, &BodyValue::Jsonb(value))
        })
        .collect::<Vec<_>>();
    for (text, image) in texts.iter().zip(&images) {
        assert_eq!(
            image, &images[0],
            "spelling {text} must canonicalise to the same bytes"
        );
    }

    // Path via the typed builder rather than the parser.
    let built = json!({"c": "x", "b": [1, {"z": true, "y": serde_json::Value::Null}], "a": 2});
    assert_eq!(
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(built)),
        images[0],
        "programmatic construction must agree with parsed construction"
    );

    // POSITIVE CONTROLS: the canonicaliser is not collapsing everything.
    assert_ne!(
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a": 1}))),
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a": "1"}))),
        "positive control: 1 and \"1\" are different JSON values"
    );
    assert_ne!(
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(json!([1, 2]))),
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(json!([2, 1]))),
        "positive control: array order is significant"
    );
    assert_ne!(
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a": 1, "b": 2}))),
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a": 1}))),
        "positive control: an extra key must change the bytes"
    );
}

#[test]
fn canonicality_jsonb_null_is_not_sql_null() {
    // JSONB `null` is a JSONB value; SQL NULL is the absence of one. They must
    // not share a byte image, or the null bitmap and the payload would disagree.
    assert_ne!(
        bytes(BodyKind::Jsonb, &BodyValue::Jsonb(serde_json::Value::Null)),
        bytes(BodyKind::Jsonb, &BodyValue::Null),
        "JSONB null and SQL NULL must be distinguishable"
    );
}

#[test]
fn canonicality_jsonb_rejects_nul() {
    assert!(
        encode_one(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a": "x\u{0}y"}))).is_err(),
        "NUL in a JSONB string must be rejected: PostgreSQL jsonb cannot hold it"
    );
    assert!(
        encode_one(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a\u{0}": 1}))).is_err(),
        "NUL in a JSONB key must be rejected"
    );
    // POSITIVE CONTROL
    assert!(encode_one(BodyKind::Jsonb, &BodyValue::Jsonb(json!({"a": "xy"}))).is_ok());
}

// ---------------------------------------------------------------------------
// NULL slots: zero-filled, with no path to stale bytes
// ---------------------------------------------------------------------------

#[test]
fn canonicality_null_fixed_slots_are_zero_filled_even_on_a_reused_buffer() {
    let plan = [
        BodyColumn {
            kind: BodyKind::Int8,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Float8,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Uuid,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Boolean,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Text,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Jsonb,
            nullable: true,
        },
    ];
    let all_null = vec![
        BodyValue::Null,
        BodyValue::Null,
        BodyValue::Null,
        BodyValue::Null,
        BodyValue::Null,
        BodyValue::Null,
    ];
    let populated = vec![
        BodyValue::Int8(-1),
        BodyValue::Float8(1.5),
        BodyValue::Uuid(uuid::Uuid::from_bytes([0xff; 16])),
        BodyValue::Boolean(true),
        BodyValue::Text("filler".to_owned()),
        BodyValue::Jsonb(json!({"k": "vvvvvvvvvvvvvvvv"})),
    ];

    // Fresh buffer.
    let mut fresh = Vec::new();
    encode_body(&plan, &all_null, &mut fresh).unwrap();

    // Buffer that previously held an all-0xff row. If any NULL slot were left
    // uninitialised, these bytes would survive and the images would differ.
    let mut reused = Vec::new();
    encode_body(&plan, &populated, &mut reused).unwrap();
    assert!(reused.contains(&0xff), "the poison actually got written");
    encode_body(&plan, &all_null, &mut reused).unwrap();

    assert_eq!(
        fresh, reused,
        "a reused buffer must not leak stale bytes into NULL slots"
    );
    // Every fixed byte after the header and bitmap must be zero.
    let bitmap_bytes = 6usize.div_ceil(8);
    let fixed = &fresh[1 + bitmap_bytes..1 + bitmap_bytes + 8 + 8 + 16 + 1];
    assert!(
        fixed.iter().all(|&byte| byte == 0),
        "NULL fixed slots must be zero-filled"
    );

    // POSITIVE CONTROL: an all-NULL row and a populated row must differ.
    let mut other = Vec::new();
    encode_body(&plan, &populated, &mut other).unwrap();
    assert_ne!(fresh, other, "positive control");

    // And NULL text is not the empty string.
    let mut null_text = Vec::new();
    let mut empty_text = Vec::new();
    encode_body(
        &[column(BodyKind::Text)],
        &[BodyValue::Null],
        &mut null_text,
    )
    .unwrap();
    encode_body(
        &[column(BodyKind::Text)],
        &[BodyValue::Text(String::new())],
        &mut empty_text,
    )
    .unwrap();
    assert_ne!(
        null_text, empty_text,
        "NULL text and '' must be distinguishable"
    );
}

// ---------------------------------------------------------------------------
// Totality: every admissible value encodes, and decoding inverts encoding
// ---------------------------------------------------------------------------

#[test]
fn encoding_is_total_and_injective_over_a_corpus() {
    let plan = [
        BodyColumn {
            kind: BodyKind::Boolean,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Int8,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Float8,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Uuid,
            nullable: false,
        },
        BodyColumn {
            kind: BodyKind::Text,
            nullable: true,
        },
        BodyColumn {
            kind: BodyKind::Jsonb,
            nullable: true,
        },
    ];
    let booleans = [
        BodyValue::Null,
        BodyValue::Boolean(false),
        BodyValue::Boolean(true),
    ];
    let int8s = [
        BodyValue::Null,
        BodyValue::Int8(i64::MIN),
        BodyValue::Int8(0),
        BodyValue::Int8(i64::MAX),
    ];
    let doubles = [
        BodyValue::Null,
        BodyValue::Float8(0.0),
        BodyValue::Float8(-0.0),
        BodyValue::Float8(f64::MIN),
        BodyValue::Float8(f64::MAX),
        BodyValue::Float8(f64::MIN_POSITIVE),
    ];
    let texts = [
        BodyValue::Null,
        BodyValue::Text(String::new()),
        BodyValue::Text("\u{1f600}".to_owned()),
        BodyValue::Text("x".repeat(300)),
    ];
    let jsons = [
        BodyValue::Null,
        BodyValue::Jsonb(serde_json::Value::Null),
        BodyValue::Jsonb(json!({})),
        BodyValue::Jsonb(json!([1, "a", {"b": [true, false]}])),
    ];
    let uuid = BodyValue::Uuid(uuid::Uuid::from_bytes([7; 16]));

    let mut seen = std::collections::HashMap::<Vec<u8>, Vec<BodyValue>>::new();
    let mut buffer = Vec::new();
    let mut rows = 0usize;
    for boolean in &booleans {
        for int8 in &int8s {
            for double in &doubles {
                for text in &texts {
                    for json in &jsons {
                        let row = vec![
                            boolean.clone(),
                            int8.clone(),
                            double.clone(),
                            uuid.clone(),
                            text.clone(),
                            json.clone(),
                        ];
                        // TOTAL: every admissible row encodes.
                        encode_body(&plan, &row, &mut buffer).expect("row must encode");
                        rows += 1;

                        // Round trip: decoding inverts encoding.
                        let decoded = decode_body(&plan, &buffer).expect("row must decode");
                        let mut reencoded = Vec::new();
                        encode_body(&plan, &decoded, &mut reencoded).expect("must re-encode");
                        assert_eq!(buffer, reencoded, "encode . decode . encode == encode");

                        // INJECTIVE: no two distinct canonical rows share bytes.
                        if let Some(previous) = seen.insert(buffer.clone(), decoded.clone()) {
                            assert_eq!(
                                previous, decoded,
                                "two distinct values collided on one byte string"
                            );
                        }
                    }
                }
            }
        }
    }
    assert_eq!(
        rows,
        3 * 4 * 6 * 4 * 4,
        "the corpus must actually have been walked"
    );
    // -0.0 and +0.0 are ONE value, so the distinct-image count is one short of
    // the row count per remaining axis. Assert the collapse happened exactly once.
    assert_eq!(
        seen.len(),
        rows - (3 * 4 * 1 * 4 * 4),
        "exactly the -0.0/+0.0 pairs may share an image"
    );
}

#[test]
fn wide_offsets_are_engaged_only_past_the_u16_boundary() {
    let plan = [
        BodyColumn {
            kind: BodyKind::Text,
            nullable: false,
        },
        BodyColumn {
            kind: BodyKind::Text,
            nullable: false,
        },
    ];
    let mut narrow = Vec::new();
    encode_body(
        &plan,
        &[
            BodyValue::Text("a".repeat(1000)),
            BodyValue::Text("b".repeat(1000)),
        ],
        &mut narrow,
    )
    .unwrap();
    assert_eq!(narrow[0] & 0b0000_1000, 0, "u16 offsets below the boundary");
    assert_eq!(decode_body(&plan, &narrow).unwrap().len(), 2);

    let mut wide = Vec::new();
    encode_body(
        &plan,
        &[
            BodyValue::Text("a".repeat(70_000)),
            BodyValue::Text("b".repeat(10)),
        ],
        &mut wide,
    )
    .unwrap();
    assert_ne!(wide[0] & 0b0000_1000, 0, "u32 offsets past the boundary");
    let decoded = decode_body(&plan, &wide).unwrap();
    assert_eq!(decoded[0], BodyValue::Text("a".repeat(70_000)));
    assert_eq!(decoded[1], BodyValue::Text("b".repeat(10)));
}

// ---------------------------------------------------------------------------
// The mechanism behind JSONB key-order canonicality
// ---------------------------------------------------------------------------

#[test]
fn canonicality_jsonb_key_order_mechanism_is_pinned() {
    // MUTATION FINDING (run nonce vlayout-8813): deleting the explicit
    // `entries.sort_by(...)` from the JSONB normaliser left the whole
    // canonicality suite GREEN. The sort is currently redundant, because
    // `serde_json::Map` without the `preserve_order` feature IS a
    // `BTreeMap<String, Value>`, which serialises in sorted key order already.
    //
    // So key-order canonicality rests on TWO independent mechanisms, and a test
    // that only exercises the encoder cannot tell which one is carrying it.
    // This test pins the one that lives outside our code: if any crate in the
    // workspace ever turns on `serde_json/preserve_order` (cargo unifies
    // features across the whole graph), raw serde_json stops sorting and this
    // assertion fails loudly — pointing at the fact that the normaliser's sort
    // has become load-bearing, rather than letting it change silently.
    let raw = serde_json::from_str::<serde_json::Value>(r#"{"b":1,"a":2,"C":3}"#)
        .unwrap()
        .to_string();
    assert_eq!(
        raw, r#"{"C":3,"a":2,"b":1}"#,
        "serde_json is expected to be built WITHOUT `preserve_order`, so its Map \
         sorts keys by byte order. If this fails, `preserve_order` was enabled \
         somewhere in the workspace and JSONB key-order canonicality now depends \
         solely on normalise_jsonb's sort_by."
    );

    // Independently of which mechanism carries it, the encoder must be
    // order-invariant. This is the property that actually matters.
    let a = encode_one(
        BodyKind::Jsonb,
        &BodyValue::Jsonb(serde_json::from_str(r#"{"b":1,"a":2,"C":3}"#).unwrap()),
    )
    .unwrap();
    let b = encode_one(
        BodyKind::Jsonb,
        &BodyValue::Jsonb(serde_json::from_str(r#"{"C":3,"b":1,"a":2}"#).unwrap()),
    )
    .unwrap();
    assert_eq!(a, b, "JSONB encoding must not depend on source key order");
}
