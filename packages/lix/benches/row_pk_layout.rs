use std::hint::black_box;
use std::mem::size_of;
use std::time::Instant;

use smallvec::{SmallVec, smallvec};

const ROWS: usize = 220_000;
const CYCLES: usize = 7;

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
enum Component {
    Uuid([u8; 16]),
    Integer(i64),
    String(Box<str>),
    Bytes(Box<[u8]>),
}

trait Tuple: Clone + Ord {
    fn uuid_single(index: usize) -> Self;
    fn builtin_mix(index: usize) -> Self;
    fn components(&self) -> &[Component];
    fn spilled(&self) -> bool;
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct VecTuple(Vec<Component>);

impl Tuple for VecTuple {
    fn uuid_single(index: usize) -> Self {
        Self(vec![Component::Uuid(uuid_bytes(index))])
    }

    fn builtin_mix(index: usize) -> Self {
        if index.is_multiple_of(9) {
            Self(vec![
                Component::Uuid(uuid_bytes(index)),
                Component::Integer(i64::try_from(index % 2).expect("fixture integer fits i64")),
            ])
        } else {
            Self(vec![Component::String(
                format!("key-{index:012x}").into_boxed_str(),
            )])
        }
    }

    fn components(&self) -> &[Component] {
        &self.0
    }

    fn spilled(&self) -> bool {
        true
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct InlineOne(SmallVec<[Component; 1]>);

impl Tuple for InlineOne {
    fn uuid_single(index: usize) -> Self {
        Self(smallvec![Component::Uuid(uuid_bytes(index))])
    }

    fn builtin_mix(index: usize) -> Self {
        if index.is_multiple_of(9) {
            Self(smallvec![
                Component::Uuid(uuid_bytes(index)),
                Component::Integer(i64::try_from(index % 2).expect("fixture integer fits i64")),
            ])
        } else {
            Self(smallvec![Component::String(
                format!("key-{index:012x}").into_boxed_str(),
            )])
        }
    }

    fn components(&self) -> &[Component] {
        &self.0
    }

    fn spilled(&self) -> bool {
        self.0.spilled()
    }
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct InlineTwo(SmallVec<[Component; 2]>);

impl Tuple for InlineTwo {
    fn uuid_single(index: usize) -> Self {
        Self(smallvec![Component::Uuid(uuid_bytes(index))])
    }

    fn builtin_mix(index: usize) -> Self {
        if index.is_multiple_of(9) {
            Self(smallvec![
                Component::Uuid(uuid_bytes(index)),
                Component::Integer(i64::try_from(index % 2).expect("fixture integer fits i64")),
            ])
        } else {
            Self(smallvec![Component::String(
                format!("key-{index:012x}").into_boxed_str(),
            )])
        }
    }

    fn components(&self) -> &[Component] {
        &self.0
    }

    fn spilled(&self) -> bool {
        self.0.spilled()
    }
}

fn uuid_bytes(index: usize) -> [u8; 16] {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&0x0192_0000_0000_7000_u64.to_be_bytes());
    bytes[8..].copy_from_slice(&(0x8000_0000_0000_0000_u64 | index as u64).to_be_bytes());
    bytes
}

fn encode(tuple: &impl Tuple, out: &mut Vec<u8>) {
    for component in tuple.components() {
        match component {
            Component::Uuid(bytes) => {
                out.push(0);
                out.extend_from_slice(bytes);
            }
            Component::Integer(value) => {
                out.push(1);
                out.extend_from_slice(
                    &(u64::from_be_bytes(value.to_be_bytes()) ^ (1 << 63)).to_be_bytes(),
                );
            }
            Component::String(value) => {
                out.push(2);
                out.extend_from_slice(value.as_bytes());
                out.push(0);
            }
            Component::Bytes(value) => {
                out.push(3);
                out.extend_from_slice(value);
                out.push(0);
            }
        }
    }
}

fn measure<T: Tuple>(name: &str, workload: &str, make: impl Fn(usize) -> T) {
    let mut samples = Vec::with_capacity(CYCLES);
    let mut spills = 0;
    let mut encoded_bytes = 0;
    for _ in 0..CYCLES {
        let started = Instant::now();
        let mut tuples = (0..ROWS).rev().map(&make).collect::<Vec<_>>();
        spills = tuples.iter().filter(|tuple| tuple.spilled()).count();
        tuples.sort_unstable();
        let mut encoded = Vec::with_capacity(ROWS * 20);
        for tuple in &tuples {
            encode(tuple, &mut encoded);
        }
        encoded_bytes = encoded.len();
        black_box(encoded);
        black_box(tuples);
        samples.push(
            u64::try_from(started.elapsed().as_nanos()).expect("benchmark duration fits u64"),
        );
    }
    samples.sort_unstable();
    println!(
        "row_pk_layout name={name} workload={workload} rows={ROWS} median_ns={} spills={spills} encoded_bytes={encoded_bytes}",
        samples[CYCLES / 2]
    );
}

fn main() {
    // Keep every production variant represented in the layout measurement.
    black_box(Component::Bytes(Box::from([0_u8; 4])));
    println!(
        "row_pk_layout_size component={} vec={} inline1={} inline2={}",
        size_of::<Component>(),
        size_of::<VecTuple>(),
        size_of::<InlineOne>(),
        size_of::<InlineTwo>(),
    );
    measure::<VecTuple>("vec", "uuid_single", VecTuple::uuid_single);
    measure::<InlineOne>("inline1", "uuid_single", InlineOne::uuid_single);
    measure::<InlineTwo>("inline2", "uuid_single", InlineTwo::uuid_single);
    measure::<VecTuple>("vec", "builtin_mix", VecTuple::builtin_mix);
    measure::<InlineOne>("inline1", "builtin_mix", InlineOne::builtin_mix);
    measure::<InlineTwo>("inline2", "builtin_mix", InlineTwo::builtin_mix);
}
