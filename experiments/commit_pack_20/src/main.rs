use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::Instant;

const OBJECT_MAGIC: &[u8; 8] = b"LIXFTO\0\x01";
const PACK_MAGIC: &[u8; 8] = b"LIXCPK\0\x01";
const PAGE_TARGET: usize = 64 * 1024;
const PAGE_MAX: usize = 4 * 1024 * 1024;
const EDGE_LIMIT: usize = 256;
const MAX_PAGES: usize = EDGE_LIMIT - 2;

#[derive(Clone, Copy)]
enum Kind {
    Introduced,
    Selected,
}

#[derive(Clone)]
struct Member {
    change_id: [u8; 16],
    payload: Vec<u8>,
    source_commit: Option<[u8; 32]>,
    source_ordinal: u32,
}

#[derive(Clone)]
struct Page {
    start: u32,
    members: Vec<Member>,
    bytes: Vec<u8>,
    id: [u8; 32],
}

struct Geometry {
    envelope: Vec<u8>,
    pages: Vec<Page>,
    pack: Vec<u8>,
    inline: bool,
}

fn put_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&u32::try_from(value).expect("model bound").to_be_bytes());
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) {
    put_u32(out, value.len());
    out.extend_from_slice(value);
}

fn id<const N: usize>(ordinal: usize, salt: u8) -> [u8; N] {
    let mut out = [0_u8; N];
    let ordinal = (ordinal as u64).to_be_bytes();
    out[..8.min(N)].copy_from_slice(&ordinal[..8.min(N)]);
    for (index, byte) in out.iter_mut().enumerate().skip(8) {
        *byte = salt.wrapping_add((index as u8).wrapping_mul(17));
    }
    out
}

fn payload(ordinal: usize, length: usize) -> Vec<u8> {
    let mut state = (ordinal as u64).wrapping_add(0x9e37_79b9_7f4a_7c15);
    (0..length)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect()
}

fn members(kind: Kind, count: usize, payload_bytes: usize) -> Vec<Member> {
    (0..count)
        .map(|ordinal| Member {
            change_id: id(ordinal, 0x31),
            payload: match kind {
                Kind::Introduced => payload(ordinal, payload_bytes),
                Kind::Selected => Vec::new(),
            },
            source_commit: match kind {
                Kind::Introduced => None,
                Kind::Selected => Some(id(ordinal / 32, 0x71)),
            },
            source_ordinal: ordinal as u32,
        })
        .collect()
}

fn encode_member(member: &Member, out: &mut Vec<u8>) {
    match member.source_commit {
        None => {
            out.push(0);
            out.extend_from_slice(&member.change_id);
            put_bytes(out, &member.payload);
            out.push(0);
            put_u64(out, 1);
            put_u32(out, 0);
        }
        Some(source) => {
            out.push(1);
            out.extend_from_slice(&member.change_id);
            out.extend_from_slice(&source);
            out.extend_from_slice(&member.source_ordinal.to_be_bytes());
            put_u64(out, 1);
        }
    }
}

fn encode_page(start: usize, members: Vec<Member>) -> Page {
    let mut body = Vec::new();
    body.extend_from_slice(&id::<16>(0, 0x51));
    put_u32(&mut body, start);
    put_u32(&mut body, members.len());
    for member in &members {
        encode_member(member, &mut body);
    }
    let compressed = zstd::bulk::compress(&body, 1).expect("compress model page");
    let mut bytes = Vec::new();
    bytes.extend_from_slice(OBJECT_MAGIC);
    put_u32(&mut bytes, 20);
    put_u32(&mut bytes, body.len());
    put_bytes(&mut bytes, &compressed);
    let id = *blake3::hash(&bytes).as_bytes();
    Page {
        start: start as u32,
        members,
        bytes,
        id,
    }
}

fn pages(input: &[Member]) -> Vec<Page> {
    if input.is_empty() {
        return Vec::new();
    }
    let member_bytes = input
        .iter()
        .map(|member| {
            let mut bytes = Vec::new();
            encode_member(member, &mut bytes);
            bytes.len()
        })
        .collect::<Vec<_>>();
    let total = member_bytes.iter().sum::<usize>();
    let max = member_bytes.iter().copied().max().unwrap_or(0);
    let target = PAGE_TARGET
        .max(
            total
                .div_ceil(MAX_PAGES)
                .saturating_add(max)
                .saturating_add(128),
        )
        .min(PAGE_MAX);
    let mut output = Vec::new();
    let mut start = 0;
    let mut current = Vec::new();
    let mut current_bytes = 0;
    let mut current_edges = 0;
    for (member, bytes) in input.iter().cloned().zip(member_bytes) {
        let edges = usize::from(member.source_commit.is_some());
        if !current.is_empty()
            && (current_bytes + bytes > target.saturating_sub(128)
                || current_edges + edges > EDGE_LIMIT)
        {
            let count = current.len();
            output.push(encode_page(start, std::mem::take(&mut current)));
            start += count;
            current_bytes = 0;
            current_edges = 0;
        }
        current.push(member);
        current_bytes += bytes;
        current_edges += edges;
    }
    if !current.is_empty() {
        output.push(encode_page(start, current));
    }
    assert!(output.len() <= MAX_PAGES);
    output
}

fn encode_envelope(pages: &[Page], parent_count: usize, member_count: usize) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(OBJECT_MAGIC);
    put_u32(&mut out, 21);
    out.extend_from_slice(&id::<16>(0, 0x51));
    put_u64(&mut out, 2);
    put_u32(&mut out, parent_count);
    for ordinal in 0..parent_count {
        out.extend_from_slice(&id::<32>(ordinal, 0x81));
    }
    put_u32(&mut out, pages.len());
    for page in pages {
        out.extend_from_slice(&page.id);
    }
    out.extend_from_slice(&id::<32>(0, 0x91));
    out.extend_from_slice(&id::<32>(0, 0xa1));
    out.push(1);
    out.extend_from_slice(&id::<16>(0, 0xb1));
    out.extend_from_slice(&id::<32>(0, 0xc1));
    put_u32(&mut out, 1);
    out.extend_from_slice(&id::<32>(0, 0xd1));
    put_u32(&mut out, 1);
    let mut metadata = vec![0xe1; 92];
    metadata.extend_from_slice(&(member_count as u32).to_be_bytes());
    put_bytes(&mut out, &metadata);
    out
}

fn geometry(kind: Kind, count: usize, payload_bytes: usize, parents: usize) -> Geometry {
    let members = members(kind, count, payload_bytes);
    let pages = pages(&members);
    let envelope = encode_envelope(&pages, parents, count);
    let mut pack = Vec::new();
    pack.extend_from_slice(PACK_MAGIC);
    put_u32(&mut pack, 1);
    put_bytes(&mut pack, &envelope);
    put_u32(&mut pack, pages.len());
    for page in &pages {
        put_bytes(&mut pack, &page.bytes);
    }
    let inline = pages.len() <= 1 && pack.len() <= PAGE_TARGET;
    Geometry {
        envelope,
        pages,
        pack,
        inline,
    }
}

fn hash_ns(bytes: &[u8], iterations: usize) -> u128 {
    let start = Instant::now();
    for _ in 0..iterations {
        black_box(blake3::hash(black_box(bytes)));
    }
    start.elapsed().as_nanos() / iterations as u128
}

fn validate(geometry: &Geometry, expected_members: usize) -> Result<(), &'static str> {
    if !geometry.inline {
        return Ok(());
    }
    if geometry.pack.len() < 20 || &geometry.pack[..8] != PACK_MAGIC {
        return Err("truncated pack");
    }
    let mut seen = BTreeSet::new();
    let mut expected_start = 0_u32;
    let mut count = 0_usize;
    for page in &geometry.pages {
        if page.start != expected_start {
            return Err("member order gap");
        }
        for member in &page.members {
            if !seen.insert(member.change_id) {
                return Err("duplicate member");
            }
            count += 1;
        }
        expected_start += page.members.len() as u32;
    }
    if count != expected_members {
        return Err("member count mismatch");
    }
    Ok(())
}

fn corruption_controls() {
    let good = geometry(Kind::Introduced, 10, 128, 2);
    validate(&good, 10).expect("canonical pack");

    let mut truncated = geometry(Kind::Introduced, 10, 128, 2);
    truncated.pack.truncate(7);
    assert_eq!(validate(&truncated, 10), Err("truncated pack"));

    let mut duplicate = geometry(Kind::Introduced, 10, 128, 2);
    duplicate.pages[0].members[1].change_id = duplicate.pages[0].members[0].change_id;
    assert_eq!(validate(&duplicate, 10), Err("duplicate member"));

    let mut gap = geometry(Kind::Introduced, 10, 128, 2);
    gap.pages[0].start = 1;
    assert_eq!(validate(&gap, 10), Err("member order gap"));

    assert_eq!(validate(&good, 11), Err("member count mismatch"));

    let original = blake3::hash(&good.pack);
    for offset in [0, 8, good.pack.len() / 2, good.pack.len() - 1] {
        let mut substituted = good.pack.clone();
        substituted[offset] ^= 0x80;
        assert_ne!(blake3::hash(&substituted), original);
    }
}

fn main() {
    corruption_controls();
    println!(
        "kind,members,payload_bytes,parents,pages,inline,current_objects,pack_objects,current_bytes,pack_bytes,topology_current_bytes,topology_pack_bytes,full_read_current_calls,full_read_pack_calls,hash_current_ns,hash_pack_ns"
    );
    for kind in [Kind::Introduced, Kind::Selected] {
        let payloads: &[usize] = match kind {
            Kind::Introduced => &[32, 128, 512, 2048],
            Kind::Selected => &[0],
        };
        for &payload_bytes in payloads {
            for members in [1, 10, 100, 500, 1_000, 5_000, 25_000] {
                for parents in [1, 2, 8] {
                    let value = geometry(kind, members, payload_bytes, parents);
                    let current_bytes = value.envelope.len()
                        + value
                            .pages
                            .iter()
                            .map(|page| page.bytes.len())
                            .sum::<usize>();
                    let current_objects = 1 + value.pages.len();
                    let (pack_objects, pack_bytes, topology_pack, pack_hash) = if value.inline {
                        (1, value.pack.len(), value.pack.len(), value.pack.as_slice())
                    } else {
                        (
                            current_objects,
                            current_bytes,
                            value.envelope.len(),
                            value.envelope.as_slice(),
                        )
                    };
                    let iterations = if current_bytes < 128 * 1024 { 200 } else { 20 };
                    println!(
                        "{},{members},{payload_bytes},{parents},{},{},{current_objects},{pack_objects},{current_bytes},{pack_bytes},{},{topology_pack},{current_objects},{pack_objects},{},{}",
                        match kind {
                            Kind::Introduced => "introduced",
                            Kind::Selected => "selected",
                        },
                        value.pages.len(),
                        value.inline,
                        value.envelope.len(),
                        hash_ns(&value.envelope, iterations),
                        hash_ns(pack_hash, iterations),
                    );
                }
            }
        }
    }
}
