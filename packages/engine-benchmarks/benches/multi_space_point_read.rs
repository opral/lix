use std::hint::black_box;
use std::time::{Duration, Instant};

use bytes::Bytes;
use lix::storage::{
    GetManyRequest, GetOptions, Key, PutBatch, PutEntry, ReadOptions, SpaceId, Storage,
    StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const WARMUPS: usize = 10_000;
const SAMPLES: usize = 100_001;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("create benchmark runtime");
    runtime.block_on(async {
        let rocks_dir = tempfile::tempdir().expect("create RocksDB directory");
        run_backend(
            "rocksdb",
            RocksDB::open(rocks_dir.path()).expect("open RocksDB"),
        )
        .await;
        let slate_dir = tempfile::tempdir().expect("create SlateDB directory");
        run_backend(
            "slatedb",
            SlateDB::open(slate_dir.path()).expect("open SlateDB"),
        )
        .await;
    });
}

async fn run_backend<S>(backend: &str, storage: S)
where
    S: Storage,
{
    let request_keys = (0..3_u32)
        .map(|space| {
            if space == 1 {
                vec![key("a"), key("b")]
            } else {
                vec![key("a")]
            }
        })
        .collect::<Vec<_>>();
    let requests = request_keys
        .iter()
        .enumerate()
        .map(|(space, keys)| GetManyRequest {
            space: StorageSpace::mutable(
                SpaceId(u32::try_from(space).expect("space index fits u32") + 1),
                "bench.multi_space_point_read",
            ),
            keys,
            opts: GetOptions::default(),
        })
        .collect::<Vec<_>>();
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin seed write");
    for request in &requests {
        write
            .put_many(
                request.space,
                PutBatch {
                    entries: request
                        .keys
                        .iter()
                        .cloned()
                        .map(|key| PutEntry {
                            key,
                            value: StoredValue {
                                bytes: Bytes::from_static(b"value"),
                            },
                        })
                        .collect(),
                },
            )
            .await
            .expect("seed point values");
    }
    write.commit().await.expect("commit seed values");
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin benchmark read");

    for mode in ["sequential", "batch"] {
        for _ in 0..WARMUPS {
            execute(&read, &requests, mode).await;
        }
        let mut timings = Vec::with_capacity(SAMPLES);
        for _ in 0..SAMPLES {
            let started = Instant::now();
            execute(&read, &requests, mode).await;
            timings.push(started.elapsed());
        }
        timings.sort_unstable();
        let p50 = timings[SAMPLES / 2];
        let sample_count = u32::try_from(SAMPLES).expect("sample count fits in u32");
        let mean = timings.iter().sum::<Duration>() / sample_count;
        println!(
            "multi_space_point_read,backend={backend},mode={mode},requests=3,keys=4,p50_ns={},mean_ns={}",
            p50.as_nanos(),
            mean.as_nanos()
        );
    }
}

async fn execute<R>(read: &R, requests: &[GetManyRequest<'_>], mode: &str)
where
    R: StorageRead,
{
    if mode == "batch" {
        black_box(read.get_many(requests).await.expect("batch point read"));
    } else {
        for request in requests {
            black_box(
                read.get_many(std::slice::from_ref(request))
                    .await
                    .expect("sequential point read"),
            );
        }
    }
}

fn key(value: &'static str) -> Key {
    Key(Bytes::from_static(value.as_bytes()))
}
