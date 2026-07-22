wit_bindgen::generate!({
    path: "../wit",
    world: "guest",
});

use exports::lix::p3_stream_probe::transfer::Guest;
use wit_bindgen::rt::async_support::{StreamReader, StreamResult};

struct Probe;

impl Guest for Probe {
    fn consume_list(data: Vec<u8>, checksum: bool) -> (u64, u64) {
        let sum = if checksum {
            data.iter()
                .fold(0_u64, |sum, byte| sum.wrapping_add(u64::from(*byte)))
        } else {
            0
        };
        (data.len() as u64, sum)
    }

    async fn consume_stream(
        mut data: StreamReader<u8>,
        checksum: bool,
        chunk_bytes: u32,
    ) -> (u64, u64) {
        let mut count = 0_u64;
        let mut sum = 0_u64;
        let mut buffer = Vec::with_capacity(chunk_bytes.max(1) as usize);

        loop {
            let (status, mut filled) = data.read(buffer).await;
            count += filled.len() as u64;
            if checksum {
                sum = filled
                    .iter()
                    .fold(sum, |sum, byte| sum.wrapping_add(u64::from(*byte)));
            }
            filled.clear();
            buffer = filled;

            match status {
                StreamResult::Complete(_) => {}
                StreamResult::Dropped => break,
                StreamResult::Cancelled => unreachable!("probe never cancels a read"),
            }
        }

        (count, sum)
    }
}

export!(Probe);
