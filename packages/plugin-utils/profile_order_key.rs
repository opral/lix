//! Run: rustc --edition=2024 -O packages/plugin-utils/profile_order_key.rs -o /tmp/profile-order-key && /tmp/profile-order-key
#[path = "order_key.rs"]
mod order_key;
use order_key::OrderKey;
use std::time::Instant;

fn main() {
    for append in [true, false] {
        for iteration in 0..5 {
            let start = Instant::now();
            let mut key = OrderKey::from_snapshot_string("80").unwrap();
            let mut characters = 0;
            let mut maximum = 0;
            for _ in 0..20_000 {
                key = if append {
                    OrderKey::evenly_between(Some(&key), None, 1)
                } else {
                    OrderKey::evenly_between(None, Some(&key), 1)
                }
                .unwrap()
                .remove(0);
                let length = key.to_snapshot_string().len();
                characters += length;
                maximum = maximum.max(length);
            }
            println!(
                "append={append} iteration={iteration} count=20000 elapsed_us={} total_characters={characters} max_characters={maximum}",
                start.elapsed().as_micros()
            );
        }
    }
}
