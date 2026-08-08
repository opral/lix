#[derive(Clone, Copy)]
struct ObjectId(u64);
#[derive(Clone, Copy)]
struct BlobId(u128);

fn read_blob(_: BlobId) {}

fn main() {
    read_blob(ObjectId(41));
}
