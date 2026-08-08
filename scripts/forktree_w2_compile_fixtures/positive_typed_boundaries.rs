#[derive(Clone, Copy)]
struct ObjectId(u64);
#[derive(Clone, Copy)]
struct BlobId(u128);

fn read_blob(blob: BlobId, object: ObjectId) {
    let _ = (blob.0, object.0);
}

fn main() {
    read_blob(BlobId(7), ObjectId(41));
}
