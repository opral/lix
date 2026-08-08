struct ManifestObject(u64);
struct ChunkObject(u64);

fn load_chunk(_: ChunkObject) {}

fn main() {
    load_chunk(ManifestObject(41));
}
