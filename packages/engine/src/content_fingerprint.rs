pub(crate) fn stable_content_fingerprint_hex(data: &[u8]) -> String {
    blake3::hash(data).to_hex().to_string()
}
