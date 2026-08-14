#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 4 || $# -gt 5 ]]; then
  echo "usage: $0 EXPECTED_HEAD LABEL OUTPUT_DIR BINARY [SAMPLES]" >&2
  exit 64
fi

expected_head=$1
label=$2
output_dir=$(mkdir -p "$3" && realpath "$3")
binary=$(realpath "$4")
samples=${5:-5}
repo=$(git rev-parse --show-toplevel)
head=$(git rev-parse HEAD)
tree=$(git rev-parse HEAD^{tree})
production_head=$(git rev-parse HEAD^)
production_tree=$(git rev-parse HEAD^^{tree})

[[ "$head" == "$expected_head" ]] || {
  echo "head mismatch: expected $expected_head, got $head" >&2
  exit 65
}
[[ -x "$binary" ]] || {
  echo "qualification binary is not executable: $binary" >&2
  exit 66
}
[[ "$samples" =~ ^[1-9][0-9]*$ ]] || {
  echo "samples must be a positive integer" >&2
  exit 64
}

mapfile -t changed_paths < <(git diff --name-only HEAD^ HEAD)
expected_paths=(
  packages/e2e/Cargo.toml
  packages/e2e/benches/FINAL_FILE_QUALIFICATION.md
  packages/e2e/benches/run_final_file_qualification.sh
  packages/e2e/benches/summarize_final_file_qualification.py
)
[[ "${changed_paths[*]}" == "${expected_paths[*]}" ]] || {
  printf 'unexpected harness overlay paths:\n%s\n' "${changed_paths[*]}" >&2
  exit 65
}

manifest="$output_dir/manifest.tsv"
printf 'sample\tlabel\tbackend\tsize\tstdout\tresource\n' > "$manifest"
{
  printf 'harness_head=%s\n' "$head"
  printf 'harness_tree=%s\n' "$tree"
  printf 'production_head=%s\n' "$production_head"
  printf 'production_tree=%s\n' "$production_tree"
  printf 'label=%s\n' "$label"
  printf 'binary=%s\n' "$binary"
  printf 'binary_sha256=%s\n' "$(sha256sum "$binary" | cut -d' ' -f1)"
  printf 'workload_sha256=%s\n' "$(sha256sum "$repo/packages/e2e/benches/large_binary_multimedia_qualification.rs" | cut -d' ' -f1)"
  printf 'runner_sha256=%s\n' "$(sha256sum "$repo/packages/e2e/benches/run_final_file_qualification.sh" | cut -d' ' -f1)"
  rustc -Vv | sed 's/^/rustc_/'
  cargo -V | sed 's/^/cargo_/'
} > "$output_dir/provenance.txt"

for sample in $(seq 1 "$samples"); do
  for backend in rocksdb slatedb; do
    for size in 64m 256m; do
      stem=$(printf 's%02d-%s-%s-%s' "$sample" "$label" "$backend" "$size")
      stdout="$output_dir/$stem.jsonl"
      resource="$output_dir/$stem.resource"
      database=$(mktemp -d "/tmp/lix-final-file-${backend}-${size}-XXXXXX")
      timeout 1200 /usr/bin/time -v -o "$resource" \
        "$binary" "$backend" "$database" "$size" > "$stdout"
      grep -q '"event":"result"' "$stdout" || {
        echo "cell produced no terminal result: $stem" >&2
        exit 1
      }
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$sample" "$label" "$backend" "$size" "$stdout" "$resource" >> "$manifest"
      case "$database" in
        /tmp/lix-final-file-*) rm -rf -- "$database" ;;
        *) echo "refusing to remove unexpected database path: $database" >&2; exit 70 ;;
      esac
    done
  done
done

python3 "$repo/packages/e2e/benches/summarize_final_file_qualification.py" \
  "$manifest" > "$output_dir/summary.tsv"
sha256sum "$output_dir"/*.jsonl "$output_dir"/*.resource \
  "$manifest" "$output_dir/provenance.txt" "$output_dir/summary.tsv" \
  > "$output_dir/SHA256SUMS"
