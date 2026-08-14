#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 2 || $# -gt 4 ]]; then
  echo "usage: $0 BINARY OUTPUT_DIR [quick|full] [SAMPLES]" >&2
  exit 64
fi

binary=$(realpath "$1")
output_dir=$(mkdir -p "$2" && realpath "$2")
mode=${3:-quick}
samples=${4:-5}

[[ -x "$binary" ]] || { echo "benchmark binary is not executable: $binary" >&2; exit 66; }
[[ "$mode" == quick || "$mode" == full ]] || { echo "mode must be quick or full" >&2; exit 64; }

pks=(integer uuid text composite)
shapes=(narrow wide)
jsons=(absent sparse dense)
counts=(1000 10000 50000 100000)
geometries=(c2_slotted pack2 pack4 pack8 pack16 pack_cdc)
if [[ "$mode" == quick ]]; then
  pks=(integer text)
  jsons=(absent dense)
  counts=(1000 10000)
fi

manifest="$output_dir/manifest.tsv"
printf 'sample\tpk\tshape\tjson\tn\tgeometry\ttarget\tstdout\ttime\n' > "$manifest"
for sample in $(seq 1 "$samples"); do
  for pk in "${pks[@]}"; do
    for shape in "${shapes[@]}"; do
      target=4096
      [[ "$shape" == wide ]] && target=16384
      for json in "${jsons[@]}"; do
        for n in "${counts[@]}"; do
          for geometry in "${geometries[@]}"; do
            stem=$(printf 's%02d-%s-%s-%s-n%s-%s' "$sample" "$pk" "$shape" "$json" "$n" "$geometry")
            stdout="$output_dir/$stem.csv"
            timing="$output_dir/$stem.time"
            timeout 1200 /usr/bin/time -v -o "$timing" \
              env LIX_PACK_N="$n" \
                  LIX_PACK_PK="$pk" \
                  LIX_PACK_SHAPE="$shape" \
                  LIX_PACK_JSON="$json" \
                  LIX_PACK_TARGET="$target" \
                  LIX_PACK_GEOMETRY="$geometry" \
                  "$binary" > "$stdout"
            printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
              "$sample" "$pk" "$shape" "$json" "$n" "$geometry" "$target" "$stdout" "$timing" >> "$manifest"
          done
        done
      done
    done
  done
done

sha256sum "$manifest" "$output_dir"/*.csv "$output_dir"/*.time > "$output_dir/SHA256SUMS"
