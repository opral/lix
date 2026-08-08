#!/bin/sh
set -eu

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
verifier="$script_dir/verify_w5_r7_structure.mjs"

green=$(timeout 1200 node "$verifier" /tmp fixture-anchor fixture-anchor --fixture "$script_dir/structural_fixtures/green.rs")
printf '%s\n' "$green"
printf '%s\n' "$green" | grep -q '^GREEN structural W5/R7 authority gate '

for fixture in negative_second_read.rs negative_second_writer.rs negative_fallback_alias.rs; do
  if timeout 1200 node "$verifier" /tmp "fixture-$fixture" fixture-anchor --fixture "$script_dir/structural_fixtures/$fixture" >"/tmp/w5-r7-$fixture.out" 2>&1; then
    echo "fixture unexpectedly passed: $fixture" >&2
    exit 1
  fi
  cat "/tmp/w5-r7-$fixture.out"
  grep -q '^RED structural authority gate:' "/tmp/w5-r7-$fixture.out"
done

echo 'structural fixtures: GREEN positive and 3 discriminating RED negatives'
