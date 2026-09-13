#!/usr/bin/env bash

set -euo pipefail

script_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
size_mib="${1:-512}"

if [[ ! "$size_mib" =~ ^[1-9][0-9]*$ ]]; then
  echo "Usage: $0 [size-in-MiB] [output-file]" >&2
  exit 2
fi

output_file="${2:-${script_directory}/input/random-numbers-${size_mib}mb.txt}"

# MiB matches the dashboard's binary MB display and HDFS block sizes.
target_bytes=$((size_mib * 1024 * 1024))

# Every record is padded to 15 characters plus one newline. The mapper trims
# whitespace before parsing, so each line remains a valid signed integer.
# One MiB is exactly divisible by this 16-byte record size.
record_bytes=16
record_count=$((target_bytes / record_bytes))

mkdir -p "$(dirname "$output_file")"
temporary_file="${output_file}.tmp.$$"
trap 'rm -f "$temporary_file"' EXIT

LC_ALL=C awk -v count="$record_count" -v seed="$(date +%s)" '
  BEGIN {
    srand(seed)
    for (record = 0; record < count; record++) {
      magnitude = int(rand() * 2147483647)
      value = rand() < 0.5 ? -magnitude : magnitude
      printf "%15d\n", value
    }
  }
' > "$temporary_file"

actual_bytes="$(wc -c < "$temporary_file" | tr -d ' ')"
if [[ "$actual_bytes" -ne "$target_bytes" ]]; then
  echo "Generation failed: expected $target_bytes bytes, got $actual_bytes bytes." >&2
  exit 1
fi

mv "$temporary_file" "$output_file"
trap - EXIT

printf 'Created %s\n' "$output_file"
printf 'Size: %s bytes (%s MiB)\n' "$actual_bytes" "$size_mib"
printf 'Records: %s\n' "$record_count"
