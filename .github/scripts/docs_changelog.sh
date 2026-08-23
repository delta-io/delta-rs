#!/usr/bin/env bash
set -euo pipefail

FILE="../../CHANGELOG.md"
BASE_URL="https://github.com/delta-io/delta-rs/blob/main/CHANGELOG.md"

output="$(
  sed -nE '
  /^[[:space:]]*##[[:space:]]*\[rust-v/ {
    s|^[[:space:]]*##[[:space:]]*\[(rust-v)([0-9]+)\.([0-9]+)\.([0-9]+)\]\([^)]*\)[[:space:]]*\(([0-9-]+)\)|- [\1\2.\3.\4]('"$BASE_URL"'#rust-v\2\3\4-\5)|p
  }
  ' "$FILE" | sed -n '1,5p'
)"

# Fail CI if nothing was produced
if [ -z "$output" ]; then
  echo "ERROR: No rust versions found in $FILE" >&2
  exit 1
fi

printf "# Latest changes\n\n" > ../../docs/latest.md
printf "%s" "$output" >> ../../docs/latest.md
printf "\n\nOr see the [full changelog]($BASE_URL).\n" >> ../../docs/latest.md
