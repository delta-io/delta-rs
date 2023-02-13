#!/bin/bash

# invokes the changelog generator from
# https://github.com/github-changelog-generator/github-changelog-generator
#
# With the config located in
# delta-rs/.github_changelog_generator
#
# Usage:
# GITHUB_API_TOKEN=<TOKEN> ./update_change_log.sh

set -e

LANGUAGE="rust"
SINCE_VERSION="0.7.0"
FUTURE_RELEASE="0.8.0"

# only consider tags of the correct language
if [ "$LANGUAGE" == "rust" ]; then
	EXCLUDED_TAGS_REGEX="python.*"
  INCLUDED_LABELS="rust"
elif [ "$LANGUAGE" == "python" ]; then
	EXCLUDED_TAGS_REGEX="rust.*"
  INCLUDED_LABELS="python,binding/python"
else
  echo "Language $LANGUAGE is invalid. Should be one of Python, Ruby and Rust."
fi

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

OUTPUT_PATH="${SOURCE_TOP_DIR}/CHANGELOG.md"
OLD_OUTPUT_PATH="${SOURCE_TOP_DIR}/CHANGELOG-old.md"

# remove header so github-changelog-generator has a clean base to append
sed -i '1d' "${OUTPUT_PATH}"
sed -i '1d' "${OLD_OUTPUT_PATH}"
# remove the github-changelog-generator footer from the old CHANGELOG.md
LINE_COUNT=$(wc -l <"${OUTPUT_PATH}")
sed -i "$(( $LINE_COUNT-3 )),$ d" "${OUTPUT_PATH}"

# Copy the previous CHANGELOG.md to CHANGELOG-old.md
echo '# Historical Changelog
' | cat - "${OUTPUT_PATH}" "${OLD_OUTPUT_PATH}" > "${OLD_OUTPUT_PATH}".tmp
mv "${OLD_OUTPUT_PATH}".tmp "${OLD_OUTPUT_PATH}"

# use exclude-tags-regex to filter out tags used in the wrong language
pushd "${SOURCE_TOP_DIR}"
docker run -it --rm -e CHANGELOG_GITHUB_TOKEN="$GITHUB_API_TOKEN" -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator \
    --user delta-io \
    --project delta-rs \
    --cache-file=.githubchangeloggenerator.cache \
    --cache-log=.githubchangeloggenerator.cache.log \
    --http-cache \
    --max-issues=300 \
    --include-labels "${INCLUDED_LABELS}" \
    --exclude-labels dependencies \
    --exclude-tags-regex "${EXCLUDED_TAGS_REGEX}" \
    --since-tag ${LANGUAGE}-v${SINCE_VERSION} \
    --future-release ${LANGUAGE}-v${FUTURE_RELEASE}

sed -i.bak "s/\\\n/\n\n/" "${OUTPUT_PATH}"
