#!/bin/bash

# invokes the changelog generator from
# https://github.com/github-changelog-generator/github-changelog-generator
#
# With the config located in
# delta-rs/.github_changelog_generator
#
# Usage:
# GITHUB_API_TOKEN=<TOKEN> ./update_change_log.sh
#
# Please edit the resulting change log to remove remaining irrelevant changes
# and to put interesting changes (features, bugfixes) above all the minor 
# changes (depandabot updates).

set -e

LANGUAGE="rust"
SINCE_VERSION="0.6.0"
FUTURE_RELEASE="0.7.0"

# only consider tags of the correct language
if [ "$LANGUAGE" == "rust" ]; then
	EXCLUDED_LANGUAGES_REGEX=".*python.*"
elif [ "$LANGUAGE" == "python" ]; then
	EXCLUDED_LANGUAGES_REGEX=".*rust.*"
else
  echo "Language $LANGUAGE is invalid. Should be one of Python and Rust."
fi

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

OUTPUT_PATH="${SOURCE_TOP_DIR}/CHANGELOG.md"

# remove header so github-changelog-generator has a clean base to append
sed -i '1d' "${OUTPUT_PATH}"
# remove the github-changelog-generator footer from the old CHANGELOG.md
LINE_COUNT=$(wc -l <"${OUTPUT_PATH}")
sed -i "$(( $LINE_COUNT-3 )),$ d" "${OUTPUT_PATH}"

# Copy the previous CHANGELOG.md to CHANGELOG-old.md
cat - "${OUTPUT_PATH}" > "${OUTPUT_PATH}".tmp

# use exclude-tags-regex to filter out tags used in the wrong language
pushd "${SOURCE_TOP_DIR}"
docker run -it --rm -e CHANGELOG_GITHUB_TOKEN="$GITHUB_API_TOKEN" \
    -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator \
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

# Remove footer from github-changelog-generator (we already have one at bottom)
LINE_COUNT=$(wc -l <"${OUTPUT_PATH}")
sed -i.bak "$(( $LINE_COUNT-3 )),$ d" "${OUTPUT_PATH}"

# Add historical change log back in
cat $HISTORIAL_PATH >> $OUTPUT_PATH

# Remove temporary files
rm $HISTORIAL_PATH