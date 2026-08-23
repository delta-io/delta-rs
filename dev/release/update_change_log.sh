#!/bin/bash

# invokes the changelog generator from
# https://github.com/github-changelog-generator/github-changelog-generator
#
# With the config located in
# delta-rs/.github_changelog_generator
#
# Usage:
# GH_TOKEN=<TOKEN> ./update_change_log.sh
#
# Please edit the resulting change log to remove remaining irrelevant changes
# and to put interesting changes (features, bugfixes) above all the minor 
# changes (depandabot updates).

set -e

LANGUAGE=${LANGUAGE:-"rust"}
SINCE_VERSION=${SINCE_VERSION:-"0.30.0"}
FUTURE_RELEASE=${FUTURE_RELEASE:-"0.31.0"}

# only consider tags of the correct language
if [ "$LANGUAGE" == "rust" ]; then
	EXCLUDED_LANGUAGES_REGEX=".*python.*"
elif [ "$LANGUAGE" == "python" ]; then
	EXCLUDED_LANGUAGES_REGEX=".*rust.*"
else
  echo "Language $LANGUAGE is invalid. Should be one of 'python' or 'rust'"
fi

SOURCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_TOP_DIR="$(cd "${SOURCE_DIR}/../../" && pwd)"

OUTPUT_PATH="${SOURCE_TOP_DIR}/CHANGELOG.md"
HISTORICAL_PATH="${SOURCE_TOP_DIR}/CHANGELOG-old.md"

cp $OUTPUT_PATH $HISTORICAL_PATH

# Remove header from historical change logs; will add back at the end.
sed -i.bak '1d' "${HISTORICAL_PATH}"

# use exclude-tags-regex to filter out tags used in the wrong language
pushd "${SOURCE_TOP_DIR}"
docker run -it --rm -e CHANGELOG_GITHUB_TOKEN="$GH_TOKEN" \
    -v "$(pwd)":/usr/local/src/your-app githubchangeloggenerator/github-changelog-generator \
    --user delta-io \
    --project delta-rs \
    --cache-file=.githubchangeloggenerator.cache \
    --cache-log=.githubchangeloggenerator.cache.log \
    --http-cache \
    --max-issues=300 \
    --exclude-labels dependencies \
    --exclude-tags-regex "${EXCLUDED_LANGUAGES_REGEX}" \
    --since-tag ${LANGUAGE}-v${SINCE_VERSION} \
    --future-release ${LANGUAGE}-v${FUTURE_RELEASE}

# Remove footer from github-changelog-generator (we already have one at bottom)
LINE_COUNT=$(wc -l <"${OUTPUT_PATH}")
sed -i.bak "$(( $LINE_COUNT-3 )),$ d" "${OUTPUT_PATH}"

# Add historical change log back in
cat $HISTORICAL_PATH >> $OUTPUT_PATH

# Remove temporary files
rm $HISTORICAL_PATH
