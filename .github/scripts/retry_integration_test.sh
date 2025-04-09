#!/bin/bash
set +e
TEST_NAME=$1
MAX_RETRIES=$2
RETRY_DELAY=$3
ATTEMPT=1
run_command() {
  uv run --no-sync pytest -m "($TEST_NAME and integration)" --doctest-modules 2>&1
}
until [ $ATTEMPT -gt $MAX_RETRIES ]
do
  echo "Attempt $ATTEMPT"
  OUTPUT=$(run_command)
  EXIT_CODE=$?
  echo "$OUTPUT"
  if [ $EXIT_CODE -eq 0 ]; then
    echo "Tests passed."
    exit 0
  fi
  echo "Failed. Retrying."
  ATTEMPT=$((ATTEMPT+1))
  sleep $RETRY_DELAY
done
if [ $ATTEMPT -gt $MAX_RETRIES ]; then
  echo "Tests failed after $MAX_RETRIES attempts."
  exit 1
fi
