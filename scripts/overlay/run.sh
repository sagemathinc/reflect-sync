#!/usr/bin/env bash
set -x  # or: set -v

for ((run=1; ; run++)); do
  echo "=== Run #$run ==="

  if ./stress.sh
  then
    :  # success; loop again
  else
    status=$?
    echo "Stopping after run #$run (exit $status)."
    break
  fi

  # avoid hammering apt mirrors
  sleep 5
done
