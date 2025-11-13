#!/usr/bin/env bash
set -x  # or: set -v

# wait for sync to init
sleep 5

for ((run=1; ; run++)); do
  echo "=== Run #$run ===" >> /log

  if ./stress.sh
  then
    echo "Success!" >> log
  else
    status=$?
    echo "Stopping after run #$run (exit $status)." >> log
    break
  fi

  # avoid hammering apt mirrors
  sleep 5
done
