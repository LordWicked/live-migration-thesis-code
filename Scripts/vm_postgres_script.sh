#!/usr/bin/env bash
set -u

done_file="${1:-/tmp/bench.done}"
out_file="${2:-/tmp/pg-buffer-hit-ratio.csv}"
database="${3:-ycsb}"

echo "timestamp,elapsed_sec,blks_hit,blks_read,hit_delta,read_delta,buffer_hit_ratio_pct" > "$out_file"

prev_hit=""
prev_read=""
start="$(date +%s)"

while true; do
  row="$(
  PGPASSWORD=pass psql -h 127.0.0.1 -U ycsb -d ycsb -At -F, -c \
    "SELECT extract(epoch from clock_timestamp())::bigint, blks_hit, blks_read FROM pg_stat_database WHERE datname = 'ycsb';"
)"

  if [ -z "$row" ]; then
    echo "No pg_stat_database row returned for database '$database'" >&2
  else
    ts="${row%%,*}"
    rest="${row#*,}"
    hit="${rest%%,*}"
    read="${rest#*,}"

    if [ -n "$prev_hit" ]; then
      hit_delta=$((hit - prev_hit))
      read_delta=$((read - prev_read))
      ratio="$(awk -v h="$hit_delta" -v r="$read_delta" 'BEGIN { if (h+r == 0) print ""; else printf "%.2f", 100*h/(h+r) }')"
      echo "$ts,$((ts - start)),$hit,$read,$hit_delta,$read_delta,$ratio" >> "$out_file"
    else
      echo "$ts,$((ts - start)),$hit,$read,,," >> "$out_file"
    fi

    prev_hit="$hit"
    prev_read="$read"
  fi

  [ -f "$done_file" ] && break
  sleep 1
done