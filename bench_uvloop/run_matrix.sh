#!/usr/bin/env bash
# Run uvloop's own echo benchmark (examples/bench) across loops x server modes x
# message sizes. Each cell runs the server, then the unmodified uvloop client,
# and records requests/sec. Runs are SEQUENTIAL on purpose: echo throughput is
# contention-sensitive, so concurrent cells would skew each other.
set -u

PY="${PY:-/Users/marcelotryle/dev/encode/zloop/.venv/bin/python}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PORT=25000
NUM="${NUM:-100000}"     # messages per worker
WORKERS="${WORKERS:-3}"  # uvloop default
BEST_OF="${BEST_OF:-3}"

loops=(asyncio uvloop zloop)
declare -A modes=(
  [proto]="--proto"
  [buffered]="--proto --buffered"
  [streams]="--streams"
)
sizes=(1000 10240 102400)   # 1 KiB, 10 KiB, 100 KiB

printf '%-10s %-9s %-8s %14s\n' loop mode msize "requests/sec"
printf -- '---------------------------------------------------\n'

for size in "${sizes[@]}"; do
  for mode in proto buffered streams; do
    for loop in "${loops[@]}"; do
      flag=""; [ "$loop" = "uvloop" ] && flag="--uvloop"; [ "$loop" = "zloop" ] && flag="--zloop"
      best=0
      for _ in $(seq "$BEST_OF"); do
        # start server
        PYTHONPATH=/Users/marcelotryle/dev/encode/zloop "$PY" "$HERE/echoserver.py" $flag ${modes[$mode]} \
          --addr "127.0.0.1:$PORT" >/dev/null 2>&1 &
        srv=$!
        # wait for the port to accept
        for _ in $(seq 50); do
          if "$PY" -c "import socket,sys; s=socket.socket(); s.settimeout(0.1)
sys.exit(0 if s.connect_ex(('127.0.0.1',$PORT))==0 else 1)" 2>/dev/null; then break; fi
          sleep 0.1
        done
        # run client, capture requests/sec
        rps=$(PYTHONPATH=/Users/marcelotryle/dev/encode/zloop "$PY" "$HERE/echoclient.py" \
              --msize "$size" --num "$NUM" --workers "$WORKERS" --addr "127.0.0.1:$PORT" 2>/dev/null \
              | awk '/requests\/sec/{print $1}')
        kill "$srv" 2>/dev/null; wait "$srv" 2>/dev/null
        # track best
        if [ -n "$rps" ]; then
          best=$("$PY" -c "print(max($best, $rps))")
        fi
        sleep 0.3
      done
      printf '%-10s %-9s %-8s %14.0f\n' "$loop" "$mode" "$size" "$best"
    done
    echo
  done
done
