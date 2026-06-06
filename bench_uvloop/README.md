# uvloop's benchmark, run against zloop

This runs [uvloop's own echo benchmark](https://github.com/MagicStack/uvloop/tree/master/examples/bench)
unchanged, with one addition: a `--zloop` flag on the server that mirrors the
existing `--uvloop` branch. The client (`echoclient.py`) is **byte-for-byte**
uvloop's; the server (`echoserver.py`) differs only by that one branch.

## Run it

```bash
# one cell
python echoserver.py --zloop --proto --addr 127.0.0.1:25000 &
python echoclient.py --msize 1000 --num 100000 --workers 3 --addr 127.0.0.1:25000

# the whole matrix (loops x {proto,buffered,streams} x {1,10,100 KiB}), best of 3
NUM=50000 WORKERS=3 BEST_OF=3 bash run_matrix.sh

# CI-friendly runner: portable paths, smaller matrix, Markdown-table output
scripts/bench                                  # asyncio vs uvloop vs zloop
scripts/bench --modes proto --sizes 1000       # narrow it down
```

`bench_ci.py` is what the **Benchmark** GitHub Actions workflow runs on Ubuntu
(real Linux), writing the table to the run's job summary. uvloop comes from the
`bench` dependency group. Linux is the platform that matters here - uvloop's lead
over asyncio is a Linux number, so the CI table is the meaningful comparison.

`run_matrix.sh` runs every cell **sequentially** — echo throughput is
contention-sensitive, so running cells concurrently would skew the numbers.

## Results (macOS arm64, CPython 3.14, best of 3)

requests/sec, higher is better:

| msg   | mode     | asyncio | uvloop | zloop  | zloop vs uvloop |
| ----- | -------- | ------: | -----: | -----: | --------------: |
| 1 KiB | proto    |  ~113k  | ~113k  | ~121k  | **+7%**         |
| 1 KiB | buffered |  ~115k  | ~115k  | ~123k  | **+7%**         |
| 1 KiB | streams  |   ~83k  |  ~90k  | ~103k  | **+14%**        |
| 10 KiB| proto    |  ~105k  | ~110k  | ~113k  | **+3%**         |
| 10 KiB| buffered |  ~105k  | ~105k  | ~124k  | **+18%**        |
| 10 KiB| streams  |   ~81k  |  ~86k  |  ~95k  | **+11%**        |
| 100 KiB| *       |    —    |   —    |   —    | loopback-bound (noisy) |

## Caveats

- **macOS / loopback / single machine.** uvloop's published "2-4x over asyncio"
  is a Linux number; on macOS's loopback + kqueue stack the spread is much
  tighter (even uvloop barely beats asyncio here). The *relative* ordering should
  hold on Linux, but absolute gaps will differ.
- **The 100 KiB row is not trustworthy on this setup.** At that size the test
  measures loopback bandwidth, not the event loop — all three loops swing wildly
  run-to-run. Measure large messages on real hardware if you care about them.
- Numbers vary ±10% run-to-run; the 1-10 KiB lead is reproducible.
