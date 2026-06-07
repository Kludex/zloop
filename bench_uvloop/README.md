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

## Results (Linux io_uring, kernel 6.10, best of 3)

The meaningful comparison: real Linux, where the two zloop backends are measured
against each other and against uvloop/asyncio. `num=30000` msgs/worker,
`workers=3`, requests/sec, higher is better.

### Single loop (GIL on, CPython 3.14)

| mode    | size    | asyncio | uvloop  | zloop (epoll) | zloop (io_uring) | epoll vs uvloop |
| ------- | ------: | ------: | ------: | ------------: | ---------------: | --------------: |
| proto   | 1 KB    | 139,888 | 133,536 |   **143,592** |           75,653 | **+7.5%**       |
| proto   | 100 KiB |  61,691 |  58,206 |        57,641 |           21,090 | -1.0%           |
| streams | 1 KB    |  94,972 |  73,588 |   **130,618** |           85,476 | **+77.5%**      |
| streams | 100 KiB |  42,578 |  40,188 |    **47,062** |           18,236 | **+17.1%**      |

For a single GIL-bound loop, the **default epoll backend beats uvloop** and the
**io_uring completion backend is slower** - its submit/reap overhead and the
buffer-ring copy aren't amortized when one GIL-serialized loop is the bottleneck.
Completion is not for this case (see below).

### Free-threaded parallel loops (GIL off, CPython 3.14t)

N independent loops on N threads, 8 conns/thread, 1 KB messages, 2s each, 3-sample
medians (12-CPU host). uvloop runs here too: it imports on 3.14t without forcing
the GIL back on and drives one loop per thread fine.

| threads | zloop epoll | zloop io_uring | uvloop    | completion vs uvloop |
| ------: | ----------: | -------------: | --------: | -------------------: |
|       1 |     157,684 |        199,489 |   173,537 | **+15.0%**           |
|       2 |     272,095 |        358,838 |   321,936 | **+11.5%**           |
|       4 |     480,934 |        629,977 |   607,786 | **+3.7%**            |
|       8 |     647,740 |        917,239 | 1,029,682 | -10.9%               |
|      16 |     726,142 |      1,089,033 | 1,268,657 | -14.2%               |

The completion backend (`ZLOOP_IO_URING=completion`) **beats uvloop at 1-4 parallel
free-threaded loops** and beats zloop's own epoll default everywhere. The wins come
from making the io_uring path lean: batched submits (one `io_uring_enter` per loop
turn, not per op), multishot recv, and completion-path writes (SEND on the ring, no
per-message `write()` syscall) - together they cut a busy single loop from ~17.7k to
~3.6k enters and drop write syscalls to zero.

Past ~8 loops on this 12-CPU host the workload is oversubscribed and the bottleneck
moves from syscall efficiency to CPU-per-request, where uvloop's mature hot path
still leads; closing that means cutting per-request CPU (e.g. the per-recv PyBytes
copy), not more io_uring tuning.

## Caveats

- **macOS / loopback / single machine.** uvloop's published "2-4x over asyncio"
  is a Linux number; on macOS's loopback + kqueue stack the spread is much
  tighter (even uvloop barely beats asyncio here). The *relative* ordering should
  hold on Linux, but absolute gaps will differ.
- **The 100 KiB row is not trustworthy on this setup.** At that size the test
  measures loopback bandwidth, not the event loop — all three loops swing wildly
  run-to-run. Measure large messages on real hardware if you care about them.
- Numbers vary ±10% run-to-run; the 1-10 KiB lead is reproducible.
- **The io_uring tables are Linux-only** (the backend doesn't exist on macOS) and
  were measured in a kernel-6.10 VM, not bare metal. Treat the parallel-loop
  speedups as directional - the trend (completion pulls ahead as loops parallelize
  off the GIL) is the point, not the exact percentages.
