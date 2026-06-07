"""Run the uvloop echo benchmark across loops and emit a Markdown table.

Drives the existing echoserver.py / echoclient.py over a small, CI-friendly
matrix (loops x mode x message size), best-of-N, and prints a Markdown table.
Portable: no hardcoded paths, resolves everything relative to this file.

Usage:
    python bench_uvloop/bench_ci.py [--loops asyncio,uvloop,zloop] [--num 30000]
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SERVER = HERE / "echoserver.py"
CLIENT = HERE / "echoclient.py"

MODES: dict[str, list[str]] = {
    "proto": ["--proto"],
    "buffered": ["--proto", "--buffered"],
    "streams": ["--streams"],
}
LOOP_FLAG = {"asyncio": [], "uvloop": ["--uvloop"], "zloop": ["--zloop"]}


def wait_port(port: int, timeout: float = 10.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with socket.socket() as s:
            s.settimeout(0.1)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(0.05)
    return False


def run_cell(py: str, loop: str, mode: str, size: int, num: int, workers: int, port: int) -> float:
    env = {**os.environ, "PYTHONPATH": str(ROOT)}
    server = subprocess.Popen(
        [py, str(SERVER), *LOOP_FLAG[loop], *MODES[mode], "--addr", f"127.0.0.1:{port}"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )
    try:
        if not wait_port(port):
            return 0.0
        out = subprocess.run(
            [
                py,
                str(CLIENT),
                "--msize",
                str(size),
                "--num",
                str(num),
                "--workers",
                str(workers),
                "--addr",
                f"127.0.0.1:{port}",
            ],
            capture_output=True,
            text=True,
            env=env,
            timeout=120,
        ).stdout
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
    for line in out.splitlines():
        if "requests/sec" in line:
            return float(line.split()[0])
    return 0.0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--loops", default="asyncio,uvloop,zloop")
    ap.add_argument("--modes", default="proto,buffered,streams")
    ap.add_argument("--sizes", default="1000,10240,102400")
    ap.add_argument("--num", type=int, default=50000)
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--best-of", type=int, default=5)
    ap.add_argument("--python", default=sys.executable)
    ap.add_argument("--json", type=Path, default=None, help="also write results as JSON to this path")
    args = ap.parse_args()

    loops = [x for x in args.loops.split(",") if x]
    modes = [x for x in args.modes.split(",") if x]
    sizes = [int(x) for x in args.sizes.split(",") if x]

    rows: list[tuple[str, int, dict[str, float]]] = []
    port = 25000
    for mode in modes:
        for size in sizes:
            cell: dict[str, float] = {}
            for loop in loops:
                best = 0.0
                for _ in range(args.best_of):
                    port += 1
                    rps = run_cell(args.python, loop, mode, size, args.num, args.workers, port)
                    best = max(best, rps)
                    time.sleep(0.2)
                cell[loop] = best
            rows.append((mode, size, cell))

    if args.json is not None:
        payload = {
            "meta": {
                "num": args.num,
                "workers": args.workers,
                "best_of": args.best_of,
                "python": f"{sys.version_info.major}.{sys.version_info.minor}",
                "loops": loops,
            },
            "rows": [{"mode": mode, "size": size, "rps": cell} for mode, size, cell in rows],
        }
        args.json.write_text(json.dumps(payload, indent=2) + "\n")

    print(f"## Echo benchmark (requests/sec, best of {args.best_of})\n")
    print(
        f"`num={args.num}` messages/worker, `workers={args.workers}`, "
        f"Python {sys.version_info.major}.{sys.version_info.minor}\n"
    )
    header = "| mode | size | " + " | ".join(loops) + " | zloop vs uvloop |"
    sep = "| --- | ---: | " + " | ".join(["---:"] * len(loops)) + " | ---: |"
    print(header)
    print(sep)
    for mode, size, cell in rows:
        cells = " | ".join(f"{cell.get(loop, 0):,.0f}" for loop in loops)
        delta = ""
        if "zloop" in cell and "uvloop" in cell and cell["uvloop"] > 0:
            pct = (cell["zloop"] / cell["uvloop"] - 1) * 100
            delta = f"{pct:+.1f}%"
        label = f"{size / 1024:.0f} KiB" if size >= 1024 else f"{size} B"
        print(f"| {mode} | {label} | {cells} | {delta} |")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
