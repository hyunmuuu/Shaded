"""Run bot + periodic sync in a single terminal.

Usage (from project root):
  .\.venv\Scripts\python.exe -m tools.run_all

Options:
  --interval 600        # seconds (default: 600)
  --no-sync-on-start    # don't run sync immediately

Notes:
  - This keeps the "two-process" architecture (bot + sync) which matches your future GCP layout.
  - Outputs from both processes will appear in this terminal.
"""

from __future__ import annotations

import argparse
import os
import signal
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class Shared:
    stop: threading.Event
    bot_proc: Optional[subprocess.Popen]
    sync_proc: Optional[subprocess.Popen]
    lock: threading.Lock


def _terminate_process(p: Optional[subprocess.Popen], name: str, timeout_sec: float = 5.0) -> None:
    if not p:
        return
    if p.poll() is not None:
        return
    try:
        print(f"[STOP] terminating {name}...", flush=True)
        p.terminate()
        p.wait(timeout=timeout_sec)
    except Exception:
        try:
            print(f"[STOP] killing {name}...", flush=True)
            p.kill()
        except Exception:
            pass


def _run_sync_once(py: str, shared: Shared) -> int:
    if shared.stop.is_set():
        return 0

    cmd = [py, "-m", "tools.sync_weekly_kills"]
    try:
        with shared.lock:
            if shared.stop.is_set():
                return 0
            print("[SYNC] start", flush=True)
            shared.sync_proc = subprocess.Popen(cmd)
            proc = shared.sync_proc

        rc = proc.wait()
        print(f"[SYNC] end rc={rc}", flush=True)
        return int(rc)
    finally:
        with shared.lock:
            shared.sync_proc = None


def _scheduler_thread(py: str, interval_sec: int, sync_on_start: bool, shared: Shared) -> None:
    # fixed-rate schedule (best-effort): aim for every interval_sec boundary
    now = time.time()
    next_run = now if sync_on_start else (now + interval_sec)

    while not shared.stop.is_set():
        # sleep until next_run (responsive to stop)
        while not shared.stop.is_set():
            now = time.time()
            delay = next_run - now
            if delay <= 0:
                break
            time.sleep(min(delay, 0.5))

        if shared.stop.is_set():
            break

        _run_sync_once(py, shared)

        # schedule next
        next_run += interval_sec
        # if we're too far behind (e.g., sync took very long), reset to "interval from now"
        if next_run < time.time() - interval_sec:
            next_run = time.time() + interval_sec


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="tools.run_all")
    ap.add_argument("--interval", type=int, default=600, help="sync interval in seconds (default: 600)")
    ap.add_argument("--no-sync-on-start", action="store_true", help="do not run sync immediately")
    args = ap.parse_args(argv)

    py = sys.executable  # should be .venv\Scripts\python.exe when launched from venv
    env = os.environ.copy()

    shared = Shared(stop=threading.Event(), bot_proc=None, sync_proc=None, lock=threading.Lock())

    def _handle_sig(_signum, _frame):
        shared.stop.set()

    try:
        signal.signal(signal.SIGINT, _handle_sig)
        if hasattr(signal, "SIGTERM"):
            signal.signal(signal.SIGTERM, _handle_sig)
    except Exception:
        pass

    # Start bot
    bot_cmd = [py, "-m", "shaded"]
    print(f"[BOT] start: {' '.join(bot_cmd)}", flush=True)
    shared.bot_proc = subprocess.Popen(bot_cmd, env=env)

    # Start scheduler
    t = threading.Thread(
        target=_scheduler_thread,
        args=(py, int(args.interval), (not args.no_sync_on_start), shared),
        daemon=True,
        name="sync-scheduler",
    )
    t.start()

    # Wait
    try:
        while True:
            rc = shared.bot_proc.poll()
            if rc is not None:
                print(f"[BOT] exit rc={rc}", flush=True)
                shared.stop.set()
                break
            time.sleep(0.5)
    except KeyboardInterrupt:
        shared.stop.set()
        print("[STOP] cancelled by user", flush=True)

    # Shutdown children
    with shared.lock:
        sync_p = shared.sync_proc
        bot_p = shared.bot_proc

    _terminate_process(sync_p, "sync")
    _terminate_process(bot_p, "bot")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
