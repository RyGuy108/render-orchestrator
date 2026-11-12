"""Process supervisor for the worker pool.

Spawns N worker processes, restarts any that die (exponential restart backoff so a crash loop
does not spin), and on SIGTERM forwards it and waits for a graceful drain.
Run:  python -m orchestrator.supervisor
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import sys
import time

from .config import settings

log = logging.getLogger("supervisor")


def spawn(i: int) -> subprocess.Popen:
    return subprocess.Popen([sys.executable, "-m", "orchestrator.worker", "--id", f"w{i}"])


def main() -> None:
    logging.basicConfig(level="INFO", format="%(asctime)s %(name)s %(message)s")
    n = settings.workers
    procs = {i: spawn(i) for i in range(n)}
    restarts = {i: 0 for i in range(n)}
    stopping = False

    def stop(signum, _frame):
        nonlocal stopping
        stopping = True
        for p in procs.values():
            if p.poll() is None:
                p.send_signal(signal.SIGTERM)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    log.info("supervising %d workers (pid %d)", n, os.getpid())

    while True:
        time.sleep(0.2)
        if stopping:
            deadline = time.time() + settings.mh_timeout_s + 10
            while time.time() < deadline and any(p.poll() is None for p in procs.values()):
                time.sleep(0.2)
            for p in procs.values():
                if p.poll() is None:
                    p.kill()
            log.info("all workers stopped")
            return
        for i, p in list(procs.items()):
            code = p.poll()
            if code is not None:
                restarts[i] += 1
                delay = min(5.0, 0.1 * 2 ** min(restarts[i], 6))
                log.warning("worker w%d exited with %s; restart #%d in %.1fs", i, code, restarts[i], delay)
                time.sleep(delay)
                procs[i] = spawn(i)


if __name__ == "__main__":
    main()
