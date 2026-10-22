"""Start API, dispatcher and N local worker processes using this uv environment."""

import argparse
import os
import signal
import subprocess
import sys
import time


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--workers", type=int, default=2)
    args = parser.parse_args()
    if not 1 <= args.workers <= 16:
        parser.error("--workers must be 1..16")
    env = dict(os.environ, DF_EXECUTION_MODE="local")
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], env=env, check=True)
    commands = [
        ["uvicorn", "app.main:app", "--host", args.host, "--port", str(args.port)],
        ["app.scheduler.dispatcher"],
        ["app.scheduler.maintenance"],
        *[["app.workers.local"] for _ in range(args.workers)],
    ]
    children = []
    stopping = False

    def stop(*_):
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for command in commands:
            children.append(subprocess.Popen([sys.executable, "-m", *command], env=env))
        print(f"DataFlow Jobs: http://{args.host}:{args.port} ({args.workers} workers)", flush=True)
        while not stopping:
            if any(child.poll() is not None for child in children):
                raise RuntimeError("A child process exited; see logs above")
            time.sleep(0.3)
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        deadline = time.monotonic() + 30
        for child in children:
            try:
                child.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait()


if __name__ == "__main__":
    main()
