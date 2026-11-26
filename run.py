#!/usr/bin/env python3
"""Convenience entrypoint: python3 run.py [--target /path/to/bench] [--port 8787] [--cli] [--export snap.html]."""
from bench_dashboard.server import main

if __name__ == "__main__":
    raise SystemExit(main())
