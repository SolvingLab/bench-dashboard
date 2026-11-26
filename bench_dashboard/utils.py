"""Shared stdlib-only helpers for timestamps, JSONL reading, and K8s unit parsing."""
from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Any


def get_tz(offset_hours: float | None = None) -> dt.tzinfo:
    if offset_hours is None:
        return dt.datetime.now().astimezone().tzinfo or dt.timezone.utc
    return dt.timezone(dt.timedelta(hours=offset_hours))


def parse_iso(s: str | None, tz: dt.tzinfo | None = None) -> dt.datetime | None:
    if not s or not isinstance(s, str):
        return None
    s = s.strip()
    if not s:
        return None
    tz = tz or get_tz()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        elif re.search(r"[+-]\d{4}$", s):
            s = s[:-2] + ":" + s[-2:]
        d = dt.datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=tz)
        return d.astimezone(tz)
    except Exception:
        return None


def fmt_dt(d: dt.datetime | None) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S") if d else ""


def fmt_epoch(ts: float | None, tz: dt.tzinfo | None = None) -> str:
    if not ts:
        return ""
    tz = tz or get_tz()
    return dt.datetime.fromtimestamp(ts, tz=tz).strftime("%Y-%m-%d %H:%M:%S")


def read_jsonl(path: Path, max_lines: int | None = None) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if max_lines is not None and len(lines) > max_lines:
            lines = lines[-max_lines:]
        for ln in lines:
            s = ln.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict):
                    out.append(obj)
            except json.JSONDecodeError:
                continue
    except OSError:
        return []
    return out


def parse_cpu(val: Any) -> float:
    """Parse Kubernetes CPU quantity (e.g. '250m', '500000n', '4', '2.5') -> cores."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    try:
        if s.endswith("n"):
            return float(s[:-1]) / 1e9
        if s.endswith("u"):
            return float(s[:-1]) / 1e6
        if s.endswith("m"):
            return float(s[:-1]) / 1e3
        if s.endswith("k"):
            return float(s[:-1]) * 1e3
        return float(s)
    except ValueError:
        return 0.0


def parse_mem_gib(val: Any) -> float:
    """Parse Kubernetes memory quantity (e.g. '16Gi', '4096Mi', '512000Ki', '1000000000') -> GiB."""
    if val is None:
        return 0.0
    s = str(val).strip()
    if not s:
        return 0.0
    units = [
        ("Ti", 1024.0),
        ("Gi", 1.0),
        ("Mi", 1.0 / 1024.0),
        ("Ki", 1.0 / (1024.0 ** 2)),
        ("T", 1000.0 ** 4 / (1024.0 ** 3)),
        ("G", 1000.0 ** 3 / (1024.0 ** 3)),
        ("M", 1000.0 ** 2 / (1024.0 ** 3)),
        ("K", 1000.0 / (1024.0 ** 3)),
        ("k", 1000.0 / (1024.0 ** 3)),
    ]
    for suf, mult in units:
        if s.endswith(suf):
            try:
                return float(s[: -len(suf)]) * mult
            except ValueError:
                return 0.0
    try:
        return float(s) / (1024.0 ** 3)
    except ValueError:
        return 0.0


def resolve_path(base: Path, raw: str | Path) -> Path:
    p = Path(raw).expanduser()
    return p if p.is_absolute() else (base / p)


def deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in override.items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = deep_merge(out[k], v)
        else:
            out[k] = v
    return out
