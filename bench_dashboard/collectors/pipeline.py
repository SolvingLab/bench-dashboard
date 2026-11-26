"""Generic read-only background pipeline, JSONL stream throughput, and backlog collector."""
from __future__ import annotations

import collections
import datetime as dt
import re
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

from ..utils import fmt_dt, get_tz, parse_iso, read_jsonl, resolve_path


def _scan_processes(repo: Path, proc_rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    try:
        ps = subprocess.run(["ps", "-eo", "pid,etime,pcpu,args"], capture_output=True, text=True, timeout=3).stdout
    except Exception:
        return items

    for line in ps.splitlines()[1:]:
        parts = line.strip().split(None, 3)
        if len(parts) < 4:
            continue
        pid, etime, pcpu_s, args = parts
        if "bench_dashboard" in args or "dashboard/server.py" in args:
            continue
        try:
            pcpu = float(pcpu_s)
        except ValueError:
            pcpu = 0.0

        for rule in proc_rules:
            pat = str(rule.get("match") or "")
            if not pat:
                continue
            is_regex = bool(rule.get("regex", False))
            matched = bool(re.search(pat, args)) if is_regex else (pat in args)
            if not matched:
                continue
            title = str(rule.get("name") or pat)
            grp_re = rule.get("group_regex")
            if grp_re:
                m = re.search(str(grp_re), args)
                if m:
                    title = f"{title} ({m.group(1)})"
            items.append({
                "id": f"proc-{pid}",
                "kind": str(rule.get("kind") or "custom"),
                "status": "running",
                "title": title,
                "detail": args[:140],
                "etime": etime,
                "pid": pid,
                "cpu_pct": round(pcpu, 1),
            })
            break

    items.sort(key=lambda x: (x["kind"], x["title"]))
    return items


def _collect_jsonl_stream(repo: Path, spec: dict[str, Any], tz: dt.tzinfo) -> dict[str, Any]:
    path = resolve_path(repo, spec["path"])
    rows = read_jsonl(path)
    ts_field = str(spec.get("ts_field") or "timestamp")
    status_field = str(spec.get("status_field") or "status")
    id_field = str(spec.get("id_field") or "id")
    elapsed_field = str(spec.get("elapsed_field") or "elapsed_s")
    note_field = str(spec.get("note_field") or "note")

    now_ts = time.time()
    now_dt = dt.datetime.fromtimestamp(now_ts, tz=tz)
    now_h = now_dt.replace(minute=0, second=0, microsecond=0)

    parsed: list[dict[str, Any]] = []
    status_counts: collections.Counter[str] = collections.Counter()
    elapsed_mins: list[float] = []

    for r in rows:
        st = str(r.get(status_field) or "unknown")
        status_counts[st] += 1
        d = parse_iso(str(r.get(ts_field) or r.get("ts") or r.get("finished_at") or ""), tz)
        ts_epoch = d.timestamp() if d else 0.0
        el = r.get(elapsed_field)
        el_m = round(float(el) / 60.0, 1) if isinstance(el, (int, float)) and el > 0 else None
        if el_m is not None:
            elapsed_mins.append(el_m)
        parsed.append({
            "id": str(r.get(id_field) or "-"),
            "status": st,
            "ts_epoch": ts_epoch,
            "ts_str": fmt_dt(d),
            "elapsed_min": el_m,
            "note": str(r.get(note_field) or r.get("error") or r.get("reason") or "")[:160],
        })

    def _win(hours: float) -> int:
        cut = now_ts - hours * 3600.0
        return sum(1 for x in parsed if x["ts_epoch"] >= cut)

    c1, c6, c24 = _win(1.0), _win(6.0), _win(24.0)
    rate_ph = round(c6 / 6.0, 2) if c6 >= 2 else round(c24 / 24.0, 2)

    h24_keys = [(now_h - dt.timedelta(hours=i)).strftime("%Y-%m-%d %H") for i in range(23, -1, -1)]
    h24_labels = [(now_h - dt.timedelta(hours=i)).strftime("%m-%d %H:00") for i in range(23, -1, -1)]
    h24_cnt = {k: 0 for k in h24_keys}
    for x in parsed:
        if x["ts_epoch"] >= now_ts - 90000:
            hk = dt.datetime.fromtimestamp(x["ts_epoch"], tz=tz).strftime("%Y-%m-%d %H")
            if hk in h24_cnt:
                h24_cnt[hk] += 1

    return {
        "name": str(spec.get("name") or path.stem),
        "path": str(spec["path"]),
        "exists": path.is_file(),
        "total": len(parsed),
        "count_1h": c1,
        "count_6h": c6,
        "count_24h": c24,
        "rate_ph": rate_ph,
        "rate_pd": round(rate_ph * 24.0, 1),
        "median_min": round(statistics.median(elapsed_mins), 1) if elapsed_mins else None,
        "status_counts": dict(status_counts.most_common(12)),
        "hourly_24h": {"labels": h24_labels, "counts": [h24_cnt[k] for k in h24_keys]},
        "recent": list(reversed(parsed[-30:])),
    }


def _read_table_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    if path.suffix.lower() in (".jsonl", ".ndjson"):
        return read_jsonl(path)
    out: list[dict[str, Any]] = []
    sep = "," if path.suffix.lower() == ".csv" else "\t"
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        if not lines:
            return out
        header = [h.strip() for h in lines[0].split(sep)]
        for ln in lines[1:]:
            if not ln.strip():
                continue
            out.append(dict(zip(header, [v.strip() for v in ln.split(sep)])))
    except OSError:
        return []
    return out


def _collect_backlog(repo: Path, bcfg: dict[str, Any], streams: list[dict[str, Any]]) -> dict[str, Any]:
    if not bcfg.get("enabled") or not bcfg.get("pool_path"):
        return {"enabled": False}
    pool_rows = _read_table_rows(resolve_path(repo, bcfg["pool_path"]))
    id_f = str(bcfg.get("id_field") or "id")
    cat_f = str(bcfg.get("category_field") or "category")
    tier_f = str(bcfg.get("tier_field") or "tier")
    dec_f = str(bcfg.get("decision_field") or "decision")

    done_ids: set[str] = set()
    for dp in bcfg.get("done_paths") or []:
        for r in _read_table_rows(resolve_path(repo, dp)):
            rid = str(r.get(id_f) or "").strip()
            if rid and rid != "-":
                done_ids.add(rid)

    remaining: list[dict[str, Any]] = []
    by_cat: collections.Counter[str] = collections.Counter()
    by_tier: collections.Counter[str] = collections.Counter()
    by_dec: collections.Counter[str] = collections.Counter()
    done_cnt = 0

    for r in pool_rows:
        rid = str(r.get(id_f) or "").strip()
        if not rid:
            continue
        if rid in done_ids:
            done_cnt += 1
            continue
        cat = str(r.get(cat_f) or "unknown")
        tier = str(r.get(tier_f) or "unknown")
        dec = str(r.get(dec_f) or "-")
        by_cat[cat] += 1
        by_tier[tier] += 1
        by_dec[dec] += 1
        remaining.append({"id": rid, "category": cat, "tier": tier, "decision": dec})

    pool_total = done_cnt + len(remaining)
    pct = round(done_cnt / max(1, pool_total) * 100.0, 1)
    rate_pd = streams[0]["rate_pd"] if streams and streams[0].get("rate_pd", 0) > 0 else 0.0
    eta_days = round(len(remaining) / rate_pd, 2) if rate_pd > 0 else None

    return {
        "enabled": True,
        "label": str(bcfg.get("label") or "Backlog"),
        "pool_total": pool_total,
        "done_total": done_cnt,
        "done_pct": pct,
        "remaining_total": len(remaining),
        "by_category": dict(by_cat.most_common()),
        "by_tier": dict(by_tier.most_common()),
        "by_decision": dict(by_dec.most_common()),
        "eta_days": eta_days,
        "items": remaining[:300],
    }


def collect_pipelines(cfg: dict[str, Any]) -> dict[str, Any]:
    pcfg = cfg.get("pipelines") or {}
    if not pcfg.get("enabled", True):
        return {"enabled": False, "processes": [], "streams": [], "backlog": {"enabled": False}}

    tz = get_tz(cfg["project"].get("tz_offset_hours"))
    repo = Path(cfg["project"]["target_repo"])
    procs = _scan_processes(repo, list(pcfg.get("processes") or []))
    streams = [
        _collect_jsonl_stream(repo, sp, tz)
        for sp in (pcfg.get("jsonl_streams") or [])
        if isinstance(sp, dict) and sp.get("path")
    ]
    backlog = _collect_backlog(repo, dict(pcfg.get("backlog") or {}), streams)
    return {
        "enabled": True,
        "processes": procs,
        "streams": streams,
        "backlog": backlog,
        "has_content": bool(procs or streams or backlog.get("enabled")),
    }
