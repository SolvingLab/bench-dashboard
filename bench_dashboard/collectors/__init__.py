"""Top-level parallel snapshot aggregator, built-in safety alerts, and optional custom adapter hook."""
from __future__ import annotations

import concurrent.futures
import datetime as dt
import importlib.util
import time
from pathlib import Path
from typing import Any

from ..utils import get_tz, resolve_path
from .harbor import collect_harbor
from .host import collect_host
from .k8s import collect_k8s
from .pipeline import collect_pipelines

_ADAPTER_CACHE: dict[str, tuple[float, Any]] = {}


def _load_custom_adapter(adapter_path: Path) -> Any | None:
    if not adapter_path.is_file():
        return None
    mt = adapter_path.stat().st_mtime
    key = str(adapter_path)
    cached = _ADAPTER_CACHE.get(key)
    if cached is not None and cached[0] == mt:
        return cached[1]
    spec = importlib.util.spec_from_file_location("bench_dashboard_custom_adapter", str(adapter_path))
    if spec is None or spec.loader is None:
        return None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    _ADAPTER_CACHE[key] = (mt, mod)
    return mod


def _build_alerts(cfg: dict[str, Any], host: dict[str, Any], k8s: dict[str, Any]) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    acfg = (cfg.get("host") or {}).get("alerts") or {}

    if host.get("ok"):
        # Swap & /dev/shm safety checks
        sw_pct = float(host.get("swap_used_pct") or 0.0)
        sw_tot = float(host.get("swap_total_gb") or 0.0)
        sw_used = float(host.get("swap_used_gb") or 0.0)
        sw_free = float(host.get("swap_free_gb") or 0.0)
        top_sw = host.get("top_swap") or []
        top_s = " | Top: " + ", ".join(f"{x['user']}:{x['cmd']}({x['swap_gb']}G)" for x in top_sw[:3]) if top_sw else ""
        if sw_tot > 0 and sw_pct >= float(acfg.get("swap_danger_pct", 80.0)):
            alerts.append({
                "level": "danger",
                "stage": "Host Swap Critical",
                "msg": f"Swap {sw_used}/{sw_tot} GiB ({sw_pct}%, {sw_free} GiB free){top_s}",
            })
        elif sw_tot > 0 and sw_pct >= float(acfg.get("swap_warn_pct", 50.0)):
            alerts.append({
                "level": "warn",
                "stage": "Host Swap High",
                "msg": f"Swap {sw_used}/{sw_tot} GiB ({sw_pct}%, {sw_free} GiB free){top_s}",
            })

        shm = host.get("shm") or {}
        if shm.get("ok") and shm.get("total_gb", 0) > 0:
            shm_pct = float(shm.get("use_pct") or 0.0)
            if shm_pct >= float(acfg.get("shm_danger_pct", 60.0)):
                alerts.append({
                    "level": "danger",
                    "stage": "/dev/shm Critical",
                    "msg": f"/dev/shm {shm['used_gb']}/{shm['total_gb']} GiB ({shm_pct}%) — clean stale shared-memory files",
                })
            elif shm_pct >= float(acfg.get("shm_warn_pct", 40.0)):
                alerts.append({
                    "level": "warn",
                    "stage": "/dev/shm High",
                    "msg": f"/dev/shm {shm['used_gb']}/{shm['total_gb']} GiB ({shm_pct}%)",
                })

        d_warn = float(acfg.get("disk_free_warn_gb", 40.0))
        d_danger = float(acfg.get("disk_free_danger_gb", 20.0))
        for d in host.get("disks") or []:
            if not d.get("ok") or not d.get("critical"):
                continue
            if d["free_gb"] < d_danger:
                alerts.append({
                    "level": "danger",
                    "stage": f"Disk Low: {d['label']}",
                    "msg": f"Only {d['free_gb']} GiB free ({d['use_pct']}% used) at {d['path']}",
                })
            elif d["free_gb"] < d_warn:
                alerts.append({
                    "level": "warn",
                    "stage": f"Disk Tight: {d['label']}",
                    "msg": f"{d['free_gb']} GiB free ({d['use_pct']}% used) at {d['path']}",
                })

    if k8s.get("ok"):
        h = k8s.get("health") or {}
        if h.get("ok") is False:
            alerts.append({"level": "danger", "stage": "K8s Cluster Health", "msg": str(h.get("msg") or "Unhealthy")})
        dp = k8s.get("disk_pressure_nodes") or []
        if dp:
            alerts.append({"level": "warn", "stage": "K8s DiskPressure", "msg": f"Nodes under DiskPressure: {', '.join(dp)}"})
        if (k8s.get("idle_suspect_pods") or 0) >= 3:
            alerts.append({
                "level": "warn",
                "stage": "K8s Low-CPU Pods",
                "msg": f"{k8s['idle_suspect_pods']} running pods have low CPU (waiting on API / download or stalled)",
            })

    return alerts


def collect_snapshot(cfg: dict[str, Any]) -> dict[str, Any]:
    t0 = time.time()
    tz = get_tz(cfg["project"].get("tz_offset_hours"))
    now_dt = dt.datetime.now(tz)

    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        f_host = ex.submit(collect_host, cfg)
        f_k8s = ex.submit(collect_k8s, cfg)
        f_harbor = ex.submit(collect_harbor, cfg)
        f_pipe = ex.submit(collect_pipelines, cfg)

        host = f_host.result()
        k8s = f_k8s.result()
        harbor = f_harbor.result()
        pipelines = f_pipe.result()

    alerts = _build_alerts(cfg, host, k8s)

    snap: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": now_dt.strftime("%Y-%m-%d %H:%M:%S %z"),
        "generated_epoch": now_dt.timestamp(),
        "collect_sec": round(time.time() - t0, 2),
        "project": {
            "name": cfg["project"]["name"],
            "badge": cfg["project"]["badge"],
            "target_repo": cfg["project"]["target_repo"],
            "locale": cfg["project"].get("locale", "auto"),
            "refresh_sec": int(cfg["project"].get("refresh_sec", 20)),
        },
        "alerts": alerts,
        "host": host,
        "k8s": k8s,
        "harbor": harbor,
        "pipelines": pipelines,
        "custom": {
            "kpis": [],
            "panels": [],
        },
    }

    # Optional project-specific read-only Python adapter
    adapter_raw = str((cfg.get("pipelines") or {}).get("custom_adapter") or "").strip()
    if adapter_raw:
        base_dir = Path(cfg.get("_config_dir") or cfg["project"]["target_repo"])
        ap = resolve_path(base_dir, adapter_raw)
        try:
            mod = _load_custom_adapter(ap)
            if mod is not None and hasattr(mod, "extend_snapshot"):
                mod.extend_snapshot(snap, cfg)
        except Exception as e:
            snap["alerts"].append({
                "level": "warn",
                "stage": "Custom Adapter Error",
                "msg": f"{ap.name}: {str(e)[:180]}",
            })

    return snap
