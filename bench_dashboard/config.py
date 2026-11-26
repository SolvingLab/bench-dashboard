"""Configuration loader with zero-config auto-detection and environment overrides.

Uses Python 3.11+ stdlib `tomllib` only.
"""
from __future__ import annotations

import os
import socket
import tomllib
from pathlib import Path
from typing import Any

from .utils import deep_merge, resolve_path

DEFAULT_CONFIG: dict[str, Any] = {
    "project": {
        "name": "",              # Auto-filled from target_repo directory name
        "badge": "BD",           # 2-char sidebar logo text
        "target_repo": ".",      # Path to the benchmark repository being monitored
        "locale": "auto",        # "auto" | "en" | "zh"
        "tz_offset_hours": None, # None = host local timezone
        "refresh_sec": 20,
        "history_file": ".live_history.jsonl",
        "history_max_points": 180,
    },
    "host": {
        "enabled": True,
        "label": "",             # Auto-filled from hostname
        "docker_host": "",       # Empty = inherit DOCKER_HOST or default unix:///var/run/docker.sock
        "docker_bench_keywords": ["harbor", "__env", "__verifier", "bench", "trial"],
        "disks": [
            # Empty list => auto-populated with target_repo disk, /tmp, and /dev/shm
        ],
        "alerts": {
            "disk_free_warn_gb": 40.0,
            "disk_free_danger_gb": 20.0,
            "swap_warn_pct": 50.0,
            "swap_danger_pct": 80.0,
            "shm_warn_pct": 40.0,
            "shm_danger_pct": 60.0,
        },
    },
    "k8s": {
        "enabled": "auto",       # True | False | "auto" (enabled if kubeconfig exists)
        "kubeconfig": "",        # Empty = KUBECONFIG env or ~/.kube/config
        "namespace": "default",  # Overridden by K8S_NAMESPACE or kubeconfig context namespace
        "strip_proxy_env": "auto",# True | False | "auto" (auto-bypass HTTP_PROXY for private LAN / loopback API IPs)
        "owner_prefixes": [],    # e.g. ["alice-"] to highlight "mine" vs "other" in shared namespace
        "owner_labels": {},      # e.g. {"owner": "alice"} or {"app.kubernetes.io/part-of": "my-bench"}
        "workload_prefixes": [], # Filter pods/jobs shown in Active K8s Workloads table (empty = all in namespace)
        "workload_part_of": [],  # Additional app.kubernetes.io/part-of values to include as tracked workloads
        "gpu_resource": "auto",  # "auto" | "nvidia.com/gpu" | "tencent.com/vcuda-core"
        "vcuda_core_per_gpu": 100,
        "vcuda_mem_units_per_gib": 4,
        "vcuda_default_mem_gib": 80.0,
        "idle_cpu_threshold": 0.05,
        "idle_age_min": 15.0,
        "shared_storage_paths": [], # Optional mounted shared paths (e.g. NFS) to display usage: [{"label":"NFS","path":"/mnt/shared"}]
        "watch_deployments_or_pods": [], # Optional pod name prefixes that must be Ready (e.g. ["harbor-registry-"])
        "watch_event_reasons": ["FailedMount"],
    },
    "harbor": {
        "enabled": "auto",       # True | False | "auto" (enabled if tasks/ or jobs/ exists)
        "dataset_toml": "tasks/dataset.toml",
        "jobs_dir": "jobs",
        "checksum_cache": "cache/bench-status/checksums.json",
        "primary_model": "",     # Empty = auto-pick model with most valid trials
        "doc_only_prefixes": ["authoring/", "README.md"],
        "expected_rewards": {"nop": 0, "oracle": 1},
        "scored_exceptions": ["AgentTimeoutError"],
        "score_metric_keys": ["position_score", "score", "accuracy", "pass_rate"],
        "primary_metric_keys": ["primary_metric", "metric_value"],
        "rank_metric_keys": ["rank_percentile"],
        "score_scale": "auto",   # "auto" | "unit" (0..1) | "percent" (0..100)
        "slot_lock_dir": "",     # Optional flock directory (e.g. /tmp/harbor-subnet-locks-{uid})
        "slots_total": 0,        # Optional total concurrency slots
        "buckets": [
            # Empty => auto-discover tasks/*/* (or tasks/*), plus lite/* and candidates/* if present
        ],
    },
    "pipelines": {
        "enabled": True,
        "processes": [
            # Example:
            # {name = "Harbor Runner", match = "harbor_slots.py", kind = "harbor"}
        ],
        "jsonl_streams": [
            # Example:
            # {name = "Build Results", path = "work/build-results.jsonl", ts_field = "timestamp", status_field = "status", id_field = "task_id"}
        ],
        "backlog": {
            "enabled": False,
            "pool_path": "",       # JSONL or TSV of candidate tasks
            "done_paths": [],      # JSONL or TSV files recording attempted/completed IDs
            "id_field": "id",
            "category_field": "category",
            "tier_field": "tier",
            "decision_field": "decision",
            "label": "Backlog",
        },
        "custom_adapter": "",      # Optional path to a Python module defining extend_snapshot(snap, cfg)
    },
}


def load_config(
    config_path: Path | None = None,
    target_repo: Path | None = None,
    no_k8s: bool = False,
) -> dict[str, Any]:
    cfg = deep_merge(DEFAULT_CONFIG, {})
    cfg_dir: Path | None = None

    # Auto-discover config file if not explicitly passed
    if config_path is None:
        search_roots = []
        if target_repo is not None:
            search_roots.append(target_repo.resolve())
        search_roots.append(Path.cwd().resolve())
        search_roots.append(Path(__file__).resolve().parents[1])
        for root in search_roots:
            for fname in ("bench-dashboard.toml", "dashboard.toml", ".bench-dashboard.toml"):
                cand = root / fname
                if cand.is_file():
                    config_path = cand
                    break
            if config_path is not None:
                break

    if config_path is not None and config_path.is_file():
        raw = tomllib.loads(config_path.read_text(encoding="utf-8"))
        cfg = deep_merge(cfg, raw)
        cfg_dir = config_path.resolve().parent

    # Resolve target_repo
    if target_repo is not None:
        repo_path = target_repo.resolve()
    else:
        raw_repo = cfg["project"].get("target_repo") or "."
        base = cfg_dir if cfg_dir is not None else Path.cwd()
        repo_path = resolve_path(base, raw_repo).resolve()

    cfg["project"]["target_repo"] = str(repo_path)
    cfg["_config_path"] = str(config_path.resolve()) if (config_path and config_path.is_file()) else ""
    cfg["_config_dir"] = str(cfg_dir) if cfg_dir else str(repo_path)

    if not cfg["project"].get("name"):
        cfg["project"]["name"] = repo_path.name or "bench-dashboard"
    if not cfg["project"].get("badge"):
        words = [w for w in cfg["project"]["name"].replace("-", " ").replace("_", " ").split() if w]
        cfg["project"]["badge"] = (
            (words[0][0] + words[1][0]).upper() if len(words) >= 2 else cfg["project"]["name"][:2].upper()
        )
    if not cfg["host"].get("label"):
        cfg["host"]["label"] = socket.gethostname()

    # Environment overrides
    if os.environ.get("BENCH_DASH_DOCKER_HOST"):
        cfg["host"]["docker_host"] = os.environ["BENCH_DASH_DOCKER_HOST"]
    elif not cfg["host"].get("docker_host") and os.environ.get("DOCKER_HOST"):
        cfg["host"]["docker_host"] = os.environ["DOCKER_HOST"]

    if os.environ.get("K8S_NAMESPACE"):
        cfg["k8s"]["namespace"] = os.environ["K8S_NAMESPACE"]
    if os.environ.get("K8S_USER_PREFIX") and not cfg["k8s"]["owner_prefixes"]:
        p = os.environ["K8S_USER_PREFIX"].strip("-")
        if p:
            cfg["k8s"]["owner_prefixes"] = [f"{p}-"]
    if os.environ.get("KUBECONFIG") and not cfg["k8s"].get("kubeconfig"):
        cfg["k8s"]["kubeconfig"] = os.environ["KUBECONFIG"]

    # Auto-detect disks if none configured
    if not cfg["host"]["disks"]:
        disks = [{"label": "Workspace", "path": str(repo_path), "critical": True}]
        jobs_p = repo_path / cfg["harbor"]["jobs_dir"]
        if jobs_p.exists():
            try:
                resolved_jobs_parent = jobs_p.resolve().parent
                if resolved_jobs_parent.stat().st_dev != repo_path.stat().st_dev:
                    disks.append({"label": "Jobs Storage", "path": str(resolved_jobs_parent), "critical": False})
            except OSError:
                pass
        if Path("/tmp").exists():
            disks.append({"label": "/tmp", "path": "/tmp", "critical": False})
        cfg["host"]["disks"] = disks

    # Resolve auto switches
    if no_k8s:
        cfg["k8s"]["enabled"] = False
    elif cfg["k8s"]["enabled"] == "auto":
        kc = Path(cfg["k8s"]["kubeconfig"] or (Path.home() / ".kube" / "config")).expanduser()
        cfg["k8s"]["enabled"] = bool(kc.is_file() or os.environ.get("K8S_API_SERVER"))

    if cfg["harbor"]["enabled"] == "auto":
        has_tasks = (repo_path / "tasks").is_dir() or resolve_path(repo_path, cfg["harbor"]["dataset_toml"]).is_file()
        has_jobs = resolve_path(repo_path, cfg["harbor"]["jobs_dir"]).is_dir()
        cfg["harbor"]["enabled"] = bool(has_tasks or has_jobs)

    # Auto-discover buckets if none specified
    if not cfg["harbor"]["buckets"]:
        buckets = []
        tasks_dir = repo_path / "tasks"
        if tasks_dir.is_dir():
            if any(tasks_dir.glob("*/*/task.toml")):
                buckets.append({"name": "main", "label": "Main", "glob": "tasks/*/*", "primary": True})
            elif any(tasks_dir.glob("*/task.toml")):
                buckets.append({"name": "main", "label": "Main", "glob": "tasks/*", "primary": True})
        for extra_name, extra_label in (("lite", "Lite"), ("candidates", "Candidates")):
            edir = repo_path / extra_name
            if edir.is_dir():
                if any(edir.glob("*/*/task.toml")):
                    buckets.append({"name": extra_name, "label": extra_label, "glob": f"{extra_name}/*/*", "primary": False})
                elif any(edir.glob("*/task.toml")):
                    buckets.append({"name": extra_name, "label": extra_label, "glob": f"{extra_name}/*", "primary": False})
        cfg["harbor"]["buckets"] = buckets

    return cfg
