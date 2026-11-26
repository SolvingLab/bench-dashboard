"""Generic read-only Harbor / Benchmark task inventory & evaluation trial collector.

Works out-of-the-box with Harbor-native repositories (Terminal-Bench, SWE-bench-style, OpenSciEnv):
- Auto-discovers tasks via `dataset.toml` and/or configured `buckets` (`task.toml`)
- Scans `jobs/` trial `result.json` + `verifier/result.json` with mtime caching
- Supports multiple models simultaneously (automatic leaderboard + primary model breakdown)
- Detects live `harbor run` processes and optional concurrency slot lock directories
"""
from __future__ import annotations

import collections
import concurrent.futures
import datetime as dt
import json
import os
import re
import statistics
import subprocess
import time
import tomllib
from pathlib import Path
from typing import Any

from ..utils import fmt_dt, fmt_epoch, get_tz, parse_iso, resolve_path

_TRIAL_CACHE: dict[str, tuple[float, dict[str, Any] | None]] = {}
_JOB_DIR_CACHE: dict[str, tuple[float, list[Path]]] = {}
_TASK_META_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
OK_STATES = {"ok", "ok-doc"}


def _load_registered(dataset_toml: Path) -> list[str]:
    if not dataset_toml.is_file():
        return []
    try:
        d = tomllib.loads(dataset_toml.read_text(encoding="utf-8"))
        return [str(t["path"]).rstrip("/") for t in d.get("tasks", []) if isinstance(t, dict) and t.get("path")]
    except Exception:
        return []


def _discover_tasks(repo: Path, hcfg: dict[str, Any], registered_set: set[str], tz: dt.tzinfo) -> list[dict[str, Any]]:
    tasks: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for bspec in hcfg.get("buckets") or []:
        bname = str(bspec.get("name") or "main")
        blabel = str(bspec.get("label") or bname)
        glob_pat = str(bspec.get("glob") or "tasks/*")
        is_primary = bool(bspec.get("primary", bname == "main"))
        root = resolve_path(repo, bspec.get("root") or ".")

        for toml_path in sorted(root.glob(f"{glob_pat.rstrip('/')}/task.toml")):
            td = toml_path.parent
            try:
                rel = td.relative_to(root).as_posix()
            except ValueError:
                rel = td.as_posix()
            key = (bname, rel)
            if key in seen:
                continue
            seen.add(key)

            try:
                mtime = toml_path.stat().st_mtime
            except OSError:
                mtime = 0.0

            ck = str(td)
            cached = _TASK_META_CACHE.get(ck)
            if cached is not None and cached[0] == mtime:
                cm = cached[1]
            else:
                cfg: dict[str, Any] = {}
                try:
                    cfg = tomllib.loads(toml_path.read_text(encoding="utf-8", errors="replace"))
                except Exception:
                    pass
                meta = cfg.get("metadata") or {}
                env = cfg.get("environment") or {}
                ver = cfg.get("verifier") or {}
                ver_env = ver.get("environment") or {}

                category = str(
                    meta.get("category")
                    or meta.get("field")
                    or meta.get("domain")
                    or (td.parent.name if td.parent != root / "tasks" and td.parent != root else "general")
                ).lower()
                tier = str(meta.get("compute_tier") or meta.get("difficulty") or ("gpu" if int(env.get("gpus") or 0) > 0 else "standard")).lower()
                dual_offline = (
                    str(env.get("network_mode") or "") == "no-network"
                    and str(ver.get("environment_mode") or "") == "separate"
                    and str(ver_env.get("network_mode") or "") == "no-network"
                )
                cm = {
                    "category": category,
                    "tier": tier,
                    "gpus": int(env.get("gpus") or 0),
                    "metric": str(meta.get("primary_metric") or meta.get("metric") or ""),
                    "dual_offline": dual_offline,
                    "tags": list(meta.get("tags") or []) if isinstance(meta.get("tags"), list) else [],
                }
                _TASK_META_CACHE[ck] = (mtime, cm)

            is_reg = (rel in registered_set) if registered_set else is_primary
            tasks.append({
                "task_path": rel,
                "slug": td.name,
                "bucket": bname,
                "bucket_label": blabel,
                "registered": is_reg,
                "category": cm["category"],
                "tier": cm["tier"],
                "gpus": cm["gpus"],
                "metric": cm["metric"],
                "dual_offline": cm["dual_offline"],
                "mtime": mtime,
                "mtime_str": fmt_epoch(mtime, tz),
                "nop": "none",
                "oracle": "none",
                "model": "none",
                "model_name": "",
                "model_score": None,
                "model_reward": None,
                "model_sec": None,
                "model_timeout": False,
            })
    return tasks


def _scan_harbor_ps(repo: Path, hcfg: dict[str, Any]) -> dict[str, Any]:
    live_by_kind: dict[str, set[str]] = collections.defaultdict(set)
    queued_by_kind: dict[str, set[str]] = collections.defaultdict(set)
    groups: collections.Counter[str] = collections.Counter()
    launcher_groups: set[str] = set()
    live_trials: list[dict[str, Any]] = []
    launchers = 0

    try:
        ps = subprocess.run(["ps", "-eo", "pid,etime,args"], capture_output=True, text=True, timeout=3).stdout
    except Exception:
        ps = ""

    for line in ps.splitlines()[1:]:
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        pid, etime, args = parts
        argv0 = args.split()[0]

        if "harbor_slots.py" in args and "python" in argv0:
            launchers += 1
            w = args.split()
            if "--group" in w and w.index("--group") + 1 < len(w):
                launcher_groups.add(w[w.index("--group") + 1])
            try:
                tf = resolve_path(repo, w[w.index("--tasks-file") + 1])
                bf = resolve_path(repo, w[w.index("--base-config") + 1])
                if tf.is_file() and bf.is_file():
                    bc = json.loads(bf.read_text(encoding="utf-8"))
                    ag = (bc.get("agents") or [{}])[0]
                    aname = ag.get("name") or ""
                    kind = aname if aname in ("nop", "oracle") else ("model" if ag.get("model_name") else "other")
                    queued_by_kind[kind].update(x.strip().rstrip("/") for x in tf.read_text().split() if x.strip())
            except Exception:
                pass
            continue

        if "harbor" in args and " run " in args and "flock " not in args:
            cfg_file = next((w for w in args.split() if w.endswith(".json") and os.path.isfile(w)), None)
            if cfg_file:
                try:
                    c = json.loads(Path(cfg_file).read_text(encoding="utf-8"))
                    ag = (c.get("agents") or [{}])[0]
                    aname = ag.get("name") or ""
                    mname = ag.get("model_name") or ""
                    kind = aname if aname in ("nop", "oracle") else ("model" if mname else "other")
                    grp = Path(cfg_file).parents[1].name if len(Path(cfg_file).parents) >= 2 else "harbor"
                    groups[grp] += 1
                    for t in c.get("tasks", []):
                        tp = str(t.get("path") or "").rstrip("/")
                        if tp:
                            live_by_kind[kind].add(tp)
                            live_trials.append({
                                "pid": pid,
                                "etime": etime,
                                "group": grp,
                                "kind": kind,
                                "model": mname or aname,
                                "task_path": tp,
                                "slug": Path(tp).name,
                            })
                    continue
                except Exception:
                    pass
            # Direct CLI `harbor run -p <task> -a <agent>`
            m_p = re.search(r"(?:-p|--path)\s+(\S+)", args)
            m_a = re.search(r"(?:-a|--agent)\s+(\S+)", args)
            m_m = re.search(r"(?:-m|--model)\s+(\S+)", args)
            if m_p:
                tp = m_p.group(1).rstrip("/")
                aname = m_a.group(1) if m_a else ""
                mname = m_m.group(1) if m_m else ""
                kind = aname if aname in ("nop", "oracle") else ("model" if mname or aname else "other")
                groups["cli"] += 1
                live_by_kind[kind].add(tp)
                live_trials.append({
                    "pid": pid,
                    "etime": etime,
                    "group": "cli",
                    "kind": kind,
                    "model": mname or aname or "agent",
                    "task_path": tp,
                    "slug": Path(tp).name,
                })

    slots_total = int(hcfg.get("slots_total") or 0)
    slots_used = 0
    lock_tpl = str(hcfg.get("slot_lock_dir") or "")
    if lock_tpl:
        lock_dir = Path(lock_tpl.replace("{uid}", str(os.getuid())))
        if lock_dir.is_dir():
            locked = set(re.findall(re.escape(lock_dir.name) + r"/([^\s]+\.lock)", ps))
            slots_used = min(len(locked), slots_total) if slots_total else len(locked)

    return {
        "live_by_kind": live_by_kind,
        "queued_by_kind": queued_by_kind,
        "groups": dict(groups.most_common()),
        "launcher_groups": launcher_groups,
        "running_count": len(live_trials),
        "live_trials": live_trials,
        "launchers_alive": launchers,
        "slots_total": slots_total,
        "slots_used": slots_used,
        "slots_free": max(0, slots_total - slots_used) if slots_total else 0,
    }


def _changed_since(repo: Path, rel_path: str, finished_iso: str, doc_only: tuple[str, ...], tz: dt.tzinfo) -> bool:
    d = parse_iso(finished_iso, tz)
    if not d:
        return True
    t = d.timestamp()
    root = repo / rel_path
    if not root.is_dir():
        return True
    for r, _, fs in os.walk(root):
        for f in fs:
            fp = os.path.join(r, f)
            rel = os.path.relpath(fp, root)
            if doc_only and rel.startswith(doc_only):
                continue
            try:
                if os.stat(fp).st_mtime > t:
                    return True
            except OSError:
                continue
    return False


def _parse_trial_file(
    rf: Path,
    scored_exceptions: set[str],
    score_keys: list[str],
    primary_keys: list[str],
    rank_keys: list[str],
    tz: dt.tzinfo,
) -> dict[str, Any] | None:
    key = str(rf)
    try:
        mt = rf.stat().st_mtime
    except OSError:
        return None
    cached = _TRIAL_CACHE.get(key)
    if cached is not None and cached[0] == mt:
        return cached[1]

    try:
        r = json.loads(rf.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        _TRIAL_CACHE[key] = (mt, None)
        return None
    if "task_id" not in r and "trial_name" not in r:
        _TRIAL_CACHE[key] = (mt, None)
        return None

    p = str(((r.get("task_id") or {}).get("path") if isinstance(r.get("task_id"), dict) else r.get("task_path")) or "").rstrip("/")
    if not p:
        _TRIAL_CACHE[key] = (mt, None)
        return None

    ai = r.get("agent_info") or {}
    aname = str(ai.get("name") or "")
    mname = str((ai.get("model_info") or {}).get("name") or ai.get("model_name") or "")
    if aname in ("nop", "oracle"):
        kind = aname
    elif mname or aname:
        kind = "model"
    else:
        _TRIAL_CACHE[key] = (mt, None)
        return None

    exc = r.get("exception_info")
    rw = ((r.get("verifier_result") or {}).get("rewards") or {}).get("reward")
    exc_type = (exc or {}).get("exception_type")
    err = None if (exc_type in scored_exceptions and rw is not None) else exc_type

    ae = r.get("agent_execution") or {}
    t0, t1 = parse_iso(ae.get("started_at"), tz), parse_iso(ae.get("finished_at"), tz)
    dur = (t1 - t0).total_seconds() if (t0 and t1) else None
    fin_dt = parse_iso(r.get("finished_at"), tz)

    score_val: float | None = None
    score_key_used = "reward"
    primary_val: float | None = None
    primary_name = ""
    rank_pct: float | None = None

    vfile = rf.parent / "verifier" / "result.json"
    if vfile.is_file():
        try:
            vr = json.loads(vfile.read_text(encoding="utf-8", errors="replace"))
            mets = vr.get("metrics") or vr
            if isinstance(mets, dict):
                for sk in score_keys:
                    if isinstance(mets.get(sk), (int, float)):
                        score_val = float(mets[sk])
                        score_key_used = sk
                        break
                primary_name = str(mets.get("primary") or mets.get("metric") or "")
                for pk in primary_keys:
                    if isinstance(mets.get(pk), (int, float)):
                        primary_val = float(mets[pk])
                        break
                for rk in rank_keys:
                    if isinstance(mets.get(rk), (int, float)):
                        rank_pct = float(mets[rk])
                        break
        except Exception:
            pass

    if score_val is None and isinstance(rw, (int, float)):
        score_val = float(rw)
        score_key_used = "reward"

    rec = {
        "path": p,
        "slug": Path(p).name,
        "kind": kind,
        "model_name": mname or aname,
        "checksum": r.get("task_checksum"),
        "error": err,
        "timeout": exc_type == "AgentTimeoutError",
        "reward": rw,
        "score": score_val,
        "score_key": score_key_used,
        "primary": primary_name,
        "primary_metric": primary_val,
        "rank_percentile": rank_pct,
        "finished_at": r.get("finished_at") or "",
        "finished_str": fmt_dt(fin_dt),
        "finished_epoch": fin_dt.timestamp() if fin_dt else mt,
        "agent_sec": round(dur, 1) if dur is not None else None,
        "job_group": rf.parents[2].name if len(rf.parents) >= 3 else "",
    }
    _TRIAL_CACHE[key] = (mt, rec)
    return rec


def collect_harbor(cfg: dict[str, Any]) -> dict[str, Any]:
    hcfg = cfg.get("harbor") or {}
    if not hcfg.get("enabled"):
        return {"enabled": False, "ok": False}

    tz = get_tz(cfg["project"].get("tz_offset_hours"))
    repo = Path(cfg["project"]["target_repo"])
    dataset_toml = resolve_path(repo, hcfg.get("dataset_toml") or "tasks/dataset.toml")
    jobs_dir = resolve_path(repo, hcfg.get("jobs_dir") or "jobs")
    checksum_cache = resolve_path(repo, hcfg.get("checksum_cache") or "cache/bench-status/checksums.json")
    doc_only = tuple(str(x) for x in (hcfg.get("doc_only_prefixes") or []))
    expect_rw = dict(hcfg.get("expected_rewards") or {"nop": 0, "oracle": 1})
    scored_exc = set(hcfg.get("scored_exceptions") or ["AgentTimeoutError"])
    score_keys = list(hcfg.get("score_metric_keys") or ["position_score", "score", "accuracy"])
    primary_keys = list(hcfg.get("primary_metric_keys") or ["primary_metric", "metric_value"])
    rank_keys = list(hcfg.get("rank_metric_keys") or ["rank_percentile"])

    registered_list = _load_registered(dataset_toml)
    registered_set = set(registered_list)
    tasks = _discover_tasks(repo, hcfg, registered_set, tz)

    cache_sums: dict[str, str | None] = {}
    if checksum_cache.is_file():
        try:
            raw = json.loads(checksum_cache.read_text(encoding="utf-8"))
            for p, obj in raw.items():
                if isinstance(obj, dict):
                    cache_sums[p.rstrip("/")] = obj.get("sum")
                elif isinstance(obj, str):
                    cache_sums[p.rstrip("/")] = obj
        except Exception:
            pass

    ps_info = _scan_harbor_ps(repo, hcfg)
    live_by_kind = ps_info["live_by_kind"]
    queued_by_kind = ps_info["queued_by_kind"]
    active_groups = set(ps_info.get("launcher_groups") or set()) | set((ps_info.get("groups") or {}).keys())

    trials_list: list[dict[str, Any]] = []
    by_pk: dict[tuple[str, str], list[dict[str, Any]]] = collections.defaultdict(list)
    by_pkm: dict[tuple[str, str, str], list[dict[str, Any]]] = collections.defaultdict(list)

    if jobs_dir.is_dir():
        result_files: list[Path] = []
        now_scan = time.time()
        top_entries: list[os.DirEntry[str]] = []
        try:
            with os.scandir(jobs_dir) as it1:
                top_entries = [e1 for e1 in it1 if e1.is_dir() and not e1.name.startswith(".")]
        except OSError:
            top_entries = []

        def _scan_one_group(e1: os.DirEntry[str]) -> list[Path]:
            try:
                mt1 = e1.stat().st_mtime
                llog = Path(e1.path) / "launcher.log"
                if llog.is_file():
                    mt1 = max(mt1, llog.stat().st_mtime)
            except OSError:
                return []
            is_live_grp = e1.name in active_groups
            cached_rfs = _JOB_DIR_CACHE.get(e1.path)
            if (not is_live_grp) and cached_rfs is not None and cached_rfs[0] == mt1 and (now_scan - mt1 > 600):
                return cached_rfs[1]
            found: list[Path] = []
            r1 = Path(e1.path) / "result.json"
            if r1.is_file():
                found.append(r1)
            try:
                with os.scandir(e1.path) as it2:
                    for e2 in it2:
                        if not e2.is_dir() or e2.name in ("_dup_cancelled", "_configs", "verifier", "agent"):
                            continue
                        r2 = Path(e2.path) / "result.json"
                        if r2.is_file():
                            found.append(r2)
                        try:
                            with os.scandir(e2.path) as it3:
                                for e3 in it3:
                                    if e3.is_dir() and e3.name not in ("_dup_cancelled", "verifier", "agent"):
                                        r3 = Path(e3.path) / "result.json"
                                        if r3.is_file():
                                            found.append(r3)
                        except OSError:
                            pass
            except OSError:
                pass
            if not is_live_grp:
                _JOB_DIR_CACHE[e1.path] = (mt1, found)
            return found

        if top_entries:
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as ex:
                for grp_files in ex.map(_scan_one_group, top_entries):
                    result_files.extend(grp_files)
                for rec in ex.map(
                    lambda rf: _parse_trial_file(rf, scored_exc, score_keys, primary_keys, rank_keys, tz),
                    result_files,
                ):
                    if rec is None:
                        continue
                    trials_list.append(rec)
                    by_pk[(rec["path"], rec["kind"])].append(rec)
                    if rec["kind"] == "model":
                        by_pkm[(rec["path"], "model", rec["model_name"])].append(rec)

    trials_list.sort(key=lambda x: x["finished_epoch"], reverse=True)
    for lst in by_pk.values():
        lst.sort(key=lambda x: x["finished_epoch"], reverse=True)
    for lst in by_pkm.values():
        lst.sort(key=lambda x: x["finished_epoch"], reverse=True)

    # Discover all models & choose primary model
    model_trial_counts = collections.Counter(
        t["model_name"] for t in trials_list if t["kind"] == "model" and not t["error"] and t["score"] is not None
    )
    pref_model = str(hcfg.get("primary_model") or "").strip()
    if not pref_model and model_trial_counts:
        pref_model = model_trial_counts.most_common(1)[0][0]

    reg_tasks = [t for t in tasks if t["registered"]]
    reg_paths = {t["task_path"] for t in reg_tasks}
    state_counts = {k: collections.Counter() for k in ("nop", "oracle", "model")}
    selfcheck_both_ok = 0
    fully_done = 0
    scored_rows: list[dict[str, Any]] = []

    def _eval_latest(ts: list[dict[str, Any]], p: str, k: str, cur_sum: str | None) -> tuple[str, dict[str, Any] | None]:
        cur = [x for x in ts if (cur_sum is None or (x["checksum"] and x["checksum"] == cur_sum))]
        good = [x for x in cur if not x["error"] and x["reward"] is not None]
        doc_only_hit = False
        if not good and cur_sum is not None:
            old = [x for x in ts if not x["error"] and x["reward"] is not None and x["finished_at"]]
            if old and not _changed_since(repo, p, old[0]["finished_at"], doc_only, tz):
                good, doc_only_hit = old[:1], True

        if p in live_by_kind.get(k, set()):
            return "running", good[0] if good else None
        if good and doc_only_hit and not (k in expect_rw and good[0]["reward"] != expect_rw[k]):
            return "ok-doc", good[0]
        if good:
            if k in expect_rw and good[0]["reward"] != expect_rw[k]:
                return "wrong", good[0]
            return "ok", good[0]
        if cur:
            return "failed", None
        if ts:
            return "stale", None
        return "none", None

    for t in reg_tasks:
        p = t["task_path"]
        cur_sum = cache_sums.get(p)
        for k in ("nop", "oracle", "model"):
            ts = by_pk.get((p, k), [])
            if k == "model" and pref_model:
                ts_pref = [x for x in ts if x["model_name"] == pref_model or x["model_name"].endswith("/" + pref_model)] or ts
            else:
                ts_pref = ts
            s, g = _eval_latest(ts_pref, p, k, cur_sum)
            if s in ("failed", "stale", "none") and p in queued_by_kind.get(k, set()):
                s = "queued"
            t[k] = s
            state_counts[k][s] += 1

            if k == "model" and g is not None:
                t["model_name"] = g["model_name"]
                t["model_score"] = round(g["score"], 3) if g["score"] is not None else None
                t["model_reward"] = g["reward"]
                t["model_sec"] = g["agent_sec"]
                t["model_timeout"] = g["timeout"]
                if s in OK_STATES and g["score"] is not None:
                    scored_rows.append({
                        "task_path": p,
                        "slug": t["slug"],
                        "category": t["category"],
                        "tier": t["tier"],
                        "model_name": g["model_name"],
                        "score": round(g["score"], 3),
                        "score_key": g["score_key"],
                        "reward": g["reward"],
                        "primary": g["primary"] or t["metric"],
                        "primary_metric": round(g["primary_metric"], 4) if g["primary_metric"] is not None else None,
                        "rank_percentile": round(g["rank_percentile"], 1) if g["rank_percentile"] is not None else None,
                        "agent_min": round(g["agent_sec"] / 60.0, 1) if g["agent_sec"] else None,
                        "timeout": g["timeout"],
                        "finished_str": g["finished_str"],
                    })

        if t["nop"] in OK_STATES and t["oracle"] in OK_STATES:
            selfcheck_both_ok += 1
            if t["model"] in OK_STATES:
                fully_done += 1

    # Multi-model leaderboard across all discovered models
    all_models = sorted({t["model_name"] for t in trials_list if t["kind"] == "model" and t["model_name"]})
    leaderboard: list[dict[str, Any]] = []
    for mname in all_models:
        m_scores: list[float] = []
        m_mins: list[float] = []
        m_pass = m_err = m_to = 0
        paths_for_model = sorted({p for (p, k, m) in by_pkm.keys() if m == mname and (not reg_paths or p in reg_paths)})
        for p in paths_for_model:
            rec = by_pkm[(p, "model", mname)][0]
            if rec["error"]:
                m_err += 1
                continue
            if rec["timeout"]:
                m_to += 1
            if rec["score"] is not None:
                m_scores.append(rec["score"])
            if isinstance(rec["reward"], (int, float)) and rec["reward"] >= 1:
                m_pass += 1
            if rec["agent_sec"] is not None:
                m_mins.append(rec["agent_sec"] / 60.0)
        n_ok = len(m_scores)
        leaderboard.append({
            "model": mname,
            "scored": n_ok,
            "errors": m_err,
            "timeouts": m_to,
            "pass_count": m_pass,
            "pass_rate_pct": round(m_pass / n_ok * 100.0, 1) if n_ok else 0.0,
            "mean_score": round(statistics.mean(m_scores), 3) if m_scores else None,
            "median_score": round(statistics.median(m_scores), 3) if m_scores else None,
            "median_min": round(statistics.median(m_mins), 1) if m_mins else None,
        })
    leaderboard.sort(key=lambda x: (x["scored"], x["mean_score"] or -1.0), reverse=True)

    # Score distribution buckets (auto-detect 0..1 unit scale vs 0..100 scale)
    vals = [r["score"] for r in scored_rows]
    scale = str(hcfg.get("score_scale") or "auto")
    if scale == "auto":
        scale = "unit" if (vals and max(vals) <= 1.0001 and min(vals) >= 0.0) else "percent"
    if scale == "unit":
        buckets = collections.OrderedDict([("0", 0), ("(0, 0.5)", 0), ("[0.5, 0.8)", 0), ("[0.8, 1.0)", 0), ("1.0", 0)])
        for v in vals:
            if v <= 0:
                buckets["0"] += 1
            elif v < 0.5:
                buckets["(0, 0.5)"] += 1
            elif v < 0.8:
                buckets["[0.5, 0.8)"] += 1
            elif v < 1.0:
                buckets["[0.8, 1.0)"] += 1
            else:
                buckets["1.0"] += 1
    else:
        buckets = collections.OrderedDict([("0", 0), ("1-25", 0), ("25-50", 0), ("50-75", 0), ("75-100", 0), (">100", 0)])
        for v in vals:
            if v <= 0:
                buckets["0"] += 1
            elif v < 25:
                buckets["1-25"] += 1
            elif v < 50:
                buckets["25-50"] += 1
            elif v < 75:
                buckets["50-75"] += 1
            elif v <= 100:
                buckets["75-100"] += 1
            else:
                buckets[">100"] += 1

    # Throughput velocity (1h / 6h / 24h) + 24h hourly series
    now_ts = time.time()
    now_dt = dt.datetime.fromtimestamp(now_ts, tz=tz)
    now_h = now_dt.replace(minute=0, second=0, microsecond=0)

    def _win(hours: float) -> dict[str, Any]:
        cut = now_ts - hours * 3600.0
        sub = [tr for tr in trials_list if (tr.get("finished_epoch") or 0.0) >= cut]
        return {
            "count": len(sub),
            "rate_ph": round(len(sub) / hours, 2),
            "rate_pd": round(len(sub) / hours * 24.0, 1),
            "by_kind": dict(collections.Counter(tr["kind"] for tr in sub)),
        }

    w1, w6, w24 = _win(1.0), _win(6.0), _win(24.0)
    rate_ph = w6["rate_ph"] if w6["count"] >= 2 else w24["rate_ph"]

    h24_keys = [(now_h - dt.timedelta(hours=i)).strftime("%Y-%m-%d %H") for i in range(23, -1, -1)]
    h24_labels = [(now_h - dt.timedelta(hours=i)).strftime("%m-%d %H:00") for i in range(23, -1, -1)]
    h24_map = {k: {"nop": 0, "oracle": 0, "model": 0, "err": 0} for k in h24_keys}
    for tr in trials_list:
        fe = tr.get("finished_epoch") or 0.0
        if fe < now_ts - 90000:
            continue
        hk = dt.datetime.fromtimestamp(fe, tz=tz).strftime("%Y-%m-%d %H")
        if hk in h24_map:
            if tr.get("error"):
                h24_map[hk]["err"] += 1
            elif tr["kind"] in h24_map[hk]:
                h24_map[hk][tr["kind"]] += 1

    # Category x Tier matrix
    categories = sorted({t["category"] for t in reg_tasks})
    tiers = sorted({t["tier"] for t in reg_tasks})
    matrix: dict[str, dict[str, int]] = {c: {tr: 0 for tr in tiers + ["total"]} for c in categories}
    for t in reg_tasks:
        matrix[t["category"]][t["tier"]] += 1
        matrix[t["category"]]["total"] += 1

    run_by_kind = collections.Counter(tr.get("kind") or "other" for tr in ps_info["live_trials"])
    cur_mins = [r["agent_min"] for r in scored_rows if r["agent_min"] is not None]

    return {
        "enabled": True,
        "ok": True,
        "primary_model": pref_model,
        "score_scale": scale,
        "counts": {k: dict(v) for k, v in state_counts.items()},
        "registered_total": len(reg_tasks),
        "grand_total_tasks": len(tasks),
        "buckets_summary": dict(collections.Counter(t["bucket"] for t in tasks)),
        "selfcheck_both_ok": selfcheck_both_ok,
        "fully_done": fully_done,
        "dual_offline_count": sum(1 for t in reg_tasks if t["dual_offline"]),
        "categories": categories,
        "tiers": tiers,
        "category_tier_matrix": matrix,
        "tasks": tasks,
        "running": {
            "count": ps_info["running_count"],
            "selfcheck_count": run_by_kind.get("nop", 0) + run_by_kind.get("oracle", 0),
            "model_count": run_by_kind.get("model", 0),
            "by_kind": {"nop": run_by_kind.get("nop", 0), "oracle": run_by_kind.get("oracle", 0), "model": run_by_kind.get("model", 0)},
            "groups": ps_info["groups"],
            "launchers_alive": ps_info["launchers_alive"],
            "slots_total": ps_info["slots_total"],
            "slots_free": ps_info["slots_free"],
            "slots_used": ps_info["slots_used"],
            "live_trials": ps_info["live_trials"][:80],
        },
        "velocity": {
            "count_1h": w1["count"],
            "count_6h": w6["count"],
            "count_24h": w24["count"],
            "rate_ph": rate_ph,
            "rate_pd": round(rate_ph * 24.0, 1),
            "by_kind_24h": w24["by_kind"],
            "pending_selfcheck": max(0, len(reg_tasks) - selfcheck_both_ok),
        },
        "hourly_24h": {
            "labels": h24_labels,
            "series": [h24_map[k] for k in h24_keys],
        },
        "leaderboard": leaderboard,
        "primary_eval": {
            "model": pref_model,
            "n_scored": len(scored_rows),
            "median_score": round(statistics.median(vals), 3) if vals else None,
            "mean_score": round(statistics.mean(vals), 3) if vals else None,
            "median_min": round(statistics.median(cur_mins), 1) if cur_mins else None,
            "buckets": dict(buckets),
            "rows": sorted(scored_rows, key=lambda x: x["score"], reverse=True),
        },
        "all_trials_total": len(trials_list),
        "recent_trials": trials_list[:40],
    }
