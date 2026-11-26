"""Read-only local host telemetry collector: CPU, RAM, Swap (+Top Swap), /dev/shm, Disks, NVIDIA GPUs, Docker."""
from __future__ import annotations

import collections
import concurrent.futures
import os
import shutil
import subprocess
import time
from pathlib import Path
from typing import Any

from ..utils import resolve_path

_CPU_PREV: tuple[float, tuple[int, ...]] | None = None


def _read_proc_stat_cpu() -> tuple[int, ...] | None:
    try:
        first = Path("/proc/stat").read_text(encoding="utf-8").splitlines()[0]
        parts = first.split()
        if parts and parts[0] == "cpu":
            return tuple(int(x) for x in parts[1:11])
    except Exception:
        pass
    return None


def sample_local_cpu() -> dict[str, Any]:
    global _CPU_PREV
    now = time.monotonic()
    cur = _read_proc_stat_cpu()
    prev = _CPU_PREV
    if cur is not None and (prev is None or now - prev[0] < 0.05):
        time.sleep(0.15)
        cur2 = _read_proc_stat_cpu()
        if cur2 is not None:
            prev = (now, cur)
            cur = cur2
            now = time.monotonic()
    if cur is not None:
        _CPU_PREV = (now, cur)

    busy_pct = user_pct = sys_pct = iowait_pct = 0.0
    if cur is not None and prev is not None and len(cur) >= 5 and len(prev[1]) >= 5:
        diffs = [max(0, c - p) for c, p in zip(cur, prev[1])]
        tot = sum(diffs)
        if tot > 0:
            user = diffs[0] + diffs[1]
            syst = diffs[2] + (diffs[5] if len(diffs) > 5 else 0) + (diffs[6] if len(diffs) > 6 else 0)
            idle = diffs[3]
            iowait = diffs[4]
            busy = max(0, tot - idle - iowait)
            busy_pct = round(busy / tot * 100.0, 1)
            user_pct = round(user / tot * 100.0, 1)
            sys_pct = round(syst / tot * 100.0, 1)
            iowait_pct = round(iowait / tot * 100.0, 1)

    load1, load5, load15 = (0.0, 0.0, 0.0)
    try:
        load1, load5, load15 = os.getloadavg()
    except OSError:
        pass
    ncpu = os.cpu_count() or 1
    return {
        "busy_pct": busy_pct,
        "user_pct": user_pct,
        "sys_pct": sys_pct,
        "iowait_pct": iowait_pct,
        "idle_pct": round(max(0.0, 100.0 - busy_pct - iowait_pct), 1),
        "ncpu": ncpu,
        "load": [round(load1, 2), round(load5, 2), round(load15, 2)],
        "load_pct": round(load1 / ncpu * 100.0, 1),
    }


def collect_local_gpus() -> dict[str, Any]:
    if shutil.which("nvidia-smi") is None:
        return {"ok": False, "count": 0, "cards": [], "error": "nvidia-smi not found"}
    try:
        g_out = subprocess.run(
            [
                "nvidia-smi",
                "--query-gpu=index,uuid,name,utilization.gpu,utilization.memory,memory.used,memory.total,temperature.gpu,power.draw,power.limit",
                "--format=csv,noheader,nounits",
            ],
            capture_output=True,
            text=True,
            timeout=4,
        )
        if g_out.returncode != 0 or not g_out.stdout.strip():
            return {"ok": False, "count": 0, "cards": [], "error": (g_out.stderr or "nvidia-smi unavailable")[:120]}

        procs_by_uuid: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        p_out = subprocess.run(
            ["nvidia-smi", "--query-compute-apps=gpu_uuid,pid,process_name,used_memory", "--format=csv,noheader,nounits"],
            capture_output=True,
            text=True,
            timeout=4,
        )
        if p_out.returncode == 0:
            for ln in p_out.stdout.splitlines():
                parts = [x.strip() for x in ln.split(",")]
                if len(parts) >= 4:
                    uuid, pid, pname, mem_mb = parts[0], parts[1], parts[2], parts[3]
                    try:
                        mgb = round(float(mem_mb) / 1024.0, 1)
                    except ValueError:
                        mgb = 0.0
                    short_p = pname.split("/")[-1]
                    if "vllm" in pname.lower():
                        short_p = "vLLM"
                    procs_by_uuid[uuid].append({"pid": pid, "name": short_p, "mem_gb": mgb})

        cards: list[dict[str, Any]] = []
        for ln in g_out.stdout.splitlines():
            parts = [x.strip() for x in ln.split(",")]
            if len(parts) < 10:
                continue
            idx = int(parts[0])
            uuid = parts[1]
            name = parts[2]
            u_gpu = float(parts[3]) if parts[3] not in ("", "[N/A]") else 0.0
            u_mem = float(parts[4]) if parts[4] not in ("", "[N/A]") else 0.0
            m_used_gb = round(float(parts[5]) / 1024.0, 1) if parts[5] not in ("", "[N/A]") else 0.0
            m_tot_gb = round(float(parts[6]) / 1024.0, 1) if parts[6] not in ("", "[N/A]") else 0.0
            temp_c = int(float(parts[7])) if parts[7] not in ("", "[N/A]") else 0
            pwr_w = round(float(parts[8]), 1) if parts[8] not in ("", "[N/A]") else 0.0
            pwr_lim = round(float(parts[9]), 0) if parts[9] not in ("", "[N/A]") else 0.0
            m_pct = round(m_used_gb / m_tot_gb * 100.0, 1) if m_tot_gb > 0 else 0.0
            procs = procs_by_uuid.get(uuid, [])
            if m_used_gb < 1.0 and u_gpu < 10.0:
                status = "free"
            elif any(pr["name"] == "vLLM" for pr in procs):
                status = "vllm"
            elif u_gpu >= 25.0:
                status = "active"
            else:
                status = "occupied"

            short_name = name.replace("NVIDIA ", "").replace(" PCIe", "").replace("80GB", "80G")
            cards.append({
                "index": idx,
                "name": name,
                "short_name": short_name,
                "util_gpu": round(u_gpu, 1),
                "util_mem": round(u_mem, 1),
                "mem_used_gb": m_used_gb,
                "mem_total_gb": m_tot_gb,
                "mem_pct": m_pct,
                "temp_c": temp_c,
                "power_w": pwr_w,
                "power_limit_w": pwr_lim,
                "status": status,
                "procs": procs[:4],
            })

        n = len(cards)
        tot_mem = sum(c["mem_total_gb"] for c in cards)
        used_mem = sum(c["mem_used_gb"] for c in cards)
        free_cards = sum(1 for c in cards if c["status"] == "free")
        return {
            "ok": True,
            "count": n,
            "free_cards": free_cards,
            "busy_cards": n - free_cards,
            "util_avg_pct": round(sum(c["util_gpu"] for c in cards) / n, 1) if n else 0.0,
            "mem_used_gb": round(used_mem, 1),
            "mem_total_gb": round(tot_mem, 1),
            "mem_used_pct": round(used_mem / tot_mem * 100.0, 1) if tot_mem else 0.0,
            "power_draw_w": round(sum(c["power_w"] for c in cards), 0),
            "power_limit_w": round(sum(c["power_limit_w"] for c in cards), 0),
            "temp_max_c": max((c["temp_c"] for c in cards), default=0),
            "cards": cards,
        }
    except Exception as e:
        return {"ok": False, "count": 0, "cards": [], "error": str(e)[:120]}


def collect_local_docker(docker_host: str, bench_keywords: list[str]) -> dict[str, Any]:
    if shutil.which("docker") is None:
        return {"ok": False, "total": 0, "bench": 0, "host": docker_host or "default"}
    try:
        env = dict(os.environ)
        if docker_host:
            env["DOCKER_HOST"] = docker_host
        cp = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=3,
            env=env,
        )
        if cp.returncode != 0:
            return {"ok": False, "total": 0, "bench": 0, "host": docker_host or "default"}
        names = [x.strip() for x in cp.stdout.splitlines() if x.strip()]
        kws = [k.lower() for k in bench_keywords]
        bench_n = sum(1 for n in names if any(k in n.lower() for k in kws))
        return {"ok": True, "total": len(names), "bench": bench_n, "host": docker_host or "default"}
    except Exception:
        return {"ok": False, "total": 0, "bench": 0, "host": docker_host or "default"}


def _disk_stat(label: str, path: Path, critical: bool = False) -> dict[str, Any]:
    try:
        u = shutil.disk_usage(path)
        return {
            "label": label,
            "path": str(path),
            "critical": critical,
            "ok": True,
            "total_gb": round(u.total / (1024 ** 3), 1),
            "used_gb": round(u.used / (1024 ** 3), 1),
            "free_gb": round(u.free / (1024 ** 3), 1),
            "use_pct": round(u.used / u.total * 100.0, 1) if u.total else 0.0,
        }
    except OSError:
        return {
            "label": label,
            "path": str(path),
            "critical": critical,
            "ok": False,
            "total_gb": 0.0,
            "used_gb": 0.0,
            "free_gb": 0.0,
            "use_pct": 0.0,
        }


def collect_host(cfg: dict[str, Any]) -> dict[str, Any]:
    hcfg = cfg.get("host") or {}
    if not hcfg.get("enabled", True):
        return {"enabled": False, "ok": False}

    repo = Path(cfg["project"]["target_repo"])
    docker_host = str(hcfg.get("docker_host") or "")
    bench_kws = list(hcfg.get("docker_bench_keywords") or ["harbor", "__env", "__verifier"])

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
        f_cpu = ex.submit(sample_local_cpu)
        f_gpu = ex.submit(collect_local_gpus)
        f_doc = ex.submit(collect_local_docker, docker_host, bench_kws)
        cpu = f_cpu.result()
        gpus = f_gpu.result()
        docker = f_doc.result()

    disks: list[dict[str, Any]] = []
    for dspec in hcfg.get("disks") or []:
        if not isinstance(dspec, dict) or not dspec.get("path"):
            continue
        p = resolve_path(repo, dspec["path"])
        disks.append(_disk_stat(str(dspec.get("label") or p.name or str(p)), p, bool(dspec.get("critical", False))))

    shm = _disk_stat("/dev/shm", Path("/dev/shm"), critical=False) if Path("/dev/shm").exists() else {
        "label": "/dev/shm", "path": "/dev/shm", "ok": False, "total_gb": 0.0, "used_gb": 0.0, "free_gb": 0.0, "use_pct": 0.0
    }

    mem_total_gb = mem_avail_gb = swap_total_gb = swap_free_gb = shmem_gb = 0.0
    try:
        m: dict[str, int] = {}
        for ln in Path("/proc/meminfo").read_text(encoding="utf-8").splitlines():
            p = ln.split()
            if len(p) >= 2 and p[1].isdigit():
                m[p[0].rstrip(":")] = int(p[1])
        mem_total_gb = round(m.get("MemTotal", 0) / (1024 ** 2), 1)
        mem_avail_gb = round(m.get("MemAvailable", 0) / (1024 ** 2), 1)
        swap_total_gb = round(m.get("SwapTotal", 0) / (1024 ** 2), 1)
        swap_free_gb = round(m.get("SwapFree", 0) / (1024 ** 2), 1)
        shmem_gb = round(m.get("Shmem", 0) / (1024 ** 2), 1)
    except Exception:
        pass

    mem_used_gb = round(max(0.0, mem_total_gb - mem_avail_gb), 1)
    mem_used_pct = round(mem_used_gb / mem_total_gb * 100.0, 1) if mem_total_gb else 0.0
    swap_used_gb = round(max(0.0, swap_total_gb - swap_free_gb), 1)
    swap_used_pct = round(swap_used_gb / swap_total_gb * 100.0, 1) if swap_total_gb else 0.0

    top_swap: list[dict[str, Any]] = []
    if swap_used_pct >= 25.0 and Path("/proc").is_dir():
        try:
            import pwd
            rows: list[tuple[int, str, str, str]] = []
            for st_p in Path("/proc").glob("[0-9]*/status"):
                try:
                    txt = st_p.read_text(encoding="utf-8", errors="ignore")
                except OSError:
                    continue
                name = uid_s = ""
                sw_kb = 0
                for ln in txt.splitlines():
                    if ln.startswith("Name:"):
                        name = ln.split(":", 1)[1].strip()
                    elif ln.startswith("Uid:"):
                        uid_s = ln.split()[1]
                    elif ln.startswith("VmSwap:"):
                        sw_kb = int(ln.split()[1])
                if sw_kb >= 102400:  # >= 100 MiB
                    try:
                        uname = pwd.getpwuid(int(uid_s)).pw_name if uid_s.isdigit() else uid_s
                    except KeyError:
                        uname = uid_s
                    rows.append((sw_kb, uname, st_p.parent.name, name))
            rows.sort(reverse=True)
            for sw_kb, uname, pid_s, name in rows[:5]:
                top_swap.append({
                    "user": uname,
                    "pid": int(pid_s),
                    "cmd": name,
                    "swap_gb": round(sw_kb / (1024 ** 2), 2),
                })
        except Exception:
            pass

    return {
        "enabled": True,
        "ok": True,
        "label": str(hcfg.get("label") or "localhost"),
        "cpu": cpu,
        "gpus": gpus,
        "docker": docker,
        "disks": disks,
        "shm": shm,
        "mem_total_gb": mem_total_gb,
        "mem_used_gb": mem_used_gb,
        "mem_avail_gb": mem_avail_gb,
        "mem_used_pct": mem_used_pct,
        "shmem_gb": shmem_gb,
        "swap_total_gb": swap_total_gb,
        "swap_used_gb": swap_used_gb,
        "swap_free_gb": swap_free_gb,
        "swap_used_pct": swap_used_pct,
        "top_swap": top_swap,
    }
