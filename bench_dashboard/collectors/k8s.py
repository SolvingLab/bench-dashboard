"""Zero-dependency read-only Kubernetes cluster telemetry collector.

Supports:
- Standard `nvidia.com/gpu` (and `amd.com/gpu`) as well as TKE fractional vCUDA (`tencent.com/vcuda-core` / `vcuda-memory`)
- Auth via `kubectl config view --minify --raw -o json` (zero PyYAML needed!), env K8S_API_SERVER + K8S_TOKEN, or fallback PyYAML if installed
- Namespace ResourceQuota, cluster Nodes, live pod CPU/RAM (`metrics.k8s.io`), Jobs/Pods, optional mounted shared storage, and health event checks
"""
from __future__ import annotations

import base64
import collections
import concurrent.futures
import ipaddress
import json
import os
import shutil
import ssl
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

from ..utils import fmt_dt, get_tz, parse_cpu, parse_iso, parse_mem_gib

_HEALTH_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
_KUBE_AUTH_CACHE: dict[str, Any] = {"key": "", "mtime": 0.0, "auth": None}


def _clean_env(strip_proxy: bool) -> dict[str, str]:
    env = dict(os.environ)
    if strip_proxy:
        for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY", "all_proxy"):
            env.pop(k, None)
    return env


def _is_private_host(url: str) -> bool:
    try:
        host = urllib.parse.urlparse(url).hostname or ""
        if host in ("localhost", "127.0.0.1", "::1"):
            return True
        ip = ipaddress.ip_address(host)
        return ip.is_private or ip.is_loopback or ip.is_link_local
    except ValueError:
        return False


class _KubeClient:
    def __init__(self, kubeconfig: str, strip_proxy: bool | str = "auto"):
        self.strip_proxy_mode = strip_proxy
        self.strip_proxy = bool(strip_proxy is True)
        self.server = ""
        self.token = ""
        self.namespace_hint = ""
        self.ssl_ctx: ssl.SSLContext | None = None
        self._tmp_files: list[Path] = []
        self._load(kubeconfig)
        if self.strip_proxy_mode == "auto":
            self.strip_proxy = _is_private_host(self.server)

    def _write_b64_tmp(self, b64_str: str, suffix: str) -> str:
        raw = base64.b64decode(b64_str)
        fd, path = tempfile.mkstemp(prefix="bd-k8s-", suffix=suffix)
        with os.fdopen(fd, "wb") as f:
            f.write(raw)
        os.chmod(path, 0o600)
        self._tmp_files.append(Path(path))
        return path

    def _load(self, kubeconfig: str) -> None:
        # 1. Explicit env token override
        if os.environ.get("K8S_API_SERVER") and os.environ.get("K8S_TOKEN"):
            self.server = os.environ["K8S_API_SERVER"].rstrip("/")
            self.token = os.environ["K8S_TOKEN"].strip()
            ctx = ssl.create_default_context()
            if os.environ.get("K8S_INSECURE_TLS", "").lower() in ("1", "true", "yes"):
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            self.ssl_ctx = ctx
            return

        kc_path = Path(kubeconfig or os.environ.get("KUBECONFIG") or (Path.home() / ".kube" / "config")).expanduser()
        if not kc_path.is_file():
            raise RuntimeError(f"kubeconfig not found at {kc_path}")

        cfg: dict[str, Any] | None = None
        # 2. Prefer `kubectl config view --minify --raw -o json` (zero PyYAML needed, handles merged configs)
        if shutil.which("kubectl"):
            env = _clean_env(self.strip_proxy)
            env["KUBECONFIG"] = str(kc_path)
            cp = subprocess.run(
                ["kubectl", "config", "view", "--minify", "--raw", "-o", "json"],
                capture_output=True,
                text=True,
                timeout=5,
                env=env,
            )
            if cp.returncode == 0 and cp.stdout.strip():
                try:
                    cfg = json.loads(cp.stdout)
                except json.JSONDecodeError:
                    cfg = None

        # 3. Fallback to PyYAML if installed
        if cfg is None:
            try:
                import yaml  # type: ignore
                cfg = yaml.safe_load(kc_path.read_text(encoding="utf-8")) or {}
            except Exception as e:
                raise RuntimeError(f"Unable to parse kubeconfig (install kubectl or PyYAML): {e}") from e

        cur = cfg.get("current-context")
        contexts = cfg.get("contexts") or []
        ctx_entry = next((c for c in contexts if c.get("name") == cur), None) or (contexts[0] if contexts else None)
        if not ctx_entry:
            raise RuntimeError("No context found in kubeconfig")
        ctx_obj = ctx_entry.get("context") or {}
        self.namespace_hint = str(ctx_obj.get("namespace") or "")

        cl_entry = next((c for c in (cfg.get("clusters") or []) if c.get("name") == ctx_obj.get("cluster")), None)
        us_entry = next((u for u in (cfg.get("users") or []) if u.get("name") == ctx_obj.get("user")), None)
        if not cl_entry:
            raise RuntimeError("Cluster definition missing in kubeconfig")
        cl = cl_entry.get("cluster") or {}
        us = (us_entry or {}).get("user") or {}

        self.server = str(cl.get("server") or "").rstrip("/")
        if not self.server:
            raise RuntimeError("Cluster server URL missing in kubeconfig")
        self.token = str(us.get("token") or "").strip()

        ssl_ctx = ssl.create_default_context()
        if cl.get("insecure-skip-tls-verify"):
            ssl_ctx.check_hostname = False
            ssl_ctx.verify_mode = ssl.CERT_NONE
        elif cl.get("certificate-authority-data"):
            ca_pem = base64.b64decode(cl["certificate-authority-data"]).decode("utf-8", "replace")
            ssl_ctx.load_verify_locations(cadata=ca_pem)
        elif cl.get("certificate-authority"):
            ssl_ctx.load_verify_locations(cafile=cl["certificate-authority"])

        cert_file = us.get("client-certificate")
        key_file = us.get("client-key")
        if not cert_file and us.get("client-certificate-data") and us.get("client-key-data"):
            cert_file = self._write_b64_tmp(us["client-certificate-data"], ".crt")
            key_file = self._write_b64_tmp(us["client-key-data"], ".key")
        if cert_file and key_file:
            ssl_ctx.load_cert_chain(certfile=cert_file, keyfile=key_file)
        self.ssl_ctx = ssl_ctx

    def get(self, endpoint: str, timeout: int = 8) -> dict[str, Any]:
        url = self.server + endpoint
        headers = {"Accept": "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(url, headers=headers)
        handlers: list[Any] = [urllib.request.HTTPSHandler(context=self.ssl_ctx)]
        if self.strip_proxy:
            handlers.insert(0, urllib.request.ProxyHandler({}))
        opener = urllib.request.build_opener(*handlers)
        with opener.open(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))

    def close(self) -> None:
        for p in self._tmp_files:
            try:
                p.unlink(missing_ok=True)
            except OSError:
                pass
        self._tmp_files.clear()


def _detect_gpu_mode(nodes_raw: list[dict[str, Any]], pref: str) -> str:
    if pref in ("nvidia.com/gpu", "amd.com/gpu", "tencent.com/vcuda-core"):
        return pref
    for n in nodes_raw:
        alloc = (n.get("status") or {}).get("allocatable") or {}
        if float(alloc.get("tencent.com/vcuda-core") or 0) > 0:
            return "tencent.com/vcuda-core"
        if float(alloc.get("nvidia.com/gpu") or 0) > 0:
            return "nvidia.com/gpu"
        if float(alloc.get("amd.com/gpu") or 0) > 0:
            return "amd.com/gpu"
    return "nvidia.com/gpu"


def collect_k8s(cfg: dict[str, Any]) -> dict[str, Any]:
    kcfg = cfg.get("k8s") or {}
    if not kcfg.get("enabled"):
        return {"enabled": False, "ok": False, "error": "disabled", "nodes": [], "pods": [], "active_jobs": 0, "done_jobs": 0}

    tz = get_tz(cfg["project"].get("tz_offset_hours"))
    strip_proxy = kcfg.get("strip_proxy_env", "auto")
    client: _KubeClient | None = None
    try:
        client = _KubeClient(str(kcfg.get("kubeconfig") or ""), strip_proxy=strip_proxy)
        ns = str(kcfg.get("namespace") or client.namespace_hint or "default")

        def _safe_get(ep: str) -> dict[str, Any]:
            try:
                return client.get(ep, timeout=8) if client else {}
            except BaseException:
                return {}

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
            f_nodes = ex.submit(_safe_get, "/api/v1/nodes")
            f_quotas = ex.submit(_safe_get, f"/api/v1/namespaces/{ns}/resourcequotas")
            f_pods = ex.submit(_safe_get, f"/api/v1/namespaces/{ns}/pods")
            f_jobs = ex.submit(_safe_get, f"/apis/batch/v1/namespaces/{ns}/jobs")
            f_pmet = ex.submit(_safe_get, f"/apis/metrics.k8s.io/v1beta1/namespaces/{ns}/pods")

            nodes_raw = f_nodes.result().get("items", [])
            quotas = f_quotas.result().get("items", [])
            pods = f_pods.result().get("items", [])
            jobs = f_jobs.result().get("items", [])
            pmet_items = f_pmet.result().get("items", [])

        if not pods and not nodes_raw and not quotas:
            return {
                "enabled": True,
                "ok": False,
                "namespace": ns,
                "error": "K8s API returned empty response (check kubeconfig/RBAC/proxy)",
                "nodes": [],
                "pods": [],
                "active_jobs": 0,
                "done_jobs": 0,
            }

        gpu_mode = _detect_gpu_mode(nodes_raw, str(kcfg.get("gpu_resource") or "auto"))
        is_vcuda = gpu_mode == "tencent.com/vcuda-core"
        vc_core_per_gpu = max(1, int(kcfg.get("vcuda_core_per_gpu") or 100))
        vc_mem_per_gib = max(1, int(kcfg.get("vcuda_mem_units_per_gib") or 4))

        pod_real_cpu: dict[str, float] = {}
        pod_real_mem_gb: dict[str, float] = {}
        for pm in pmet_items:
            pname = (pm.get("metadata") or {}).get("name") or ""
            if not pname:
                continue
            c_sum = m_sum = 0.0
            for c in pm.get("containers") or []:
                u = c.get("usage") or {}
                c_sum += parse_cpu(u.get("cpu"))
                m_sum += parse_mem_gib(u.get("memory"))
            pod_real_cpu[pname] = c_sum
            pod_real_mem_gb[pname] = m_sum
        metrics_ok = len(pod_real_cpu) > 0

        # Namespace ResourceQuota
        q = (quotas[0].get("status") or {}) if quotas else {}
        hard, used = q.get("hard") or {}, q.get("used") or {}
        hard_cpu = parse_cpu(hard.get("limits.cpu") or hard.get("requests.cpu"))
        used_cpu_lim = parse_cpu(used.get("limits.cpu"))
        used_cpu_req = parse_cpu(used.get("requests.cpu"))
        hard_mem = parse_mem_gib(hard.get("limits.memory") or hard.get("requests.memory"))
        used_mem_lim = parse_mem_gib(used.get("limits.memory"))
        used_mem_req = parse_mem_gib(used.get("requests.memory"))
        pods_used = int(float(str(used.get("pods") or "0").replace("k", "000")))
        pods_hard = int(float(str(hard.get("pods") or "0").replace("k", "000")))

        node_map: dict[str, dict[str, Any]] = {}
        cluster_cpu_alloc = cluster_mem_alloc = 0.0
        cluster_gpu_alloc = 0.0
        cluster_vram_alloc_gb = 0.0
        nodes_ready = 0
        disk_pressure_nodes: list[str] = []

        for n in nodes_raw:
            nname = n["metadata"]["name"]
            labels = n["metadata"].get("labels") or {}
            alloc = (n.get("status") or {}).get("allocatable") or {}
            conds = (n.get("status") or {}).get("conditions") or []
            ready = any(c.get("type") == "Ready" and c.get("status") == "True" for c in conds)
            dp = any(c.get("type") == "DiskPressure" and c.get("status") == "True" for c in conds)
            if ready:
                nodes_ready += 1
            if dp:
                disk_pressure_nodes.append(nname)

            cpu_a = parse_cpu(alloc.get("cpu"))
            mem_a = parse_mem_gib(alloc.get("memory"))
            if is_vcuda:
                gpu_a = float(alloc.get("tencent.com/vcuda-core") or 0) / vc_core_per_gpu
                vram_a = float(alloc.get("tencent.com/vcuda-memory") or 0) / vc_mem_per_gib
            else:
                gpu_a = float(alloc.get(gpu_mode) or 0)
                vram_a = 0.0

            if "node-role.kubernetes.io/control-plane" in labels or "node-role.kubernetes.io/master" in labels:
                role = "control"
            elif gpu_a > 0:
                role = "gpu"
            else:
                role = "cpu"

            cluster_cpu_alloc += cpu_a
            cluster_mem_alloc += mem_a
            cluster_gpu_alloc += gpu_a
            cluster_vram_alloc_gb += vram_a

            node_map[nname] = {
                "name": nname,
                "role": role,
                "ready": ready,
                "disk_pressure": dp,
                "cpu_alloc": round(cpu_a, 1),
                "mem_alloc_gb": round(mem_a, 1),
                "gpu_alloc": round(gpu_a, 2),
                "vram_alloc_gb": round(vram_a, 1),
                "ns_cpu_req": 0.0,
                "ns_cpu_lim": 0.0,
                "ns_cpu_real": 0.0,
                "my_cpu_req": 0.0,
                "my_cpu_real": 0.0,
                "ns_mem_req_gb": 0.0,
                "ns_mem_real_gb": 0.0,
                "my_mem_real_gb": 0.0,
                "ns_pods": 0,
                "my_pods": 0,
                "gpu_ns": 0.0,
                "gpu_mine": 0.0,
                "vram_ns_gb": 0.0,
                "vram_mine_gb": 0.0,
            }

        owner_prefixes = [str(x) for x in (kcfg.get("owner_prefixes") or []) if x]
        owner_labels = dict(kcfg.get("owner_labels") or {})
        wl_prefixes = [str(x) for x in (kcfg.get("workload_prefixes") or []) if x]
        wl_part_of = set(str(x) for x in (kcfg.get("workload_part_of") or []) if x)
        has_owner_filter = bool(owner_prefixes or owner_labels)
        has_wl_filter = bool(wl_prefixes or wl_part_of)

        def _is_mine(obj: dict[str, Any]) -> bool:
            if not has_owner_filter:
                return True
            nm = (obj.get("metadata") or {}).get("name") or ""
            lb = (obj.get("metadata") or {}).get("labels") or {}
            if any(nm.startswith(pref) for pref in owner_prefixes):
                return True
            if any(lb.get(lk) == lv for lk, lv in owner_labels.items()):
                return True
            return False

        def _is_tracked_workload(obj: dict[str, Any]) -> bool:
            if not has_wl_filter:
                return _is_mine(obj)
            nm = (obj.get("metadata") or {}).get("name") or ""
            lb = (obj.get("metadata") or {}).get("labels") or {}
            jn = lb.get("job-name") or nm
            if any(jn.startswith(p) or nm.startswith(p) for p in wl_prefixes):
                return True
            if (lb.get("app.kubernetes.io/part-of") or "") in wl_part_of:
                return True
            return False

        def _pod_res(p: dict[str, Any]) -> tuple[float, float, float, float, float, float]:
            c_req = c_lim = m_req = m_lim = g_cards = vram_gb = 0.0
            for c in (p.get("spec") or {}).get("containers") or []:
                res = c.get("resources") or {}
                rq, lm = res.get("requests") or {}, res.get("limits") or {}
                c_req += parse_cpu(rq.get("cpu"))
                c_lim += parse_cpu(lm.get("cpu"))
                m_req += parse_mem_gib(rq.get("memory"))
                m_lim += parse_mem_gib(lm.get("memory"))
                if is_vcuda:
                    g_cards += float(lm.get("tencent.com/vcuda-core") or rq.get("tencent.com/vcuda-core") or 0) / vc_core_per_gpu
                    vram_gb += float(lm.get("tencent.com/vcuda-memory") or rq.get("tencent.com/vcuda-memory") or 0) / vc_mem_per_gib
                else:
                    g_cards += float(lm.get(gpu_mode) or rq.get(gpu_mode) or 0)
            return c_req, c_lim, m_req, m_lim, g_cards, vram_gb

        ns_gpu_used = my_gpu_used = ns_vram_used = my_vram_used = 0.0
        ns_cpu_real = my_cpu_real = ns_mem_real = my_mem_real = 0.0
        my_cpu_req = my_cpu_lim = my_mem_req = 0.0
        ns_pods_active = my_pods_running = my_pods_pending = 0

        for p in pods:
            phase = (p.get("status") or {}).get("phase") or ""
            if phase not in ("Running", "Pending"):
                continue
            ns_pods_active += 1
            pname = p["metadata"]["name"]
            nname = (p.get("spec") or {}).get("nodeName") or ""
            mine = _is_mine(p)
            c_req, c_lim, m_req, m_lim, g_cards, vram_gb = _pod_res(p)
            c_real = pod_real_cpu.get(pname, 0.0)
            m_real = pod_real_mem_gb.get(pname, 0.0)

            ns_gpu_used += g_cards
            ns_vram_used += vram_gb
            ns_cpu_real += c_real
            ns_mem_real += m_real
            if mine:
                my_gpu_used += g_cards
                my_vram_used += vram_gb
                my_cpu_real += c_real
                my_mem_real += m_real
                my_cpu_req += c_req
                my_cpu_lim += c_lim
                my_mem_req += m_req
                if phase == "Running":
                    my_pods_running += 1
                else:
                    my_pods_pending += 1

            if nname in node_map:
                nd = node_map[nname]
                nd["ns_pods"] += 1
                nd["ns_cpu_req"] += c_req
                nd["ns_cpu_lim"] += c_lim
                nd["ns_cpu_real"] += c_real
                nd["ns_mem_req_gb"] += m_req
                nd["ns_mem_real_gb"] += m_real
                nd["gpu_ns"] += g_cards
                nd["vram_ns_gb"] += vram_gb
                if mine:
                    nd["my_pods"] += 1
                    nd["my_cpu_req"] += c_req
                    nd["my_cpu_real"] += c_real
                    nd["my_mem_real_gb"] += m_real
                    nd["gpu_mine"] += g_cards
                    nd["vram_mine_gb"] += vram_gb

        role_ord = {"gpu": 0, "cpu": 1, "control": 2}
        nodes_list: list[dict[str, Any]] = []
        for nd in sorted(node_map.values(), key=lambda x: (role_ord.get(x["role"], 9), x["name"])):
            gpu_other = max(0.0, nd["gpu_ns"] - nd["gpu_mine"])
            vram_other = max(0.0, nd["vram_ns_gb"] - nd["vram_mine_gb"])
            nodes_list.append({
                **nd,
                "ns_cpu_req": round(nd["ns_cpu_req"], 1),
                "ns_cpu_lim": round(nd["ns_cpu_lim"], 1),
                "ns_cpu_real": round(nd["ns_cpu_real"], 2),
                "my_cpu_req": round(nd["my_cpu_req"], 1),
                "my_cpu_real": round(nd["my_cpu_real"], 2),
                "ns_mem_req_gb": round(nd["ns_mem_req_gb"], 1),
                "ns_mem_real_gb": round(nd["ns_mem_real_gb"], 1),
                "my_mem_real_gb": round(nd["my_mem_real_gb"], 1),
                "gpu_ns": round(nd["gpu_ns"], 2),
                "gpu_mine": round(nd["gpu_mine"], 2),
                "gpu_other": round(gpu_other, 2),
                "gpu_free": round(max(0.0, nd["gpu_alloc"] - nd["gpu_ns"]), 2),
                "vram_ns_gb": round(nd["vram_ns_gb"], 1),
                "vram_mine_gb": round(nd["vram_mine_gb"], 1),
                "vram_other_gb": round(vram_other, 1),
                "vram_free_gb": round(max(0.0, nd["vram_alloc_gb"] - nd["vram_ns_gb"]), 1),
                "cpu_real_pct": round(nd["ns_cpu_real"] / nd["cpu_alloc"] * 100.0, 1) if nd["cpu_alloc"] > 0 else 0.0,
                "mem_real_pct": round(nd["ns_mem_real_gb"] / nd["mem_alloc_gb"] * 100.0, 1) if nd["mem_alloc_gb"] > 0 else 0.0,
            })

        # Tracked Jobs & Active Pods
        tracked_jobs = [j for j in jobs if _is_tracked_workload(j)]
        active_jobs = [j for j in tracked_jobs if (j.get("status") or {}).get("active")]
        done_jobs = [
            j for j in tracked_jobs
            if not (j.get("status") or {}).get("active")
            and ((j.get("status") or {}).get("succeeded") or (j.get("status") or {}).get("failed"))
        ]

        now_ts = time.time()
        idle_thr = float(kcfg.get("idle_cpu_threshold", 0.05))
        idle_age = float(kcfg.get("idle_age_min", 15.0))
        pod_rows: list[dict[str, Any]] = []
        idle_suspect_cnt = 0

        for p in pods:
            if not _is_tracked_workload(p):
                continue
            st = p.get("status") or {}
            phase = st.get("phase") or "Unknown"
            if phase not in ("Running", "Pending"):
                continue
            pname = p["metadata"]["name"]
            labels = p["metadata"].get("labels") or {}
            job_name = labels.get("job-name") or pname
            start_dt = parse_iso(st.get("startTime") or p["metadata"].get("creationTimestamp"), tz)
            age_min = round((now_ts - start_dt.timestamp()) / 60.0, 1) if start_dt else None
            c_req, c_lim, m_req, m_lim, g_cards, vram_gb = _pod_res(p)
            c_real = round(pod_real_cpu[pname], 2) if pname in pod_real_cpu else None
            m_real = round(pod_real_mem_gb[pname], 2) if pname in pod_real_mem_gb else None
            idle_suspect = bool(
                phase == "Running" and age_min is not None and age_min >= idle_age and c_real is not None and c_real < idle_thr
            )
            if idle_suspect:
                idle_suspect_cnt += 1
            pod_rows.append({
                "name": pname,
                "job": job_name,
                "app": labels.get("app.kubernetes.io/part-of") or labels.get("app") or "",
                "tier": "gpu" if g_cards > 0 else "cpu",
                "phase": phase,
                "node": (p.get("spec") or {}).get("nodeName") or "-",
                "started": fmt_dt(start_dt),
                "age_min": age_min,
                "cpu_real": c_real,
                "cpu_lim": round(c_lim, 1),
                "mem_real_gb": m_real,
                "mem_lim_gb": round(m_lim, 1),
                "gpu_cards": round(g_cards, 2),
                "vram_gb": round(vram_gb, 1),
                "idle_suspect": idle_suspect,
            })

        pod_rows.sort(key=lambda r: (0 if r["phase"] == "Running" else 1, -(r["age_min"] or 0)))

        # Optional mounted shared storage usage
        shared_storage: list[dict[str, Any]] = []
        for sp in kcfg.get("shared_storage_paths") or []:
            if not isinstance(sp, dict) or not sp.get("path"):
                continue
            p = Path(str(sp["path"])).expanduser()
            try:
                du = shutil.disk_usage(p)
                shared_storage.append({
                    "label": str(sp.get("label") or p.name or str(p)),
                    "path": str(p),
                    "ok": True,
                    "total_gb": round(du.total / (1024 ** 3), 1),
                    "used_gb": round(du.used / (1024 ** 3), 1),
                    "free_gb": round(du.free / (1024 ** 3), 1),
                    "use_pct": round(du.used / max(du.total, 1) * 100.0, 1),
                })
            except OSError:
                shared_storage.append({"label": str(sp.get("label") or str(p)), "path": str(p), "ok": False})

        # Optional watched critical pods & FailedMount events (cached 60s)
        watch_pods = list(kcfg.get("watch_deployments_or_pods") or [])
        watch_reasons = list(kcfg.get("watch_event_reasons") or ["FailedMount"])
        health = _HEALTH_CACHE["data"]
        if (watch_pods or watch_reasons) and (time.time() - _HEALTH_CACHE["ts"] > 60 or health is None):
            unready_watched: list[str] = []
            for pref in watch_pods:
                ok_one = any(
                    p["metadata"]["name"].startswith(pref)
                    and (p.get("status") or {}).get("phase") == "Running"
                    and all(cs.get("ready") for cs in ((p.get("status") or {}).get("containerStatuses") or [{}]))
                    for p in pods
                )
                if not ok_one:
                    unready_watched.append(pref)

            recent_bad_events = 0
            if watch_reasons:
                ev_data = _safe_get(f"/api/v1/namespaces/{ns}/events")
                now_ev = time.time()
                for ev in ev_data.get("items", []):
                    if (ev.get("reason") or "") not in watch_reasons:
                        continue
                    ed = parse_iso(ev.get("lastTimestamp") or ev.get("eventTime") or "", tz)
                    if ed and (now_ev - ed.timestamp()) <= 300:
                        recent_bad_events += 1

            h_ok = (not unready_watched) and recent_bad_events < 3
            msg = "Healthy" if h_ok else (
                f"Unready pods: {', '.join(unready_watched)}" if unready_watched else f"{recent_bad_events} recent {', '.join(watch_reasons)} events (5m)"
            )
            health = {"ok": h_ok, "unready": unready_watched, "recent_events": recent_bad_events, "msg": msg}
            _HEALTH_CACHE["ts"] = time.time()
            _HEALTH_CACHE["data"] = health

        gpu_total = round(cluster_gpu_alloc, 2)
        gpu_used = round(ns_gpu_used, 2)
        gpu_mine = round(my_gpu_used, 2)
        gpu_other = round(max(0.0, gpu_used - gpu_mine), 2)

        return {
            "enabled": True,
            "ok": True,
            "error": None,
            "namespace": ns,
            "has_owner_filter": has_owner_filter,
            "gpu_resource": gpu_mode,
            "is_vcuda": is_vcuda,
            "metrics_ok": metrics_ok,
            "cluster": {
                "nodes_total": len(nodes_list),
                "nodes_ready": nodes_ready,
                "cpu_alloc": round(cluster_cpu_alloc, 1),
                "mem_alloc_gb": round(cluster_mem_alloc, 1),
                "gpu_total": gpu_total,
                "vram_total_gb": round(cluster_vram_alloc_gb, 1),
            },
            "quota": {
                "has_quota": bool(quotas),
                "free_cpu": round(max(0.0, hard_cpu - used_cpu_lim), 1) if hard_cpu else 0.0,
                "used_cpu": round(used_cpu_lim, 1),
                "req_cpu": round(used_cpu_req, 1),
                "hard_cpu": round(hard_cpu, 1),
                "real_cpu_ns": round(ns_cpu_real, 2),
                "real_cpu_mine": round(my_cpu_real, 2),
                "my_cpu_req": round(my_cpu_req, 1),
                "my_cpu_lim": round(my_cpu_lim, 1),
                "free_mem_gb": round(max(0.0, hard_mem - used_mem_lim), 1) if hard_mem else 0.0,
                "used_mem_gb": round(used_mem_lim, 1),
                "req_mem_gb": round(used_mem_req, 1),
                "hard_mem_gb": round(hard_mem, 1),
                "real_mem_ns_gb": round(ns_mem_real, 1),
                "real_mem_mine_gb": round(my_mem_real, 1),
                "my_mem_req_gb": round(my_mem_req, 1),
                "pods_used": pods_used or ns_pods_active,
                "pods_hard": pods_hard,
                "my_pods_running": my_pods_running,
                "my_pods_pending": my_pods_pending,
            },
            "gpu_cards_used": gpu_used,
            "gpu_cards_mine": gpu_mine,
            "gpu_cards_other": gpu_other,
            "gpu_cards_free": round(max(0.0, gpu_total - gpu_used), 2),
            "gpu_cards_total": gpu_total,
            "gpu_vram_used_gb": round(ns_vram_used, 1),
            "gpu_vram_mine_gb": round(my_vram_used, 1),
            "gpu_vram_other_gb": round(max(0.0, ns_vram_used - my_vram_used), 1),
            "gpu_vram_total_gb": round(cluster_vram_alloc_gb, 1),
            "active_jobs": len(active_jobs),
            "done_jobs": len(done_jobs),
            "idle_suspect_pods": idle_suspect_cnt,
            "nodes": nodes_list,
            "pods": pod_rows[:120],
            "shared_storage": shared_storage,
            "health": health or {"ok": True, "msg": "Healthy"},
            "disk_pressure_nodes": disk_pressure_nodes,
        }
    except BaseException as e:
        return {
            "enabled": True,
            "ok": False,
            "namespace": str(kcfg.get("namespace") or "default"),
            "error": str(e)[:220],
            "nodes": [],
            "pods": [],
            "active_jobs": 0,
            "done_jobs": 0,
        }
    finally:
        if client is not None:
            client.close()
