#!/usr/bin/env python3
"""bench-dashboard HTTP Server, CLI Summary, JSON Dumper, and Standalone HTML Exporter.

Strictly read-only against the target repository, Harbor jobs/, Docker, and Kubernetes API.
Only writes the lightweight local sparkline ring buffer (`history_file`, default `.live_history.jsonl`).
"""
from __future__ import annotations

import argparse
import collections
import http.server
import json
import socketserver
import sys
import threading
from pathlib import Path
from typing import Any

from .collectors import collect_snapshot
from .config import load_config
from .utils import resolve_path

PKG_DIR = Path(__file__).resolve().parent
UI_HTML_PATH = PKG_DIR / "ui.html"


def _extract_history_point(snap: dict[str, Any]) -> dict[str, Any]:
    h = snap.get("host") or {}
    hcpu = h.get("cpu") or {}
    hgpu = h.get("gpus") or {}
    k8 = snap.get("k8s") or {}
    kq = k8.get("quota") or {}
    hb = snap.get("harbor") or {}
    hv = hb.get("velocity") or {}
    return {
        "ts": snap.get("generated_epoch", 0),
        "lbl": str(snap.get("generated_at") or "")[11:16],
        "host_cpu_pct": hcpu.get("busy_pct", 0.0),
        "host_mem_pct": h.get("mem_used_pct", 0.0),
        "host_swap_pct": h.get("swap_used_pct", 0.0),
        "host_gpu_util": hgpu.get("util_avg_pct", 0.0),
        "host_gpu_vram_pct": hgpu.get("mem_used_pct", 0.0),
        "host_gpu_free": hgpu.get("free_cards", 0),
        "k8s_my_cpu": kq.get("real_cpu_mine", 0.0),
        "k8s_ns_cpu": kq.get("real_cpu_ns", 0.0),
        "k8s_gpu_mine": k8.get("gpu_cards_mine", 0.0),
        "k8s_gpu_used": k8.get("gpu_cards_used", 0.0),
        "k8s_active": k8.get("active_jobs", 0),
        "harbor_run": (hb.get("running") or {}).get("count", 0),
        "harbor_ph_1h": hv.get("count_1h", 0),
    }


def _load_history(path: Path, max_points: int) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    try:
        for ln in path.read_text(encoding="utf-8", errors="replace").splitlines()[-max_points:]:
            s = ln.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
                if isinstance(obj, dict) and "ts" in obj:
                    out.append(obj)
            except json.JSONDecodeError:
                continue
    except OSError:
        return []
    return out


class SnapshotStore:
    def __init__(self, cfg: dict[str, Any]):
        self.cfg = cfg
        self.refresh_sec = int(cfg["project"].get("refresh_sec", 20))
        self.max_history = int(cfg["project"].get("history_max_points", 180))
        hist_raw = str(cfg["project"].get("history_file") or ".live_history.jsonl")
        hist_base = Path(cfg.get("_config_dir") or cfg["project"]["target_repo"])
        self.history_path = resolve_path(hist_base, hist_raw)
        self._lock = threading.Lock()
        self._snapshot: dict[str, Any] | None = None
        self._json_bytes: bytes = b"{}"
        self._history: collections.deque[dict[str, Any]] = collections.deque(
            _load_history(self.history_path, self.max_history), maxlen=self.max_history
        )
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def _record_history(self, pt: dict[str, Any]) -> list[dict[str, Any]]:
        if not self._history or (pt["ts"] - self._history[-1].get("ts", 0)) >= 10:
            self._history.append(pt)
            try:
                self.history_path.parent.mkdir(parents=True, exist_ok=True)
                with self.history_path.open("a", encoding="utf-8") as fh:
                    fh.write(json.dumps(pt, ensure_ascii=False) + "\n")
                if self.history_path.stat().st_size > 150_000:
                    self.history_path.write_text(
                        "\n".join(json.dumps(x, ensure_ascii=False) for x in self._history) + "\n",
                        encoding="utf-8",
                    )
            except OSError:
                pass
        return list(self._history)

    def refresh_now(self) -> dict[str, Any]:
        snap = collect_snapshot(self.cfg)
        pt = _extract_history_point(snap)
        with self._lock:
            snap["live_history"] = self._record_history(pt)
            raw = json.dumps(snap, ensure_ascii=False).encode("utf-8")
            self._snapshot = snap
            self._json_bytes = raw
        return snap

    def get_bytes(self) -> bytes:
        with self._lock:
            if self._snapshot is not None:
                return self._json_bytes
        self.refresh_now()
        with self._lock:
            return self._json_bytes

    def start_background(self) -> None:
        self.refresh_now()

        def _loop() -> None:
            while not self._stop.wait(self.refresh_sec):
                try:
                    self.refresh_now()
                except Exception as e:
                    print(f"[bench-dashboard] background refresh error: {e}", file=sys.stderr, flush=True)

        self._thread = threading.Thread(target=_loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()


def make_handler(store: SnapshotStore):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: Any) -> None:
            pass

        def _send(self, code: int, content_type: str, body: bytes) -> None:
            self.send_response(code)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:
            path = self.path.split("?", 1)[0]
            if path in ("/", "/index.html"):
                self._send(200, "text/html; charset=utf-8", UI_HTML_PATH.read_bytes())
            elif path == "/api/snapshot":
                self._send(200, "application/json; charset=utf-8", store.get_bytes())
            elif path == "/healthz":
                self._send(200, "text/plain; charset=utf-8", b"ok\n")
            else:
                self._send(404, "text/plain; charset=utf-8", b"Not Found\n")

        def do_POST(self) -> None:
            path = self.path.split("?", 1)[0]
            if path == "/api/refresh":
                store.refresh_now()
                self._send(200, "application/json; charset=utf-8", store.get_bytes())
            else:
                self._send(404, "text/plain; charset=utf-8", b"Not Found\n")

    return Handler


class ReusableThreadingHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def _bar(pct: float, width: int = 20) -> str:
    pct = max(0.0, min(100.0, pct))
    filled = int(round(pct / 100.0 * width))
    return "█" * filled + "░" * (width - filled)


def print_cli(snap: dict[str, Any]) -> None:
    proj = snap["project"]
    h = snap.get("host") or {}
    k8 = snap.get("k8s") or {}
    hb = snap.get("harbor") or {}
    pl = snap.get("pipelines") or {}

    print(f"\n=== {proj['name']} — Bench & Compute Dashboard [{snap['generated_at']}] ({snap['collect_sec']}s) ===")
    print(f"    target: {proj['target_repo']}")

    if snap.get("alerts"):
        print("\n[Alerts]")
        for a in snap["alerts"]:
            tag = "CRIT" if a.get("level") == "danger" else "WARN"
            print(f"  ! [{tag}] {a['stage']}: {a['msg']}")

    if h.get("ok"):
        hcpu = h["cpu"]
        hgpu = h["gpus"]
        print(f"\n[Host: {h['label']}]")
        print(f"  CPU   : [{_bar(hcpu['busy_pct'])}] {hcpu['busy_pct']:5.1f}% (usr {hcpu['user_pct']}% / sys {hcpu['sys_pct']}% / iow {hcpu['iowait_pct']}%) | Load {hcpu['load'][0]}/{hcpu['ncpu']}c")
        print(f"  RAM   : [{_bar(h['mem_used_pct'])}] {h['mem_used_gb']}/{h['mem_total_gb']} GiB ({h['mem_used_pct']}%) | Swap {h['swap_used_gb']}/{h['swap_total_gb']} GiB ({h['swap_used_pct']}%)")
        if hgpu.get("ok"):
            print(f"  GPUs  : {hgpu['count']}x | free {hgpu['free_cards']}/{hgpu['count']} | util {hgpu['util_avg_pct']}% | VRAM {hgpu['mem_used_gb']}/{hgpu['mem_total_gb']} GiB ({hgpu['mem_used_pct']}%) | {int(hgpu['power_draw_w'])}W")
        disk_parts = [f"{d['label']} {d['free_gb']}G free ({d['use_pct']}%)" for d in (h.get("disks") or []) if d.get("ok")]
        if disk_parts:
            print(f"  Disks : {' | '.join(disk_parts)}")
        doc = h.get("docker") or {}
        if doc.get("ok"):
            print(f"  Docker: {doc['total']} containers ({doc['bench']} bench)")

    if k8.get("enabled"):
        print(f"\n[Kubernetes: ns={k8.get('namespace', '-')}]")
        if k8.get("ok"):
            q = k8["quota"]
            cl = k8["cluster"]
            print(f"  Nodes : {cl['nodes_ready']}/{cl['nodes_total']} Ready | Cluster {cl['cpu_alloc']:.0f}c / {cl['mem_alloc_gb']:.0f} GiB / {cl['gpu_total']} GPUs ({k8['gpu_resource']})")
            print(f"  CPU   : real={q['real_cpu_ns']}c (mine={q['real_cpu_mine']}c) | Lim {q['used_cpu']}/{q['hard_cpu']}c | Pods {q['pods_used']}/{q['pods_hard'] or '-'}")
            if cl["gpu_total"] > 0:
                print(f"  GPUs  : mine={k8['gpu_cards_mine']} | other={k8['gpu_cards_other']} | free={k8['gpu_cards_free']} / {k8['gpu_cards_total']}")
            print(f"  Jobs  : active={k8['active_jobs']} | done={k8['done_jobs']} | idle_suspect={k8['idle_suspect_pods']}")
        else:
            print(f"  Status: unavailable ({k8.get('error')})")

    if hb.get("enabled") and hb.get("ok"):
        hv = hb["velocity"]
        pe = hb["primary_eval"]
        print("\n[Harbor / Benchmark]")
        print(f"  Tasks : {hb['registered_total']} registered ({hb['grand_total_tasks']} total across pools) | Selfcheck(nop+oracle) {hb['selfcheck_both_ok']}/{hb['registered_total']} | Running {hb['running']['count']}")
        print(f"  Speed : {hv['rate_ph']} trials/h ({hv['rate_pd']}/day) | 1h={hv['count_1h']} 6h={hv['count_6h']} 24h={hv['count_24h']} | Total trials={hb['all_trials_total']}")
        if pe["n_scored"] > 0:
            print(f"  Eval  : model={pe['model']} | scored={pe['n_scored']} | median={pe['median_score']} | mean={pe['mean_score']} | median_min={pe['median_min']}m")
        if hb.get("leaderboard"):
            top3 = ", ".join(f"{r['model']}({r['scored']}: med={r['median_score']})" for r in hb["leaderboard"][:3])
            print(f"  Models: {top3}")

    if pl.get("has_content"):
        print("\n[Pipelines]")
        for p in pl.get("processes") or []:
            print(f"  ● [{p['kind']}] {p['title']} | {p['etime']} | CPU {p['cpu_pct']}%")
        for st in pl.get("streams") or []:
            print(f"  · Stream {st['name']}: total={st['total']} | 1h={st['count_1h']} 6h={st['count_6h']} 24h={st['count_24h']} ({st['rate_ph']}/h)")
        bl = pl.get("backlog") or {}
        if bl.get("enabled"):
            print(f"  · {bl['label']}: {bl['done_total']}/{bl['pool_total']} ({bl['done_pct']}%) | rem={bl['remaining_total']} | ETA={bl['eta_days'] or '-'}d")
    print()


def export_html(out_path: Path, snap: dict[str, Any]) -> None:
    tpl = UI_HTML_PATH.read_text(encoding="utf-8")
    payload = json.dumps(snap, ensure_ascii=False).replace("</", "<\\/")
    marker = "window.__BOOTSTRAP_SNAPSHOT__ = null;"
    if marker not in tpl:
        raise RuntimeError("Bootstrap marker missing in ui.html")
    rendered = tpl.replace(marker, f"window.__BOOTSTRAP_SNAPSHOT__ = {payload};", 1)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(rendered, encoding="utf-8")
    print(f"Exported standalone snapshot HTML -> {out_path} ({len(rendered) / 1024:.1f} KB)")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="bench-dashboard: Zero-dependency read-only benchmark & compute dashboard")
    ap.add_argument("--config", "-c", type=Path, default=None, help="Path to bench-dashboard.toml")
    ap.add_argument("--target", "-t", type=Path, default=None, help="Target benchmark repo directory (default: cwd or config)")
    ap.add_argument("--host", default="127.0.0.1", help="HTTP bind address (default: 127.0.0.1; use 0.0.0.0 for LAN)")
    ap.add_argument("--port", type=int, default=8787, help="HTTP port (default: 8787)")
    ap.add_argument("--refresh-sec", type=int, default=None, help="Background refresh interval in seconds")
    ap.add_argument("--no-k8s", action="store_true", help="Disable Kubernetes telemetry")
    ap.add_argument("--cli", action="store_true", help="Print terminal summary and exit")
    ap.add_argument("--json", action="store_true", help="Print raw snapshot JSON and exit")
    ap.add_argument("--export", type=Path, default=None, help="Export standalone offline HTML snapshot and exit")
    args = ap.parse_args(argv)

    cfg = load_config(config_path=args.config, target_repo=args.target, no_k8s=args.no_k8s)
    if args.refresh_sec is not None:
        cfg["project"]["refresh_sec"] = max(5, args.refresh_sec)

    if args.cli or args.json or args.export:
        snap = collect_snapshot(cfg)
        hist_base = Path(cfg.get("_config_dir") or cfg["project"]["target_repo"])
        hist_path = resolve_path(hist_base, str(cfg["project"].get("history_file") or ".live_history.jsonl"))
        max_pts = int(cfg["project"].get("history_max_points", 180))
        hist = _load_history(hist_path, max_pts)
        hist.append(_extract_history_point(snap))
        snap["live_history"] = hist[-max_pts:]
        if args.json:
            print(json.dumps(snap, ensure_ascii=False, indent=2))
        if args.cli:
            print_cli(snap)
        if args.export:
            export_html(args.export, snap)
        return 0

    store = SnapshotStore(cfg)
    print(f"[bench-dashboard] Collecting initial live snapshot for {cfg['project']['name']}...", flush=True)
    store.start_background()
    server = ReusableThreadingHTTPServer((args.host, args.port), make_handler(store))
    print(
        f"[bench-dashboard] Listening on http://{args.host}:{args.port} "
        f"(target={cfg['project']['target_repo']}, refresh={store.refresh_sec}s)",
        flush=True,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[bench-dashboard] Stopped.", flush=True)
    finally:
        store.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
