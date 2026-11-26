"""Stdlib-only unit and end-to-end tests for bench-dashboard."""
from __future__ import annotations

import datetime as dt
import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from bench_dashboard.collectors import collect_snapshot
from bench_dashboard.config import load_config
from bench_dashboard.server import (
    ReusableThreadingHTTPServer,
    SnapshotStore,
    export_html,
    make_handler,
    print_cli,
)
from bench_dashboard.utils import parse_cpu, parse_mem_gib


def _make_sample_bench(root: Path) -> None:
    """Create a realistic miniature Harbor benchmark repository in a temp directory."""
    # Tasks
    for cat, slug, tier, gpus in (
        ("biology", "task-alpha", "standard", 0),
        ("medicine", "task-beta", "gpu", 1),
    ):
        td = root / "tasks" / cat / slug
        td.mkdir(parents=True, exist_ok=True)
        (td / "task.toml").write_text(
            f"""
[metadata]
field = "{cat}"
compute_tier = "{tier}"
primary_metric = "auroc"

[environment]
gpus = {gpus}
network_mode = "no-network"

[verifier]
environment_mode = "separate"

[verifier.environment]
network_mode = "no-network"
""".strip(),
            encoding="utf-8",
        )

    (root / "tasks" / "dataset.toml").write_text(
        """
[[tasks]]
path = "tasks/biology/task-alpha"

[[tasks]]
path = "tasks/medicine/task-beta"
""".strip(),
        encoding="utf-8",
    )

    # Checksums cache
    cs = root / "cache" / "bench-status" / "checksums.json"
    cs.parent.mkdir(parents=True, exist_ok=True)
    cs.write_text(
        json.dumps({
            "tasks/biology/task-alpha": {"sum": "sha-alpha"},
            "tasks/medicine/task-beta": {"sum": "sha-beta"},
        }),
        encoding="utf-8",
    )

    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    start_iso = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5)).isoformat()

    def _write_trial(group: str, trial: str, task_path: str, checksum: str, agent: str, model: str, reward: float, pos_score: float | None = None) -> None:
        tdir = root / "jobs" / group / trial
        (tdir / "verifier").mkdir(parents=True, exist_ok=True)
        (tdir / "result.json").write_text(
            json.dumps({
                "trial_name": trial,
                "task_id": {"path": task_path},
                "task_checksum": checksum,
                "agent_info": {"name": agent, "model_info": {"name": model} if model else {}},
                "verifier_result": {"rewards": {"reward": reward}},
                "agent_execution": {"started_at": start_iso, "finished_at": now_iso},
                "finished_at": now_iso,
            }),
            encoding="utf-8",
        )
        if pos_score is not None:
            (tdir / "verifier" / "result.json").write_text(
                json.dumps({"metrics": {"position_score": pos_score, "primary": "auroc", "primary_metric": 0.87}}),
                encoding="utf-8",
            )

    _write_trial("selfcheck-1", "t1", "tasks/biology/task-alpha", "sha-alpha", "nop", "", 0)
    _write_trial("selfcheck-1", "t2", "tasks/biology/task-alpha", "sha-alpha", "oracle", "", 1)
    _write_trial("eval-m1", "t3", "tasks/biology/task-alpha", "sha-alpha", "agent", "model-a", 1, 82.5)
    _write_trial("eval-m2", "t4", "tasks/biology/task-alpha", "sha-alpha", "agent", "model-b", 0, 41.0)

    # Pipeline JSONL & backlog
    work = root / "work"
    work.mkdir(exist_ok=True)
    (work / "builds.jsonl").write_text(
        json.dumps({"task_id": "t-1", "status": "built", "timestamp": now_iso, "elapsed_s": 180, "note": "ok"}) + "\n"
        + json.dumps({"task_id": "t-2", "status": "blocked", "timestamp": now_iso, "elapsed_s": 60, "note": "missing data"}) + "\n",
        encoding="utf-8",
    )
    (work / "pool.jsonl").write_text(
        "\n".join(json.dumps({"task_id": f"t-{i}", "field": "biology", "tier": "standard", "decision": "PASS"}) for i in range(1, 6)) + "\n",
        encoding="utf-8",
    )

    # Custom adapter
    (root / "my_adapter.py").write_text(
        """
def extend_snapshot(snap, cfg):
    snap["custom"]["kpis"].append({"label": "Adapter KPI", "tag": "test", "value": "99%", "pct": 99, "sub": "from adapter"})
    snap["custom"]["panels"].append({"title": "Adapter Table", "meta": "1 row", "type": "table", "columns": ["K", "V"], "rows": [["a", "b"]]})
""".strip(),
        encoding="utf-8",
    )

    # Config TOML
    (root / "bench-dashboard.toml").write_text(
        """
[project]
name = "Test-Bench"
badge = "TB"
target_repo = "."
history_file = "test-history.jsonl"

[k8s]
enabled = false

[harbor]
enabled = true
primary_model = "model-a"

[pipelines]
enabled = true
custom_adapter = "my_adapter.py"

[[pipelines.jsonl_streams]]
name = "Builds"
path = "work/builds.jsonl"
id_field = "task_id"

[pipelines.backlog]
enabled = true
pool_path = "work/pool.jsonl"
done_paths = ["work/builds.jsonl"]
id_field = "task_id"
category_field = "field"
tier_field = "tier"
""".strip(),
        encoding="utf-8",
    )


class TestBenchDashboard(unittest.TestCase):
    def test_k8s_unit_parsers(self) -> None:
        self.assertAlmostEqual(parse_cpu("250m"), 0.25)
        self.assertAlmostEqual(parse_cpu("2"), 2.0)
        self.assertAlmostEqual(parse_cpu("500000000n"), 0.5)
        self.assertAlmostEqual(parse_mem_gib("16Gi"), 16.0)
        self.assertAlmostEqual(parse_mem_gib("2048Mi"), 2.0)
        self.assertAlmostEqual(parse_mem_gib("1Ti"), 1024.0)

    def test_end_to_end_collect_export_and_http(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            _make_sample_bench(root)

            cfg = load_config(config_path=root / "bench-dashboard.toml")
            snap = collect_snapshot(cfg)

            self.assertEqual(snap["project"]["name"], "Test-Bench")
            self.assertTrue(snap["host"]["ok"])
            self.assertFalse(snap["k8s"]["enabled"])

            hb = snap["harbor"]
            self.assertTrue(hb["ok"])
            self.assertEqual(hb["registered_total"], 2)
            self.assertEqual(hb["selfcheck_both_ok"], 1)
            self.assertEqual(hb["fully_done"], 1)
            self.assertEqual(hb["primary_eval"]["model"], "model-a")
            self.assertEqual(hb["primary_eval"]["median_score"], 82.5)
            self.assertEqual(len(hb["leaderboard"]), 2)

            pl = snap["pipelines"]
            self.assertTrue(pl["has_content"])
            self.assertEqual(pl["streams"][0]["total"], 2)
            self.assertEqual(pl["backlog"]["pool_total"], 5)
            self.assertEqual(pl["backlog"]["done_total"], 2)
            self.assertEqual(pl["backlog"]["remaining_total"], 3)

            self.assertEqual(len(snap["custom"]["kpis"]), 1)
            self.assertEqual(len(snap["custom"]["panels"]), 1)

            # Export standalone HTML
            out_html = root / "snap.html"
            export_html(out_html, snap)
            html_txt = out_html.read_text(encoding="utf-8")
            self.assertIn("Test-Bench", html_txt)
            self.assertIn("Adapter KPI", html_txt)
            self.assertNotIn("window.__BOOTSTRAP_SNAPSHOT__ = null;", html_txt)

            # Print CLI without error
            print_cli(snap)

            # Live HTTP server check
            store = SnapshotStore(cfg)
            store.refresh_now()
            srv = ReusableThreadingHTTPServer(("127.0.0.1", 0), make_handler(store))
            port = srv.server_address[1]
            t = threading.Thread(target=srv.serve_forever, daemon=True)
            t.start()
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=3) as r:
                    self.assertEqual(r.read().decode().strip(), "ok")
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/snapshot", timeout=3) as r:
                    data = json.loads(r.read().decode())
                    self.assertEqual(data["project"]["name"], "Test-Bench")
                    self.assertGreaterEqual(len(data["live_history"]), 1)
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=3) as r:
                    body = r.read().decode()
                    self.assertIn("<!DOCTYPE html>", body)
            finally:
                srv.shutdown()
                srv.server_close()


if __name__ == "__main__":
    unittest.main()
