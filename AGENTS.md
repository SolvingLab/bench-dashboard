# AGENTS.md — bench-dashboard

> Entry point for AI agents adopting or adapting `bench-dashboard` inside any benchmark or evaluation repository.

## Purpose

`bench-dashboard` is a **zero-dependency, strictly read-only** live telemetry dashboard (Web UI + CLI + static HTML snapshot exporter) for running benchmarks on local GPU hosts, Docker/Harbor runners, and Kubernetes clusters (both standard `nvidia.com/gpu` and fractional `tencent.com/vcuda-core`).

It is deliberately designed so an AI agent can clone or vendor it into a new benchmark repo, inspect the repo layout, write a small `bench-dashboard.toml` (and optionally a tiny `adapter.py`), and have a live command-center running in under 2 minutes.

## Hard Rules (Always Enforce)

1. **Strictly Read-Only**: Never mutate the target benchmark repository's tasks, `dataset.toml`, `jobs/`, Docker containers, or Kubernetes resources. The only file written at runtime is the local sparkline ring buffer (`history_file`, default `.live_history.jsonl`).
2. **Zero Third-Party Dependencies**: Keep all core code Python 3.11+ standard library only (`tomllib`, `urllib`, `http.server`, `subprocess`, `json`) and frontend single-file vanilla HTML/CSS/JS + inline SVG. Do not introduce npm/vite/React or heavy pip dependencies unless the user explicitly asks.
3. **Never Commit Secrets or Lab-Private Info**: Host IPs, usernames, tokens, custom docker socket names, kubeconfig paths, and internal storage mounts belong in gitignored `bench-dashboard.toml` or environment variables (`KUBECONFIG`, `K8S_NAMESPACE`, `K8S_USER_PREFIX`, `BENCH_DASH_DOCKER_HOST`), never hardcoded in committed files.
4. **Verify Before Handing Off**: Always verify with `--cli`, `--json`, and `/api/snapshot` (plus `--export` if offline sharing is requested) before declaring adaptation complete.

## Repository Map

```text
bench-dashboard/
├── run.py                                  # CLI / server entrypoint
├── config.example.toml                     # Full annotated config reference
├── bench_dashboard/
│   ├── config.py                           # TOML + env loader + zero-config auto-detection
│   ├── utils.py                            # Timestamps, JSONL, K8s quantity parsers
│   ├── server.py                           # HTTP server, ring buffer, --cli, --json, --export
│   ├── ui.html                             # Single-file adaptive UI (EN/ZH, Light/Dark)
│   ├── collectors/
│   │   ├── __init__.py                     # Parallel snapshot aggregator + built-in safety alerts
│   │   ├── host.py                         # CPU, RAM, Swap (+Top Swap), /dev/shm, Disks, GPUs, Docker
│   │   ├── k8s.py                          # Zero-dep K8s REST client (nodes, quota, metrics, GPU/vCUDA, pods)
│   │   ├── harbor.py                       # Harbor tasks/ + jobs/ + multi-model leaderboard + live trials
│   │   └── pipeline.py                     # Generic process watcher + JSONL throughput streams + backlog
│   └── adapters/
│       └── custom_template.py              # Drop-in hook: extend_snapshot(snap, cfg)
├── skills/
│   └── adapt-bench-dashboard/SKILL.md      # Step-by-step agent skill for adapting to a new repo
├── examples/                               # Minimal & K8s+vCUDA example configs
└── tests/test_dashboard.py                 # Stdlib unittest suite
```

## How an Agent Should Adapt This Repo to a New Benchmark Project

Follow the bundled skill [skills/adapt-bench-dashboard/SKILL.md](skills/adapt-bench-dashboard/SKILL.md). In short:

1. **Try zero-config first**:
   ```bash
   python3 /path/to/bench-dashboard/run.py --target /path/to/your-bench --cli
   ```
   This auto-detects `tasks/`, `jobs/`, `tasks/dataset.toml`, disks, GPUs, Docker, and `~/.kube/config`.
2. **Generate a project-local `bench-dashboard.toml`** (keep it gitignored or local if it has lab-specific paths):
   - Set `[project]` `name`, `badge`, `target_repo`.
   - Set `[host]` `docker_host` (if using a custom socket) and critical `[host.disks]` mounts.
   - Set `[k8s]` `namespace`, `owner_prefixes`, `workload_prefixes`, `strip_proxy_env` (if cluster API is on direct LAN behind a proxy), `shared_storage_paths`.
   - Set `[harbor]` `primary_model`, `score_metric_keys`, `buckets` (if using extra pools like `lite/` or `candidates/`), `slot_lock_dir` / `slots_total`.
   - Add `[[pipelines.processes]]` for local dispatchers/builders and `[[pipelines.jsonl_streams]]` for any JSONL build/audit logs.
3. **Only write a Python adapter if TOML isn't enough**:
   - Copy `bench_dashboard/adapters/custom_template.py` into the target project (e.g. `dashboard_adapter.py`) and set `[pipelines] custom_adapter = "dashboard_adapter.py"`.
   - Implement `extend_snapshot(snap, cfg)` to append `snap["custom"]["kpis"]`, `snap["custom"]["panels"]` (`bars`, `table`, or `kv`), or `snap["alerts"]`. The UI renders them automatically with zero frontend edits.
4. **Only edit `collectors/` or `ui.html`** if the user explicitly asks for custom charts/tabs beyond the built-in views.
5. **Run verification**:
   ```bash
   python3 -m unittest discover -s tests
   python3 run.py --config bench-dashboard.toml --cli
   python3 run.py --config bench-dashboard.toml --export /tmp/bench-snapshot.html
   ```
