# bench-dashboard

A **zero-dependency, strictly read-only** live monitoring dashboard for benchmark evaluations, Harbor task suites, local GPU servers, and Kubernetes clusters.

Built for teams running long-horizon agent benchmarks (Harbor, Terminal-Bench, SWE-bench-style evaluations, and custom task-generation pipelines) who want an instant command center without setting up Prometheus, Grafana, Node.js, or database backends.

## Highlights

- **Zero external dependencies**: Python 3.11+ standard library backend + single-file Linear-inspired vanilla HTML/CSS/JS frontend with smooth gradient SVG curves, hover crosshairs, **`⌘K` Command Palette**, **slide-over Inspector drawer** (click any Task / Trial / K8s Pod / Node), `EN / 中文` toggle, and Light / Dark theme (follows OS preference).
- **Strictly read-only**: Never modifies tasks, ledgers, `jobs/`, Docker containers, or Kubernetes resources.
- **Host Compute & Safety Telemetry**: Real-time CPU (`/proc/stat`), RAM, **Swap + Top Swap offender alerts**, `/dev/shm`, multi-mount disk usage, per-card NVIDIA GPU monitoring (`nvidia-smi` util/VRAM/temp/power/processes incl. vLLM), and Docker container count.
- **Kubernetes Telemetry (Zero PyYAML)**: Parses kubeconfig via `kubectl config view --minify --raw -o json` (or env vars), supports both standard **`nvidia.com/gpu`** and fractional **`tencent.com/vcuda-core`**, `metrics.k8s.io` real-time pod CPU/RAM, namespace ResourceQuotas, per-node GPU slot visualization, `DiskPressure` & low-CPU idle pod warnings.
- **Harbor & Multi-Model Evaluation Tracking**: Auto-discovers `tasks/dataset.toml` and `jobs/*/result.json`, tracks `nop` / `oracle` / `model` states with checksum validity (`ok`, `ok-doc`, `stale`, `running`, `queued`, `failed`), **multi-model leaderboard**, score distributions (auto `0..1` or `0..100` scale), and 24-hour hourly trial throughput.
- **Declarative Pipelines + Hot-Reloadable Python Adapter**: Monitor arbitrary background daemons, JSONL event streams (1h/6h/24h velocity), and backlog pools via `bench-dashboard.toml` — or drop in a tiny `extend_snapshot(snap, cfg)` Python adapter for domain-specific KPIs, bars, tables, and alerts.
- **Agent-Ready Out of the Box**: Includes [`AGENTS.md`](AGENTS.md) and [`skills/adapt-bench-dashboard/SKILL.md`](skills/adapt-bench-dashboard/SKILL.md) so any coding agent can automatically adapt the dashboard to a new benchmark repository.

## Quick Start

Requires **Python 3.11+** (no `pip install` or `npm install` needed).

```bash
git clone https://github.com/SolvingLab/bench-dashboard.git
cd bench-dashboard

# 1. Instant terminal summary for any benchmark repo (zero-config auto-detect)
python3 run.py --target /path/to/your-benchmark --cli

# 2. Start live web dashboard (default: http://127.0.0.1:8787, 20s refresh)
python3 run.py --target /path/to/your-benchmark --port 8787

# 3. Allow LAN access (e.g. from your laptop)
python3 run.py --target /path/to/your-benchmark --host 0.0.0.0 --port 8787

# 4. Export a self-contained offline HTML snapshot (great for sharing in chat/PRs)
python3 run.py --target /path/to/your-benchmark --export snapshot.html
```

## Adapting to Your Benchmark Repo (With or Without an Agent)

### Option A — Let Your AI Agent Do It Automatically

Point your coding agent (Claude Code, Codex, Antigravity, Pi, etc.) at this repo and ask:

> "Read `bench-dashboard/AGENTS.md` and `bench-dashboard/skills/adapt-bench-dashboard/SKILL.md`, then set up a read-only `bench-dashboard.toml` (and custom adapter if needed) for this repository and verify it with `--cli`."

You can also symlink or copy `skills/adapt-bench-dashboard/` directly into your agent's skills directory (`.agents/skills/` or `.claude/skills/`).

### Option B — Configure Manually

1. Copy [`config.example.toml`](config.example.toml) to `bench-dashboard.toml` in your benchmark repo (see also [`examples/harbor-minimal.toml`](examples/harbor-minimal.toml) and [`examples/k8s-vcuda-lab.toml`](examples/k8s-vcuda-lab.toml)).
2. Adjust `[host]`, `[k8s]`, `[harbor]`, and `[pipelines]` sections as needed.
3. Run:
   ```bash
   python3 /path/to/bench-dashboard/run.py --config /path/to/your-benchmark/bench-dashboard.toml --port 8787
   ```
4. For domain-specific panels (custom funnels, audit ledgers, projections), copy [`bench_dashboard/adapters/custom_template.py`](bench_dashboard/adapters/custom_template.py), implement `extend_snapshot(snap, cfg)`, and point `[pipelines] custom_adapter` at it.

## CLI Options & HTTP Endpoints

```text
python3 run.py [options]
  -c, --config PATH       Path to bench-dashboard.toml (auto-discovered if omitted)
  -t, --target PATH       Target benchmark repo path
  --host ADDR             Bind address (default: 127.0.0.1)
  --port PORT             HTTP port (default: 8787)
  --refresh-sec N         Background polling interval in seconds (default: 20)
  --no-k8s                Skip Kubernetes telemetry
  --cli                   Print terminal summary and exit
  --json                  Print raw JSON snapshot and exit
  --export PATH           Export standalone offline HTML snapshot and exit
```

HTTP endpoints when running the server:
- `GET /` — Single-file live Web UI
- `GET /api/snapshot` — Latest cached JSON snapshot (with rolling 1h `live_history`)
- `POST /api/refresh` — Trigger an immediate synchronous refresh and return JSON
- `GET /healthz` — Plain-text `ok` health probe

## Environment Variables

| Variable | Effect |
| --- | --- |
| `BENCH_DASH_DOCKER_HOST` / `DOCKER_HOST` | Override Docker daemon socket (e.g. `unix:///var/run/docker.sock`) |
| `KUBECONFIG` | Override path to Kubernetes config file (default `~/.kube/config`) |
| `K8S_NAMESPACE` | Override Kubernetes namespace |
| `K8S_USER_PREFIX` | Default owner prefix for distinguishing your pods/GPUs in a shared namespace |
| `K8S_API_SERVER` & `K8S_TOKEN` | Optional direct K8s REST auth without kubeconfig (`K8S_INSECURE_TLS=1` if self-signed) |

## Running Tests

```bash
python3 -m unittest discover -s tests -v
```

## License

MIT — see [LICENSE](LICENSE).
