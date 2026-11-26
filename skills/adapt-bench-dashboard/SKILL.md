---
name: adapt-bench-dashboard
description: Inspect a benchmark or evaluation repository and adapt SolvingLab/bench-dashboard (via bench-dashboard.toml and optional read-only Python adapter) to monitor host CPU/GPU, Kubernetes cluster, Harbor trials, and custom pipelines.
---

# Adapt `bench-dashboard` to a Benchmark Repository

Use this skill whenever a user wants a live monitoring dashboard (Web UI, CLI summary, or offline HTML snapshot) for a benchmark, evaluation suite, or data-generation pipeline.

## Hard Constraints

- **Strictly read-only**: Never modify tasks, ledgers, `dataset.toml`, `jobs/`, Docker containers, or Kubernetes resources.
- **Zero extra dependencies**: Run with any Python 3.11+ interpreter (`python3`); do not install npm packages or third-party pip libraries.
- **No secrets or private lab info in committed files**: Put custom sockets, namespaces, user prefixes, and mount paths in a local `bench-dashboard.toml` (gitignored) or environment variables.

## Step-by-Step Workflow

### Step 1 — Inspect the Target Benchmark Repository (Read-Only)

Run a single read-only discovery command against the target repo to map its structure:
- Task directories: `tasks/`, `tasks/dataset.toml`, extra pools (`lite/`, `candidates/`, `staging/`), layout depth (`tasks/*/*/task.toml` vs `tasks/*/task.toml`), and a sample `task.toml` to check `[metadata]` keys (`field`, `category`, `compute_tier`, `primary_metric`).
- Evaluation outputs: `jobs/` layout, sample `result.json` and `verifier/result.json` to see which metric keys exist (`reward`, `position_score`, `accuracy`, `pass_rate`, etc.) and whether scores are `0..1` or `0..100`.
- Host / Docker / GPU environment: check `DOCKER_HOST` or custom `/var/run/docker*.sock`, `nvidia-smi`, workspace & cold-storage disk mounts, and any slot lock dirs (e.g. `/tmp/*locks*`).
- Kubernetes environment (if used): check `~/.kube/config` or `$KUBECONFIG`, active namespace, pod name/label prefixes, GPU resource type (`nvidia.com/gpu` vs `tencent.com/vcuda-core`), and whether `HTTP_PROXY` blocks direct LAN cluster access (`strip_proxy_env = true`).
- Custom pipelines (if any): background runner scripts (`ps aux`), JSONL/TSV logs (build results, audit ledgers, candidate pools).

### Step 2 — Smoke-Test Zero-Config Mode

Before writing any config, verify what works automatically:

```bash
python3 <bench-dashboard>/run.py --target <target-repo> --cli
```

Note what was auto-detected and what is missing or misconfigured (e.g., wrong K8s namespace, custom Docker socket, extra task buckets, custom JSONL logs).

### Step 3 — Generate `bench-dashboard.toml`

Create `<target-repo>/bench-dashboard.toml` (and add it to `<target-repo>/.gitignore` if it contains personal/lab-specific paths) based on `config.example.toml`:

1. **`[project]`**: Set `name`, 2-char `badge`, `target_repo = "."`, `locale` (`"zh"` / `"en"` / `"auto"`).
2. **`[host]`**: Set `label` (friendly machine tag), `docker_host` (if non-default socket), and `[[host.disks]]` with `critical = true` on the workspace/Docker NVMe mount.
3. **`[k8s]`**:
   - Set `enabled = false` if the project doesn't use Kubernetes.
   - Otherwise set `namespace`, `owner_prefixes`, `workload_prefixes` / `workload_part_of`, `strip_proxy_env` (true if LAN API), and optional `[[k8s.shared_storage_paths]]`.
4. **`[harbor]`**:
   - Set `dataset_toml`, `jobs_dir`, `checksum_cache` (if present).
   - Set `score_metric_keys` to prioritize the project's primary verifier metric.
   - Define `[[harbor.buckets]]` if the repo uses non-standard task pool paths (including out-of-repo staging dirs via `root = "/path/to/staging"`).
   - Set `slot_lock_dir` and `slots_total` if a local slot dispatcher is used.
5. **`[pipelines]`**:
   - Add `[[pipelines.processes]]` for background daemons (builders, dispatchers, evaluators).
   - Add `[[pipelines.jsonl_streams]]` for append-only JSONL build/audit logs.
   - Configure `[pipelines.backlog]` if tracking remaining work from a candidate pool file vs completion ledger.

### Step 4 — Optional: Add a Read-Only Python Adapter (Only If Needed)

If the project has domain-specific metrics that TOML cannot express (for example: multi-stage paper funnels, custom audit pass rates, domain projections, or non-Harbor eval formats):

1. Copy `<bench-dashboard>/bench_dashboard/adapters/custom_template.py` to `<target-repo>/dashboard_adapter.py`.
2. Set `custom_adapter = "dashboard_adapter.py"` under `[pipelines]` in `bench-dashboard.toml`.
3. Implement `extend_snapshot(snap, cfg)` using strictly read-only file/process reads:
   - Append high-level cards to `snap["custom"]["kpis"]`.
   - Append rich visual panels (`type = "bars" | "table" | "kv"`) to `snap["custom"]["panels"]`.
   - Append domain-specific warnings to `snap["alerts"]`.
   - The adapter is hot-reloaded automatically on file edit (no server restart needed).

### Step 5 — Verify & Launch

Run all checks in one command:

```bash
python3 <bench-dashboard>/run.py --config <target-repo>/bench-dashboard.toml --cli \
  && python3 <bench-dashboard>/run.py --config <target-repo>/bench-dashboard.toml --export /tmp/bench-snapshot.html
```

Check that:
- `collect_sec` is fast (typically `< 3s`).
- No unexpected errors appear in `alerts` or `k8s.error`.
- Task counts, model scores, and GPU counts match ground truth.

Then give the user the exact launch command (or start it in background if requested):

```bash
# Local only
python3 <bench-dashboard>/run.py --config <target-repo>/bench-dashboard.toml --port 8787

# Accessible over LAN
python3 <bench-dashboard>/run.py --config <target-repo>/bench-dashboard.toml --host 0.0.0.0 --port 8787
```
