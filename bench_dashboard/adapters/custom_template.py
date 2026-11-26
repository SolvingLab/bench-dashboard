"""Template for a project-specific read-only dashboard adapter.

Point `[pipelines] custom_adapter = "path/to/my_adapter.py"` in `bench-dashboard.toml`.
The dashboard automatically reloads the module whenever its file mtime changes.

Contract:
  Implement `extend_snapshot(snap: dict, cfg: dict) -> None`.
  You may append:
  - `snap["alerts"]`: list of `{"level": "warn"|"danger", "stage": str, "msg": str}`
  - `snap["custom"]["kpis"]`: list of `{"label": str, "value": str, "tag": str, "pct": float|None, "sub": str}`
  - `snap["custom"]["panels"]`: list of panels rendered automatically in the Overview / Pipeline view:
      * Bars panel:
        {"title": "...", "meta": "...", "type": "bars", "rows": [{"label": "A", "val": 12, "text": "12", "color": "var(--green)"}]}
      * Table panel:
        {"title": "...", "meta": "...", "type": "table", "columns": ["ID", "Status", "Note"], "rows": [["t1", "ok", "..."]]}
      * Key-value summary panel:
        {"title": "...", "meta": "...", "type": "kv", "rows": [{"k": "Pass Rate", "v": "64.2%"}]}
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


def extend_snapshot(snap: dict[str, Any], cfg: dict[str, Any]) -> None:
    """Example read-only hook (no-op by default; replace with your project's telemetry)."""
    repo = Path(cfg["project"]["target_repo"])
    _ = repo  # Replace with read-only reads against your ledgers/logs

    # Example KPI card:
    # snap["custom"]["kpis"].append({
    #     "label": "Custom Metric",
    #     "tag": "live",
    #     "value": "42 / 100",
    #     "pct": 42.0,
    #     "sub": "Read from project ledger",
    # })

    # Example Table panel:
    # snap["custom"]["panels"].append({
    #     "title": "Recent Audits",
    #     "meta": "last 20",
    #     "type": "table",
    #     "columns": ["Time", "Task", "Verdict"],
    #     "rows": [["16:40", "task-01", "APPROVE"]],
    # })
