# Monitoring Guide

## How to Monitor the Pipeline

---

## Dashboard (Streamlit)

Start the dashboard:
```bash
py -3 -m streamlit run src/monitor/dashboard.py --server.port 8501
```

Open http://localhost:8501

### Dashboard Pages

**Overview** — At-a-glance metrics:
- Total pipeline runs and success count
- Active alerts
- Data files per layer (Bronze/Silver/Gold)
- Recent pipeline runs table
- Recent alerts

**Pipeline Runs** — Filterable history of all pipeline executions:
- Filter by status (success/failed) and layer (bronze/silver/gold)
- Status distribution chart

**Data Quality** — Quality scores per table:
- Progress bars showing current scores
- Letter grades
- Dimension breakdowns (Completeness, Accuracy, Consistency, Timeliness)
- Links to generated quality reports

**Health Checks** — Run live infrastructure checks:
- Disk space
- Database accessibility
- Data directory presence
- Memory availability
- Log directory health

**Alerts** — Full alert log with severity filtering:
- CRITICAL (red) — Pipeline stopped or quality failed
- WARNING (yellow) — Issues that need attention
- INFO (blue) — Informational notices

---

## Alert Severity Levels

| Level | Meaning | Action Required |
|-------|---------|----------------|
| **CRITICAL** | Pipeline halted or data quality unacceptable | Investigate immediately |
| **WARNING** | Degraded but functional | Review within 24 hours |
| **INFO** | Informational, no action needed | Review at convenience |

---

## Log Files

All logs are stored in the `logs/` directory:

| File | Contents |
|------|----------|
| `fittrack.log` | Structured JSON logs from all pipeline components |
| `alerts.json` | Alert history (last 1000 alerts) |
| `pipeline_run_*.json` | Detailed results from each pipeline execution |

### Reading JSON Logs

Each log line is a JSON object:
```json
{
  "timestamp": "2026-02-26T06:00:00+00:00",
  "level": "INFO",
  "logger": "fittrack.ingestion.api",
  "message": "Extracted 890 records with 12 columns from exercises",
  "module": "api_ingestor",
  "source": "wger_exercises",
  "layer": "bronze"
}
```

Filter logs by level:
```bash
# Show only errors
py -3 -c "import json; [print(json.dumps(l)) for l in (json.loads(line) for line in open('logs/fittrack.log')) if l.get('level') == 'ERROR']"
```

---

## Health Checks

The pipeline runs health checks before every execution:

| Check | What It Verifies | Threshold |
|-------|-----------------|-----------|
| Disk Space | Free space on data drive | > 1 GB |
| Database | DuckDB file accessible and queryable | Connection success |
| Data Dirs | bronze/, silver/, gold/, incoming/ exist | All present |
| Memory | Available system memory | > 500 MB |
| Log Dir | Log directory writable, not too large | Writable |

Run health checks manually:
```python
from src.monitor.health_check import HealthChecker
checker = HealthChecker()
for r in checker.run_all_checks():
    status = "OK" if r.healthy else "FAIL"
    print(f"[{status}] {r.name}: {r.message}")
```

---

## Scheduling

The pipeline can run on a schedule (disabled by default). Enable in `config/pipeline_config.yaml`:

```yaml
scheduling:
  enabled: true
  full_pipeline_cron: "0 6 * * *"      # Daily at 6 AM
  quality_check_cron: "0 */4 * * *"    # Every 4 hours
  health_check_cron: "*/15 * * * *"    # Every 15 minutes
```

Start the scheduler:
```python
from src.monitor.scheduler import PipelineScheduler
scheduler = PipelineScheduler()
scheduler.start()  # Blocks and runs scheduled jobs
```
