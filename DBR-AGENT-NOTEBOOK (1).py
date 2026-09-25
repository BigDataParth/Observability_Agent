# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks Observability AI Agent — v4.2 (Production + Dynamic SQL)
# MAGIC
# MAGIC Natural-language operational intelligence over Databricks system tables.
# MAGIC
# MAGIC **What's in v4.2 (= v4_1 + Demo_10June SQL-gen fallback):**
# MAGIC - 28 pre-validated query templates (fast, deterministic path)
# MAGIC - NEW: Dynamic SQL generation fallback for questions outside the 28 templates
# MAGIC - Severity scoring (CRITICAL / HIGH / MEDIUM / LOW / INFO)
# MAGIC - Structured JSON-only output for Power Automate (unchanged contract)
# MAGIC - Multi-step reasoning (up to 5 steps) with full result persistence
# MAGIC - Secrets-based token (no hardcoding)
# MAGIC - Widget-driven catalog and configuration
# MAGIC - Spark AQE + broadcast hints + query watchdog
# MAGIC - DOWNLOAD_REPORT sentinel preserved for Power Automate download path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 — Widgets & Configuration

# COMMAND ----------

dbutils.widgets.text("question", "What's the overall health of the workspace?", "1. Question")
dbutils.widgets.dropdown("days", "30", ["7", "14", "30", "60", "90"], "2. Lookback days")
dbutils.widgets.dropdown("catalog", "system", ["system", "system_catalog"], "3. System catalog name")
dbutils.widgets.text("secret_scope", "", "4. Secret scope (optional, blank = use notebook token)")
dbutils.widgets.text("secret_key", "", "5. Secret key (optional, blank = use notebook token)")
dbutils.widgets.dropdown("llm_endpoint", "databricks-claude-haiku-4-5",
                        ["databricks-claude-haiku-4-5", "databricks-claude-sonnet-4-5",
                         "databricks-meta-llama-3-3-70b-instruct"], "6. LLM endpoint")
dbutils.widgets.dropdown("max_steps", "5", ["1", "2", "3", "5"], "7. Max reasoning steps")
dbutils.widgets.dropdown("enable_sql_gen", "Yes", ["Yes", "No"], "8. Enable dynamic SQL fallback")

# COMMAND ----------

import json
import time
import re
import requests
from datetime import datetime, timezone
from functools import lru_cache
from mlflow.deployments import get_deploy_client

# Read widgets
CATALOG          = dbutils.widgets.get("catalog")
DEFAULT_DAYS     = int(dbutils.widgets.get("days"))
LLM_ENDPOINT     = dbutils.widgets.get("llm_endpoint")
MAX_REASONING_STEPS = int(dbutils.widgets.get("max_steps"))
SECRET_SCOPE     = dbutils.widgets.get("secret_scope")
SECRET_KEY       = dbutils.widgets.get("secret_key")
ENABLE_SQL_GEN   = dbutils.widgets.get("enable_sql_gen").strip().lower() == "yes"

# Hard limits
MAX_ROWS_FOR_LLM = 15
QUERY_TIMEOUT_SEC = 60

# Severity thresholds (tune later via widgets if needed)
SEVERITY_THRESHOLDS = {
    "cost_anomaly":        {"z_score": 3.0, "pct_change": 50},   # >3 sigma or +50% = HIGH
    "repeated_failures":   {"streak": 5, "streak_days": 3},
    "idle_clusters":       {"days_idle": 14},
    "oversized_clusters":  {"avg_cpu": 10, "avg_mem": 20},
    "sla_breach":          {"multiplier": 3.0},
    "failure_rate":        {"pct": 25.0},
    "abandoned_tables":    {"days_unused": 60},
    "sql_warehouse_health":{"queue_pct": 30},
}

# Token sourcing — prefer secret scope if configured, else use notebook context (no hardcoding)
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
WORKSPACE_URL = ctx.apiUrl().get()

DATABRICKS_TOKEN = None
TOKEN_SOURCE = None

# Try secret scope only if user has set non-default values
if SECRET_SCOPE and SECRET_KEY:
    try:
        DATABRICKS_TOKEN = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
        TOKEN_SOURCE = f"secret:{SECRET_SCOPE}/{SECRET_KEY}"
    except Exception:
        DATABRICKS_TOKEN = None

# Fallback: notebook context token (works without any setup)
if not DATABRICKS_TOKEN:
    DATABRICKS_TOKEN = ctx.apiToken().get()
    TOKEN_SOURCE = "notebook_context"

llm_client = get_deploy_client("databricks")

print(f"Catalog       : {CATALOG}")
print(f"LLM Endpoint  : {LLM_ENDPOINT}")
print(f"Lookback      : {DEFAULT_DAYS} days")
print(f"Max Steps     : {MAX_REASONING_STEPS}")
print(f"Token Source  : {TOKEN_SOURCE}")
print(f"Workspace URL : {WORKSPACE_URL}")
print(f"SQL Gen       : {'ENABLED' if ENABLE_SQL_GEN else 'DISABLED'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 — Spark Optimizations

# COMMAND ----------

# Enable Adaptive Query Execution + dynamic optimizations
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(50 * 1024 * 1024))  # 50MB broadcast
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.databricks.queryWatchdog.enabled", "true")
spark.conf.set("spark.databricks.queryWatchdog.maxQueryTasks", "20000")

print("Spark optimizations applied (AQE, skew handling, broadcast, watchdog).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 — Query Templates Library (28 templates)

# COMMAND ----------

QUERY_TEMPLATES = [
    # ============================================================
    # ORIGINAL v2 TEMPLATES (11)
    # ============================================================
    {
        "intent": "job_failures",
        "description": "Recent job failures with error codes and termination reasons",
        "keywords": ["fail", "failed", "failure", "error", "broken", "crash", "issue", "wrong"],
        "query": """
            SELECT
              j.name AS job_name, t.job_id, t.run_id, t.result_state,
              t.termination_code, t.period_start_time AS failed_at,
              ROUND((unix_timestamp(t.period_end_time) - unix_timestamp(t.period_start_time))/60.0, 2) AS duration_min
            FROM {CATALOG}.lakeflow.job_task_run_timeline t
            LEFT JOIN (
              SELECT workspace_id, job_id, name FROM {CATALOG}.lakeflow.jobs
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
            ) j ON j.job_id = t.job_id
            WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
              AND t.result_state IN ('FAILED', 'ERROR', 'TIMED_OUT')
              AND t.period_end_time IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ORDER BY t.period_start_time DESC LIMIT 20
        """
    },
    {
        "intent": "repeated_failures",
        "description": "Jobs failing multiple times consecutively — broken pipelines nobody fixed",
        "keywords": ["repeat", "recurring", "again", "keep failing", "consecutive", "streak", "broken pipeline"],
        "query": """
            WITH task_runs AS (
              SELECT t.job_id, t.run_id, t.result_state, t.period_start_time
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.period_end_time IS NOT NULL AND t.result_state IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ),
            run_level AS (
              SELECT job_id, run_id, MIN(period_start_time) AS run_started_at,
                CASE
                  WHEN MIN(CASE WHEN result_state='FAILED' THEN 0 ELSE 1 END)=0 THEN 'FAILED'
                  WHEN MIN(CASE WHEN result_state='ERROR' THEN 0 ELSE 1 END)=0 THEN 'ERROR'
                  WHEN MIN(CASE WHEN result_state='TIMED_OUT' THEN 0 ELSE 1 END)=0 THEN 'TIMED_OUT'
                  ELSE 'SUCCEEDED'
                END AS run_state,
                ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY MIN(period_start_time) DESC) AS run_rank
              FROM task_runs GROUP BY job_id, run_id
            ),
            with_success AS (
              SELECT *, MIN(CASE WHEN run_state='SUCCEEDED' THEN run_rank END) OVER (PARTITION BY job_id) AS first_success
              FROM run_level
            ),
            streaks AS (
              SELECT job_id, COUNT(*) AS consecutive_failures,
                MIN(run_started_at) AS streak_started_at, DATEDIFF(NOW(), MIN(run_started_at)) AS streak_days
              FROM with_success
              WHERE run_state IN ('FAILED','ERROR','TIMED_OUT') AND run_rank < COALESCE(first_success, 999)
              GROUP BY job_id HAVING COUNT(*) >= 2
            )
            SELECT /*+ BROADCAST(j) */ j.name AS job_name, s.job_id, s.consecutive_failures,
              s.streak_started_at, s.streak_days
            FROM streaks s
            LEFT JOIN (
              SELECT workspace_id, job_id, name FROM {CATALOG}.lakeflow.jobs
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
            ) j ON j.job_id = s.job_id
            ORDER BY s.consecutive_failures DESC LIMIT 20
        """
    },
    {
        "intent": "cost_by_team",
        "description": "Cost breakdown by team — which team is spending the most",
        "keywords": ["cost", "spend", "expensive", "team cost", "billing", "money", "budget", "price"],
        "query": """
            WITH latest_clusters AS (
              SELECT cluster_id, tags['team'] AS team FROM {CATALOG}.compute.clusters
              WHERE tags['team'] IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            ),
            usage_flat AS (
              SELECT CAST(usage_metadata['cluster_id'] AS STRING) AS cluster_id,
                usage_date, cloud, sku_name, usage_start_time, usage_end_time,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND usage_metadata['cluster_id'] IS NOT NULL
            ),
            priced AS (
              SELECT /*+ BROADCAST(lp) */ uf.cluster_id,
                SUM(uf.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM usage_flat uf
              JOIN {CATALOG}.billing.list_prices lp
                ON uf.cloud=lp.cloud AND uf.sku_name=lp.sku_name
                AND uf.usage_start_time >= lp.price_start_time
                AND (uf.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY uf.cluster_id
            )
            SELECT /*+ BROADCAST(c) */ c.team, ROUND(SUM(p.cost_usd),2) AS total_cost_usd,
              ROUND(SUM(p.cost_usd)/COUNT(DISTINCT c.cluster_id),2) AS avg_cost_per_cluster
            FROM priced p JOIN latest_clusters c ON p.cluster_id=c.cluster_id
            GROUP BY c.team ORDER BY total_cost_usd DESC LIMIT 50
        """
    },
    {
        "intent": "cost_trend",
        "description": "Daily cost trend over time — is spending going up or down",
        "keywords": ["trend", "daily cost", "day by day", "cost over time", "cost history", "cost growth"],
        "query": """
            WITH usage_flat AS (
              SELECT usage_date, cloud, sku_name, usage_start_time, usage_end_time,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
            )
            SELECT /*+ BROADCAST(lp) */ uf.usage_date,
              ROUND(SUM(uf.dbus),2) AS total_dbus,
              ROUND(SUM(uf.dbus * CAST(lp.pricing.default AS DOUBLE)),2) AS total_cost_usd
            FROM usage_flat uf
            JOIN {CATALOG}.billing.list_prices lp
              ON uf.cloud=lp.cloud AND uf.sku_name=lp.sku_name
              AND uf.usage_start_time >= lp.price_start_time
              AND (uf.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
            GROUP BY uf.usage_date ORDER BY uf.usage_date DESC LIMIT 90
        """
    },
    {
        "intent": "slowest_jobs",
        "description": "Top slowest running jobs by average duration",
        "keywords": ["slow", "longest", "time", "duration", "taking long", "slowest", "performance"],
        "query": """
            WITH run_durations AS (
              SELECT t.job_id, t.run_id,
                SUM(unix_timestamp(t.period_end_time)-unix_timestamp(t.period_start_time)) AS dur_sec
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.period_end_time IS NOT NULL AND t.result_state IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id, t.task_key ORDER BY t.period_start_time DESC) = 1
              GROUP BY t.job_id, t.run_id
            )
            SELECT /*+ BROADCAST(j) */ j.name AS job_name, d.job_id, COUNT(*) AS total_runs,
              ROUND(AVG(d.dur_sec)/3600.0,2) AS avg_hours, ROUND(MAX(d.dur_sec)/3600.0,2) AS max_hours
            FROM run_durations d
            LEFT JOIN (
              SELECT workspace_id, job_id, name FROM {CATALOG}.lakeflow.jobs
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
            ) j ON j.job_id = d.job_id
            GROUP BY j.name, d.job_id ORDER BY avg_hours DESC LIMIT 20
        """
    },
    {
        "intent": "idle_clusters",
        "description": "Clusters with no recent job activity — wasted cost",
        "keywords": ["idle", "unused", "inactive", "zombie", "not used", "wasted", "no activity"],
        "query": """
            WITH latest_clusters AS (
              SELECT cluster_id, cluster_name, owned_by, tags['team'] AS team
              FROM {CATALOG}.compute.clusters
              WHERE cluster_source IN ('UI','API') AND delete_time IS NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            ),
            last_activity AS (
              SELECT c.cluster_id, MAX(t.period_end_time) AS last_job_at
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              JOIN latest_clusters c ON array_contains(t.compute_ids, c.cluster_id)
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
              GROUP BY c.cluster_id
            )
            SELECT c.cluster_id, c.cluster_name, c.owned_by, c.team, la.last_job_at,
              DATEDIFF(NOW(), la.last_job_at) AS days_since_last_job
            FROM latest_clusters c
            LEFT JOIN last_activity la ON c.cluster_id = la.cluster_id
            WHERE la.last_job_at IS NULL OR DATEDIFF(NOW(), la.last_job_at) >= 7
            ORDER BY days_since_last_job DESC LIMIT 50
        """
    },
    {
        "intent": "failure_rate",
        "description": "Day by day job failure ratio — reliability trend",
        "keywords": ["failure rate", "success rate", "ratio", "percentage", "health", "reliability"],
        "query": """
            WITH task_runs AS (
              SELECT t.job_id, t.run_id, t.result_state, t.period_start_time
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.period_end_time IS NOT NULL AND t.result_state IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ),
            run_level AS (
              SELECT job_id, run_id, to_date(MIN(period_start_time)) AS run_date,
                CASE
                  WHEN MIN(CASE WHEN result_state='FAILED' THEN 0 ELSE 1 END)=0 THEN 'FAILED'
                  WHEN MIN(CASE WHEN result_state='ERROR' THEN 0 ELSE 1 END)=0 THEN 'ERROR'
                  WHEN MIN(CASE WHEN result_state='TIMED_OUT' THEN 0 ELSE 1 END)=0 THEN 'TIMED_OUT'
                  ELSE 'SUCCEEDED'
                END AS run_state
              FROM task_runs GROUP BY job_id, run_id
            )
            SELECT run_date, COUNT(*) AS total_runs,
              SUM(CASE WHEN run_state='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
              SUM(CASE WHEN run_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
              ROUND(100.0*SUM(CASE WHEN run_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS failure_rate_pct
            FROM run_level GROUP BY run_date ORDER BY run_date DESC LIMIT 90
        """
    },
    {
        "intent": "sla_breach",
        "description": "Jobs running slower than their historical baseline — silent degradation",
        "keywords": ["sla", "breach", "degradation", "slower", "getting slow", "baseline", "anomaly"],
        "query": """
            WITH run_durations AS (
              SELECT t.job_id, t.run_id,
                ROUND(SUM(unix_timestamp(t.period_end_time)-unix_timestamp(t.period_start_time))/60.0,2) AS dur_min,
                MIN(t.period_start_time) AS run_started_at
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -90, NOW())
                AND t.period_end_time IS NOT NULL AND t.result_state IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
              GROUP BY t.job_id, t.run_id
            ),
            baseline AS (
              SELECT job_id, ROUND(AVG(dur_min),2) AS avg_min, COUNT(*) AS runs
              FROM run_durations WHERE run_started_at < DATEADD(DAY, -{DAYS}, NOW())
              GROUP BY job_id HAVING COUNT(*) >= 5
            ),
            recent AS (SELECT * FROM run_durations WHERE run_started_at >= DATEADD(DAY, -{DAYS}, NOW()))
            SELECT /*+ BROADCAST(j, b) */ j.name AS job_name, r.job_id, b.avg_min AS baseline_avg_min,
              ROUND(MAX(r.dur_min),2) AS worst_recent_min,
              ROUND(MAX(r.dur_min)/b.avg_min,2) AS multiplier
            FROM recent r JOIN baseline b ON r.job_id=b.job_id
            LEFT JOIN (
              SELECT workspace_id, job_id, name FROM {CATALOG}.lakeflow.jobs
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
            ) j ON j.job_id=r.job_id
            WHERE r.dur_min > b.avg_min * 1.5
            GROUP BY j.name, r.job_id, b.avg_min ORDER BY multiplier DESC LIMIT 20
        """
    },
    {
        "intent": "oversized_clusters",
        "description": "Clusters with low CPU/memory utilization — wasting money on oversized compute",
        "keywords": ["oversize", "underutilized", "cpu low", "memory low", "right size", "downsize", "optimize cluster"],
        "query": """
            WITH latest_clusters AS (
              SELECT cluster_id, cluster_name, owned_by, worker_count,
                max_autoscale_workers, worker_node_type, tags['team'] AS team
              FROM {CATALOG}.compute.clusters
              WHERE cluster_source IN ('UI','API') AND delete_time IS NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            ),
            node_metrics AS (
              SELECT n.cluster_id, ROUND(AVG(n.cpu_user_percent),2) AS avg_cpu,
                ROUND(AVG(n.mem_used_percent),2) AS avg_mem, COUNT(*) AS samples
              FROM {CATALOG}.compute.node_timeline n
              WHERE n.start_time >= DATEADD(DAY, -{DAYS}, NOW())
              GROUP BY n.cluster_id HAVING COUNT(*) >= 10
            )
            SELECT /*+ BROADCAST(c) */ c.cluster_name, c.owned_by, c.team, c.worker_node_type,
              COALESCE(c.worker_count, c.max_autoscale_workers) AS max_workers,
              nm.avg_cpu, nm.avg_mem, nm.samples
            FROM latest_clusters c JOIN node_metrics nm ON c.cluster_id=nm.cluster_id
            WHERE nm.avg_cpu < 20 AND nm.avg_mem < 30 ORDER BY nm.avg_cpu ASC LIMIT 50
        """
    },
    {
        "intent": "job_status",
        "description": "Recent job run status with duration and error details",
        "keywords": ["status", "what happened", "last run", "recent run", "check job", "job run"],
        "query": """
            WITH task_runs AS (
              SELECT t.job_id, t.run_id, t.result_state, t.period_start_time,
                t.period_end_time, t.termination_code
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.result_state IS NOT NULL AND t.period_end_time IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ),
            run_level AS (
              SELECT job_id, run_id, MIN(period_start_time) AS started_at, MAX(period_end_time) AS ended_at,
                ROUND((unix_timestamp(MAX(period_end_time))-unix_timestamp(MIN(period_start_time)))/60.0,2) AS dur_min,
                CASE
                  WHEN MIN(CASE WHEN result_state='FAILED' THEN 0 ELSE 1 END)=0 THEN 'FAILED'
                  WHEN MIN(CASE WHEN result_state='ERROR' THEN 0 ELSE 1 END)=0 THEN 'ERROR'
                  WHEN MIN(CASE WHEN result_state='TIMED_OUT' THEN 0 ELSE 1 END)=0 THEN 'TIMED_OUT'
                  ELSE 'SUCCEEDED'
                END AS run_state, MAX(termination_code) AS termination_code
              FROM task_runs GROUP BY job_id, run_id
            )
            SELECT /*+ BROADCAST(j) */ j.name AS job_name, r.job_id, r.run_id, r.run_state,
              r.termination_code, r.started_at, r.ended_at, r.dur_min
            FROM run_level r
            LEFT JOIN (
              SELECT workspace_id, job_id, name FROM {CATALOG}.lakeflow.jobs
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
            ) j ON j.job_id=r.job_id
            ORDER BY r.started_at DESC LIMIT 20
        """
    },
    {
        "intent": "workspace_summary",
        "description": "Overall workspace health — total jobs, success rate, failure rate",
        "keywords": ["summary", "overview", "health", "how are things", "what's happening", "dashboard", "report"],
        "query": """
            WITH task_runs AS (
              SELECT t.job_id, t.run_id, t.result_state
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.result_state IS NOT NULL AND t.period_end_time IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ),
            run_level AS (
              SELECT job_id, run_id,
                CASE
                  WHEN MIN(CASE WHEN result_state='FAILED' THEN 0 ELSE 1 END)=0 THEN 'FAILED'
                  WHEN MIN(CASE WHEN result_state='ERROR' THEN 0 ELSE 1 END)=0 THEN 'ERROR'
                  WHEN MIN(CASE WHEN result_state='TIMED_OUT' THEN 0 ELSE 1 END)=0 THEN 'TIMED_OUT'
                  ELSE 'SUCCEEDED'
                END AS run_state
              FROM task_runs GROUP BY job_id, run_id
            )
            SELECT COUNT(DISTINCT job_id) AS total_jobs, COUNT(DISTINCT run_id) AS total_runs,
              SUM(CASE WHEN run_state='SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
              SUM(CASE WHEN run_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END) AS failed,
              ROUND(100.0*SUM(CASE WHEN run_state='SUCCEEDED' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS success_rate_pct,
              ROUND(100.0*SUM(CASE WHEN run_state IN ('FAILED','ERROR','TIMED_OUT') THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),2) AS failure_rate_pct
            FROM run_level
        """
    },

    # ============================================================
    # PHASE 1 — COST INTELLIGENCE (6 new)
    # ============================================================
    {
        "intent": "cost_anomaly",
        "description": "Detects daily cost spikes vs 14-day rolling baseline (z-score anomaly detection)",
        "keywords": ["spike", "anomaly", "unusual", "sudden", "jump", "abnormal cost", "cost spike", "why expensive"],
        "query": """
            WITH usage_flat AS (
              SELECT usage_date, cloud, sku_name, usage_start_time, usage_end_time,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
            ),
            daily_cost AS (
              SELECT /*+ BROADCAST(lp) */ uf.usage_date,
                ROUND(SUM(uf.dbus * CAST(lp.pricing.default AS DOUBLE)),2) AS cost_usd
              FROM usage_flat uf
              JOIN {CATALOG}.billing.list_prices lp
                ON uf.cloud=lp.cloud AND uf.sku_name=lp.sku_name
                AND uf.usage_start_time >= lp.price_start_time
                AND (uf.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY uf.usage_date
            ),
            stats AS (
              SELECT usage_date, cost_usd,
                AVG(cost_usd) OVER (ORDER BY usage_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS baseline_avg,
                STDDEV(cost_usd) OVER (ORDER BY usage_date ROWS BETWEEN 14 PRECEDING AND 1 PRECEDING) AS baseline_std
              FROM daily_cost
            )
            SELECT usage_date, cost_usd,
              ROUND(baseline_avg,2) AS baseline_avg_usd,
              ROUND(cost_usd - baseline_avg, 2) AS delta_usd,
              ROUND(100.0*(cost_usd - baseline_avg)/NULLIF(baseline_avg,0), 2) AS pct_change,
              ROUND((cost_usd - baseline_avg)/NULLIF(baseline_std,0), 2) AS z_score
            FROM stats
            WHERE baseline_avg IS NOT NULL
              AND ABS((cost_usd - baseline_avg)/NULLIF(baseline_std,0)) >= 2
            ORDER BY usage_date DESC LIMIT 20
        """
    },
    {
        "intent": "cost_by_sku",
        "description": "Cost split by SKU type — Jobs vs All-Purpose vs SQL vs DLT vs Serverless",
        "keywords": ["sku", "breakdown", "split", "by type", "compute type", "where is money going"],
        "query": """
            WITH usage_flat AS (
              SELECT sku_name, cloud, usage_start_time, usage_end_time, usage_date,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
            ),
            priced AS (
              SELECT /*+ BROADCAST(lp) */ uf.sku_name,
                SUM(uf.dbus) AS total_dbus,
                SUM(uf.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM usage_flat uf
              JOIN {CATALOG}.billing.list_prices lp
                ON uf.cloud=lp.cloud AND uf.sku_name=lp.sku_name
                AND uf.usage_start_time >= lp.price_start_time
                AND (uf.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY uf.sku_name
            )
            SELECT sku_name,
              CASE
                WHEN sku_name LIKE '%SERVERLESS%' THEN 'Serverless'
                WHEN sku_name LIKE '%JOBS%' THEN 'Jobs Compute'
                WHEN sku_name LIKE '%ALL_PURPOSE%' THEN 'All-Purpose'
                WHEN sku_name LIKE '%SQL%' THEN 'SQL Warehouse'
                WHEN sku_name LIKE '%DLT%' OR sku_name LIKE '%PIPELINE%' THEN 'DLT Pipelines'
                WHEN sku_name LIKE '%MODEL%' THEN 'Model Serving'
                ELSE 'Other'
              END AS compute_category,
              ROUND(total_dbus, 2) AS total_dbus,
              ROUND(cost_usd, 2) AS cost_usd
            FROM priced
            ORDER BY cost_usd DESC LIMIT 30
        """
    },
    {
        "intent": "serverless_cost",
        "description": "Serverless compute cost breakdown by warehouse/job — usually a hidden cost source",
        "keywords": ["serverless", "warehouse cost", "serverless spend"],
        "query": """
            WITH serverless_usage AS (
              SELECT usage_date, sku_name, cloud, usage_start_time, usage_end_time,
                usage_metadata['warehouse_id'] AS warehouse_id,
                usage_metadata['job_id'] AS job_id,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND sku_name LIKE '%SERVERLESS%'
            ),
            priced AS (
              SELECT /*+ BROADCAST(lp) */ su.sku_name, su.warehouse_id, su.job_id,
                SUM(su.dbus) AS total_dbus,
                SUM(su.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM serverless_usage su
              JOIN {CATALOG}.billing.list_prices lp
                ON su.cloud=lp.cloud AND su.sku_name=lp.sku_name
                AND su.usage_start_time >= lp.price_start_time
                AND (su.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY su.sku_name, su.warehouse_id, su.job_id
            )
            SELECT sku_name,
              COALESCE(warehouse_id, job_id, 'unattributed') AS resource_id,
              CASE WHEN warehouse_id IS NOT NULL THEN 'SQL Warehouse'
                   WHEN job_id IS NOT NULL THEN 'Job'
                   ELSE 'Other' END AS resource_type,
              ROUND(total_dbus, 2) AS total_dbus,
              ROUND(cost_usd, 2) AS cost_usd
            FROM priced
            ORDER BY cost_usd DESC LIMIT 30
        """
    },
    {
        "intent": "photon_adoption",
        "description": "Clusters NOT using Photon — missed performance and cost savings (Photon is ~2x faster)",
        "keywords": ["photon", "not using photon", "photon optimization"],
        "query": """
            WITH latest_clusters AS (
              SELECT cluster_id, cluster_name, owned_by, tags['team'] AS team,
                runtime_engine, dbr_version, worker_node_type
              FROM {CATALOG}.compute.clusters
              WHERE cluster_source IN ('UI','API') AND delete_time IS NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            ),
            cluster_usage AS (
              SELECT CAST(usage_metadata['cluster_id'] AS STRING) AS cluster_id,
                cloud, sku_name, usage_start_time, usage_end_time,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND usage_metadata['cluster_id'] IS NOT NULL
            ),
            cluster_cost AS (
              SELECT /*+ BROADCAST(lp) */ cu.cluster_id,
                SUM(cu.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM cluster_usage cu
              JOIN {CATALOG}.billing.list_prices lp
                ON cu.cloud=lp.cloud AND cu.sku_name=lp.sku_name
                AND cu.usage_start_time >= lp.price_start_time
                AND (cu.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY cu.cluster_id
            )
            SELECT /*+ BROADCAST(c) */ c.cluster_name, c.owned_by, c.team, c.worker_node_type,
              c.runtime_engine, c.dbr_version,
              ROUND(cc.cost_usd, 2) AS cost_usd_period
            FROM latest_clusters c
            JOIN cluster_cost cc ON c.cluster_id = cc.cluster_id
            WHERE (c.runtime_engine IS NULL OR UPPER(c.runtime_engine) != 'PHOTON')
              AND cc.cost_usd > 10
            ORDER BY cc.cost_usd DESC LIMIT 30
        """
    },
    {
        "intent": "spot_vs_ondemand",
        "description": "Clusters running fully on-demand when spot instances would save 60-90% on workers",
        "keywords": ["spot", "on demand", "on-demand", "preemptible", "save money"],
        "query": """
            WITH latest_clusters AS (
              SELECT cluster_id, cluster_name, owned_by, tags['team'] AS team,
                aws_attributes, azure_attributes, gcp_attributes, worker_node_type
              FROM {CATALOG}.compute.clusters
              WHERE cluster_source IN ('UI','API') AND delete_time IS NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            ),
            cluster_usage AS (
              SELECT CAST(usage_metadata['cluster_id'] AS STRING) AS cluster_id,
                cloud, sku_name, usage_start_time, usage_end_time,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND usage_metadata['cluster_id'] IS NOT NULL
            ),
            cluster_cost AS (
              SELECT /*+ BROADCAST(lp) */ cu.cluster_id,
                SUM(cu.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM cluster_usage cu
              JOIN {CATALOG}.billing.list_prices lp
                ON cu.cloud=lp.cloud AND cu.sku_name=lp.sku_name
                AND cu.usage_start_time >= lp.price_start_time
                AND (cu.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY cu.cluster_id
            )
            SELECT /*+ BROADCAST(c) */ c.cluster_name, c.owned_by, c.team, c.worker_node_type,
              CASE
                WHEN c.aws_attributes IS NOT NULL THEN CAST(c.aws_attributes['first_on_demand'] AS INT)
                WHEN c.azure_attributes IS NOT NULL THEN CAST(c.azure_attributes['first_on_demand'] AS INT)
                WHEN c.gcp_attributes IS NOT NULL THEN CAST(c.gcp_attributes['first_on_demand'] AS INT)
              END AS on_demand_workers,
              CASE
                WHEN c.aws_attributes IS NOT NULL THEN c.aws_attributes['availability']
                WHEN c.azure_attributes IS NOT NULL THEN c.azure_attributes['availability']
                WHEN c.gcp_attributes IS NOT NULL THEN c.gcp_attributes['availability']
              END AS availability,
              ROUND(cc.cost_usd, 2) AS cost_usd_period,
              ROUND(cc.cost_usd * 0.5, 2) AS potential_savings_usd
            FROM latest_clusters c
            JOIN cluster_cost cc ON c.cluster_id = cc.cluster_id
            WHERE cc.cost_usd > 50
              AND (
                (c.aws_attributes IS NOT NULL AND COALESCE(c.aws_attributes['availability'], 'ON_DEMAND') NOT LIKE '%SPOT%')
                OR (c.azure_attributes IS NOT NULL AND COALESCE(c.azure_attributes['availability'], 'ON_DEMAND') NOT LIKE '%SPOT%')
                OR (c.gcp_attributes IS NOT NULL AND COALESCE(c.gcp_attributes['availability'], 'ON_DEMAND') NOT LIKE '%SPOT%')
              )
            ORDER BY cc.cost_usd DESC LIMIT 30
        """
    },
    {
        "intent": "cost_forecast",
        "description": "Month-to-date spend with linear projection to end of month vs prior month",
        "keywords": ["forecast", "projection", "month end", "mtd", "burn rate", "monthly spend"],
        "query": """
            WITH usage_flat AS (
              SELECT usage_date, cloud, sku_name, usage_start_time, usage_end_time,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU'
                AND usage_date >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
            ),
            daily_cost AS (
              SELECT /*+ BROADCAST(lp) */ uf.usage_date,
                SUM(uf.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM usage_flat uf
              JOIN {CATALOG}.billing.list_prices lp
                ON uf.cloud=lp.cloud AND uf.sku_name=lp.sku_name
                AND uf.usage_start_time >= lp.price_start_time
                AND (uf.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY uf.usage_date
            ),
            current_month AS (
              SELECT SUM(cost_usd) AS mtd_cost_usd,
                COUNT(*) AS days_elapsed,
                MAX(usage_date) AS latest_date
              FROM daily_cost
              WHERE usage_date >= DATE_TRUNC('MONTH', CURRENT_DATE())
            ),
            prior_month AS (
              SELECT SUM(cost_usd) AS prior_total_usd
              FROM daily_cost
              WHERE usage_date >= DATE_TRUNC('MONTH', DATEADD(MONTH, -1, CURRENT_DATE()))
                AND usage_date < DATE_TRUNC('MONTH', CURRENT_DATE())
            )
            SELECT
              ROUND(cm.mtd_cost_usd, 2) AS mtd_cost_usd,
              cm.days_elapsed,
              ROUND(cm.mtd_cost_usd / cm.days_elapsed, 2) AS daily_avg_usd,
              DAY(LAST_DAY(CURRENT_DATE())) AS days_in_month,
              ROUND((cm.mtd_cost_usd / cm.days_elapsed) * DAY(LAST_DAY(CURRENT_DATE())), 2) AS projected_month_end_usd,
              ROUND(pm.prior_total_usd, 2) AS prior_month_usd,
              ROUND(100.0 * ((cm.mtd_cost_usd / cm.days_elapsed) * DAY(LAST_DAY(CURRENT_DATE())) - pm.prior_total_usd) / NULLIF(pm.prior_total_usd, 0), 2) AS pct_vs_prior_month
            FROM current_month cm CROSS JOIN prior_month pm
        """
    },

    # ============================================================
    # PHASE 2 — SQL + STORAGE HEALTH (6 new)
    # ============================================================
    {
        "intent": "sql_warehouse_health",
        "description": "SQL warehouse health — utilization, queue time, auto-stop config",
        "keywords": ["warehouse", "sql warehouse", "queue", "auto stop", "warehouse health"],
        "query": """
            WITH warehouse_events AS (
              SELECT warehouse_id, event_type, event_time
              FROM {CATALOG}.compute.warehouse_events
              WHERE event_time >= DATEADD(DAY, -{DAYS}, NOW())
            ),
            warehouse_usage AS (
              SELECT usage_metadata['warehouse_id'] AS warehouse_id,
                cloud, sku_name, usage_start_time, usage_end_time, usage_date,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND usage_metadata['warehouse_id'] IS NOT NULL
            ),
            warehouse_cost AS (
              SELECT /*+ BROADCAST(lp) */ wu.warehouse_id,
                SUM(wu.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM warehouse_usage wu
              JOIN {CATALOG}.billing.list_prices lp
                ON wu.cloud=lp.cloud AND wu.sku_name=lp.sku_name
                AND wu.usage_start_time >= lp.price_start_time
                AND (wu.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY wu.warehouse_id
            ),
            event_summary AS (
              SELECT warehouse_id,
                SUM(CASE WHEN event_type='SCALED_UP' THEN 1 ELSE 0 END) AS scaled_up_count,
                SUM(CASE WHEN event_type='SCALED_DOWN' THEN 1 ELSE 0 END) AS scaled_down_count,
                SUM(CASE WHEN event_type='STARTING' THEN 1 ELSE 0 END) AS start_count,
                SUM(CASE WHEN event_type='STOPPED' THEN 1 ELSE 0 END) AS stop_count,
                MAX(event_time) AS last_event_at
              FROM warehouse_events
              GROUP BY warehouse_id
            )
            SELECT es.warehouse_id, ROUND(wc.cost_usd, 2) AS cost_usd,
              es.scaled_up_count, es.scaled_down_count,
              es.start_count, es.stop_count, es.last_event_at,
              CASE WHEN es.stop_count = 0 THEN 'NEVER_STOPS' ELSE 'STOPS_NORMALLY' END AS auto_stop_status
            FROM event_summary es
            LEFT JOIN warehouse_cost wc ON es.warehouse_id = wc.warehouse_id
            ORDER BY wc.cost_usd DESC NULLS LAST LIMIT 30
        """
    },
    {
        "intent": "slow_sql_queries",
        "description": "Slowest SQL queries — long execution time or high bytes scanned",
        "keywords": ["slow query", "slow sql", "long query", "expensive query", "query history"],
        "query": """
            SELECT statement_id, executed_by, warehouse_id,
              start_time, end_time,
              ROUND(total_duration_ms / 1000.0, 2) AS duration_sec,
              ROUND(read_bytes / (1024.0*1024*1024), 2) AS read_gb,
              ROUND(produced_rows, 0) AS rows_produced,
              statement_text
            FROM {CATALOG}.query.history
            WHERE start_time >= DATEADD(DAY, -{DAYS}, NOW())
              AND total_duration_ms IS NOT NULL
              AND execution_status = 'FINISHED'
            ORDER BY total_duration_ms DESC LIMIT 20
        """
    },
    {
        "intent": "failed_sql_queries",
        "description": "SQL queries that failed repeatedly — error patterns to address",
        "keywords": ["failed query", "sql error", "query fail", "query failure"],
        "query": """
            SELECT executed_by, warehouse_id, error_message,
              COUNT(*) AS failure_count,
              MIN(start_time) AS first_failed_at,
              MAX(start_time) AS last_failed_at
            FROM {CATALOG}.query.history
            WHERE start_time >= DATEADD(DAY, -{DAYS}, NOW())
              AND execution_status = 'FAILED'
              AND error_message IS NOT NULL
            GROUP BY executed_by, warehouse_id, error_message
            HAVING COUNT(*) >= 2
            ORDER BY failure_count DESC LIMIT 20
        """
    },
    {
        "intent": "delta_table_health",
        "description": "Delta tables likely needing OPTIMIZE/VACUUM — file count and size signals",
        "keywords": ["delta", "optimize", "vacuum", "small files", "table health", "fragmentation"],
        "query": """
            SELECT table_catalog, table_schema, table_name,
              table_owner, created, last_altered,
              DATEDIFF(NOW(), last_altered) AS days_since_alter
            FROM {CATALOG}.information_schema.tables
            WHERE table_type = 'MANAGED'
              AND data_source_format = 'DELTA'
              AND last_altered IS NOT NULL
            ORDER BY last_altered DESC LIMIT 50
        """
    },
    {
        "intent": "storage_growth",
        "description": "Tables with fastest storage growth based on access patterns",
        "keywords": ["storage growth", "table size", "growing fast", "data growth"],
        "query": """
            SELECT table_catalog, table_schema, table_name,
              table_owner, created, last_altered,
              DATEDIFF(NOW(), created) AS age_days
            FROM {CATALOG}.information_schema.tables
            WHERE table_type IN ('MANAGED','EXTERNAL')
              AND created >= DATEADD(DAY, -{DAYS}, NOW())
            ORDER BY created DESC LIMIT 30
        """
    },
    {
        "intent": "abandoned_tables",
        "description": "Tables not accessed recently — candidates for deletion or archival",
        "keywords": ["abandoned", "unused table", "dead table", "stale table", "archive"],
        "query": """
            WITH table_access AS (
              SELECT
                source_table_catalog AS catalog,
                source_table_schema AS schema_name,
                source_table_name AS table_name,
                MAX(event_time) AS last_accessed_at
              FROM {CATALOG}.access.table_lineage
              WHERE event_time >= DATEADD(DAY, -180, NOW())
                AND source_table_name IS NOT NULL
              GROUP BY source_table_catalog, source_table_schema, source_table_name
            )
            SELECT t.table_catalog, t.table_schema, t.table_name,
              t.table_owner, t.created,
              ta.last_accessed_at,
              DATEDIFF(NOW(), COALESCE(ta.last_accessed_at, t.created)) AS days_since_access
            FROM {CATALOG}.information_schema.tables t
            LEFT JOIN table_access ta
              ON t.table_catalog = ta.catalog
              AND t.table_schema = ta.schema_name
              AND t.table_name = ta.table_name
            WHERE t.table_type IN ('MANAGED','EXTERNAL')
              AND (ta.last_accessed_at IS NULL OR DATEDIFF(NOW(), ta.last_accessed_at) >= 60)
              AND DATEDIFF(NOW(), t.created) >= 30
            ORDER BY days_since_access DESC LIMIT 50
        """
    },

    # ============================================================
    # PHASE 3 — RELIABILITY (5 new)
    # ============================================================
    {
        "intent": "cluster_startup_slow",
        "description": "Clusters with slow startup times impacting job SLAs",
        "keywords": ["startup", "spin up", "slow start", "cluster boot", "cold start"],
        "query": """
            WITH startup_events AS (
              SELECT cluster_id,
                MIN(CASE WHEN event_type='STARTING' THEN event_time END) AS start_at,
                MIN(CASE WHEN event_type='RUNNING' THEN event_time END) AS running_at
              FROM {CATALOG}.compute.cluster_events
              WHERE event_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND event_type IN ('STARTING', 'RUNNING')
              GROUP BY cluster_id
            ),
            latest_clusters AS (
              SELECT cluster_id, cluster_name, owned_by, tags['team'] AS team
              FROM {CATALOG}.compute.clusters
              WHERE delete_time IS NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            )
            SELECT /*+ BROADCAST(c) */ c.cluster_name, c.owned_by, c.team,
              ROUND(AVG(unix_timestamp(se.running_at) - unix_timestamp(se.start_at)) / 60.0, 2) AS avg_startup_min,
              ROUND(MAX(unix_timestamp(se.running_at) - unix_timestamp(se.start_at)) / 60.0, 2) AS max_startup_min,
              COUNT(*) AS startup_count
            FROM startup_events se
            JOIN latest_clusters c ON se.cluster_id = c.cluster_id
            WHERE se.start_at IS NOT NULL AND se.running_at IS NOT NULL
            GROUP BY c.cluster_name, c.owned_by, c.team
            HAVING avg_startup_min > 5
            ORDER BY avg_startup_min DESC LIMIT 20
        """
    },
    {
        "intent": "init_script_failures",
        "description": "Cluster init script failures — common cause of cluster startup failures",
        "keywords": ["init script", "init failure", "startup failure", "bootstrap"],
        "query": """
            SELECT cluster_id, event_type, event_time, details
            FROM {CATALOG}.compute.cluster_events
            WHERE event_time >= DATEADD(DAY, -{DAYS}, NOW())
              AND (event_type LIKE '%INIT_SCRIPT%' OR event_type IN ('STARTUP_FAILURE', 'INIT_SCRIPTS_FINISHED'))
              AND (CAST(details AS STRING) LIKE '%FAIL%' OR CAST(details AS STRING) LIKE '%ERROR%')
            ORDER BY event_time DESC LIMIT 30
        """
    },
    {
        "intent": "job_concurrency_contention",
        "description": "Concurrent overlapping runs causing queue/resource pressure",
        "keywords": ["concurrent", "overlap", "queue", "contention", "parallel runs"],
        "query": """
            WITH task_runs AS (
              SELECT t.job_id, t.run_id, t.period_start_time, t.period_end_time
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.period_end_time IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ),
            run_level AS (
              SELECT job_id, run_id,
                MIN(period_start_time) AS started_at,
                MAX(period_end_time) AS ended_at
              FROM task_runs GROUP BY job_id, run_id
            ),
            overlapping AS (
              SELECT r1.job_id, r1.run_id, COUNT(*) AS overlapping_runs
              FROM run_level r1
              JOIN run_level r2
                ON r1.job_id = r2.job_id
                AND r1.run_id != r2.run_id
                AND r1.started_at < r2.ended_at
                AND r1.ended_at > r2.started_at
              GROUP BY r1.job_id, r1.run_id
            )
            SELECT /*+ BROADCAST(j) */ j.name AS job_name, o.job_id,
              COUNT(*) AS overlap_events,
              MAX(o.overlapping_runs) AS max_concurrent_runs
            FROM overlapping o
            LEFT JOIN (
              SELECT workspace_id, job_id, name FROM {CATALOG}.lakeflow.jobs
              QUALIFY ROW_NUMBER() OVER (PARTITION BY workspace_id, job_id ORDER BY change_time DESC) = 1
            ) j ON j.job_id = o.job_id
            GROUP BY j.name, o.job_id
            ORDER BY max_concurrent_runs DESC LIMIT 20
        """
    },
    {
        "intent": "dlt_pipeline_health",
        "description": "DLT (Delta Live Tables) pipeline failures and expectation violations",
        "keywords": ["dlt", "delta live", "pipeline", "expectations", "dlt failure"],
        "query": """
            WITH dlt_usage AS (
              SELECT usage_metadata['dlt_pipeline_id'] AS pipeline_id,
                cloud, sku_name, usage_start_time, usage_end_time, usage_date,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND usage_metadata['dlt_pipeline_id'] IS NOT NULL
            )
            SELECT /*+ BROADCAST(lp) */ pipeline_id,
              SUM(dbus) AS total_dbus,
              ROUND(SUM(dbus * CAST(lp.pricing.default AS DOUBLE)), 2) AS cost_usd,
              COUNT(DISTINCT usage_date) AS active_days
            FROM dlt_usage du
            JOIN {CATALOG}.billing.list_prices lp
              ON du.cloud=lp.cloud AND du.sku_name=lp.sku_name
              AND du.usage_start_time >= lp.price_start_time
              AND (du.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
            GROUP BY pipeline_id
            ORDER BY cost_usd DESC LIMIT 30
        """
    },
    {
        "intent": "team_efficiency",
        "description": "Cost-per-successful-run by team — efficiency leaderboard",
        "keywords": ["efficiency", "team efficiency", "cost per run", "team ranking", "team performance"],
        "query": """
            WITH latest_clusters AS (
              SELECT cluster_id, tags['team'] AS team FROM {CATALOG}.compute.clusters
              WHERE tags['team'] IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1
            ),
            usage_flat AS (
              SELECT CAST(usage_metadata['cluster_id'] AS STRING) AS cluster_id,
                cloud, sku_name, usage_start_time, usage_end_time, usage_date,
                CAST(usage_quantity AS DOUBLE) AS dbus
              FROM {CATALOG}.billing.usage
              WHERE usage_unit='DBU' AND usage_date >= DATEADD(DAY, -{DAYS}, CURRENT_DATE())
                AND usage_metadata['cluster_id'] IS NOT NULL
            ),
            team_cost AS (
              SELECT /*+ BROADCAST(lp, c) */ c.team,
                SUM(uf.dbus * CAST(lp.pricing.default AS DOUBLE)) AS cost_usd
              FROM usage_flat uf
              JOIN latest_clusters c ON uf.cluster_id = c.cluster_id
              JOIN {CATALOG}.billing.list_prices lp
                ON uf.cloud=lp.cloud AND uf.sku_name=lp.sku_name
                AND uf.usage_start_time >= lp.price_start_time
                AND (uf.usage_end_time <= lp.price_end_time OR lp.price_end_time IS NULL)
              GROUP BY c.team
            ),
            task_runs AS (
              SELECT t.job_id, t.run_id, t.result_state, t.compute_ids
              FROM {CATALOG}.lakeflow.job_task_run_timeline t
              WHERE t.period_start_time >= DATEADD(DAY, -{DAYS}, NOW())
                AND t.period_end_time IS NOT NULL
              QUALIFY ROW_NUMBER() OVER (PARTITION BY t.job_id, t.run_id ORDER BY t.period_start_time DESC) = 1
            ),
            run_with_team AS (
              SELECT DISTINCT tr.job_id, tr.run_id, tr.result_state, c.team
              FROM task_runs tr
              LATERAL VIEW explode(tr.compute_ids) ce AS cluster_id_exp
              JOIN latest_clusters c ON ce.cluster_id_exp = c.cluster_id
            ),
            team_runs AS (
              SELECT team,
                COUNT(*) AS total_runs,
                SUM(CASE WHEN result_state='SUCCEEDED' THEN 1 ELSE 0 END) AS successful_runs
              FROM run_with_team GROUP BY team
            )
            SELECT tc.team,
              ROUND(tc.cost_usd, 2) AS total_cost_usd,
              tr.total_runs,
              tr.successful_runs,
              ROUND(100.0 * tr.successful_runs / NULLIF(tr.total_runs, 0), 2) AS success_rate_pct,
              ROUND(tc.cost_usd / NULLIF(tr.successful_runs, 0), 2) AS cost_per_success_usd
            FROM team_cost tc
            LEFT JOIN team_runs tr ON tc.team = tr.team
            ORDER BY cost_per_success_usd DESC NULLS LAST LIMIT 30
        """
    },
]

INTENT_LIST = "\n".join([f"- {t['intent']}: {t['description']}" for t in QUERY_TEMPLATES])
print(f"Loaded {len(QUERY_TEMPLATES)} query templates.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3b — Dynamic SQL Generation (fallback for questions outside the 28 templates)
# MAGIC
# MAGIC When the intent classifier cannot map the question to a pre-built template, the agent
# MAGIC asks the LLM to generate SQL on the fly against the system catalog. SQL is validated
# MAGIC by execution; failures trigger up to 3 LLM-driven retries with the error fed back in.

# COMMAND ----------

def _clean_sql(raw: str) -> str:
    """Strip markdown fences from LLM output."""
    return re.sub(r"```(?:sql)?", "", raw, flags=re.IGNORECASE).replace("```", "").strip()


def _safe_int(value, default=30):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def resolve_time_context(question: str = "", default_days: int = None):
    """
    Resolves user prompt into deterministic SQL time filters.
    Supports: today, yesterday, last/past/previous N days. Falls back to widget lookback.
    """
    default_days = default_days or DEFAULT_DAYS
    q = str(question or "").lower()

    if "yesterday" in q:
        return {
            "mode": "yesterday", "days": 1, "label": "yesterday",
            "jr_filter": "DATE(jr.period_start_time) = DATE_SUB(CURRENT_DATE(), 1)",
            "t_filter":  "DATE(t.period_start_time) = DATE_SUB(CURRENT_DATE(), 1)",
            "n_filter":  "DATE(n.start_time) = DATE_SUB(CURRENT_DATE(), 1)",
            "u_filter":  "u.usage_date = DATE_SUB(CURRENT_DATE(), 1)",
        }
    if "today" in q:
        return {
            "mode": "today", "days": 1, "label": "today",
            "jr_filter": "DATE(jr.period_start_time) = CURRENT_DATE()",
            "t_filter":  "DATE(t.period_start_time) = CURRENT_DATE()",
            "n_filter":  "DATE(n.start_time) = CURRENT_DATE()",
            "u_filter":  "u.usage_date = CURRENT_DATE()",
        }

    patterns = [
        r"last\s+(\d+)\s+days?", r"past\s+(\d+)\s+days?",
        r"previous\s+(\d+)\s+days?", r"lookback\s*days?\s*[:=]?\s*(\d+)",
        r"(\d+)\s+day\s+lookback",
    ]
    prompt_days = None
    for pat in patterns:
        m = re.search(pat, q)
        if m:
            prompt_days = _safe_int(m.group(1), default=default_days)
            break

    days = prompt_days if prompt_days is not None else default_days
    return {
        "mode": "lookback", "days": days, "label": f"last {days} days",
        "jr_filter": f"jr.period_start_time >= CAST(DATE_SUB(CURRENT_DATE(), {days}) AS TIMESTAMP)",
        "t_filter":  f"t.period_start_time >= CAST(DATE_SUB(CURRENT_DATE(), {days}) AS TIMESTAMP)",
        "n_filter":  f"n.start_time >= CAST(DATE_SUB(CURRENT_DATE(), {days}) AS TIMESTAMP)",
        "u_filter":  f"u.usage_date >= DATE_SUB(CURRENT_DATE(), {days})",
    }


SCHEMA_CONTEXT = f"""
=====================
DATABRICKS SYSTEM TABLE SCHEMAS (catalog = {{CATALOG}})
=====================

{{CATALOG}}.compute.clusters (c):
  cluster_id, cluster_name, owned_by, worker_count,
  min_autoscale_workers, max_autoscale_workers, auto_termination_minutes,
  worker_node_type, driver_node_type, runtime_engine, dbr_version,
  cluster_source, tags (MAP), change_time, delete_time,
  aws_attributes, azure_attributes, gcp_attributes

{{CATALOG}}.compute.node_timeline (n):
  cluster_id, cpu_user_percent, cpu_system_percent,
  mem_used_percent, start_time
  (NOTE: node_timeline uses start_time, not period_start_time)

{{CATALOG}}.lakeflow.job_run_timeline (jr):
  account_id, workspace_id, job_id, run_id, run_name, run_type,
  result_state, trigger_type, termination_code, termination_type,
  period_start_time, period_end_time,
  run_duration_seconds, execution_duration_seconds,
  setup_duration_seconds, queue_duration_seconds, cleanup_duration_seconds,
  compute_ids, compute, job_parameters

{{CATALOG}}.lakeflow.job_task_run_timeline (t):
  account_id, workspace_id, job_id, run_id, period_start_time, period_end_time,
  task_key, compute_ids (ARRAY), result_state, job_run_id, parent_run_id,
  termination_code, compute, termination_type, task_parameters,
  setup_duration_seconds, cleanup_duration_seconds, execution_duration_seconds

{{CATALOG}}.lakeflow.jobs (j):
  account_id, workspace_id, job_id, name, creator_id, tags,
  run_as, change_time, delete_time, description, trigger, trigger_type,
  run_as_user_name, creator_user_name, paused, timeout_seconds
  NOTE: NO settings column. NO tasks/depends_on columns.

{{CATALOG}}.billing.usage (u):
  account_id, workspace_id, record_id, sku_name, cloud,
  usage_start_time, usage_end_time, usage_date,
  usage_unit, usage_quantity, custom_tags, usage_metadata
  CRITICAL: NO job_id column. Job link is usage_metadata['job_id'] / usage_metadata['cluster_id'].

{{CATALOG}}.billing.list_prices (lp):
  cloud, sku_name, price_start_time, price_end_time, pricing (STRUCT with .default DOUBLE)

=====================
SQL RULES
=====================
- Always aggregate in a CTE first; never use aggregates in WHERE.
- Cast timestamps with CAST(... AS TIMESTAMP) when needed.
- Return ONLY raw SQL — no markdown, no backticks, no explanation.
- Always end with LIMIT (50 max) unless the question is a single-row aggregate.
- Prefer ROUND() for numeric outputs; format durations in minutes or hours.
- For DBU-to-USD: JOIN billing.usage with billing.list_prices on (cloud, sku_name)
  with usage_start_time between price_start_time and (price_end_time OR NULL).
"""


def generate_sql(question: str) -> str:
    """LLM-generated SQL for questions outside the 28 templates."""
    time_ctx = resolve_time_context(question, default_days=DEFAULT_DAYS)

    time_rules = f"""
=====================
STRICT TIME WINDOW RULES
=====================
Resolved analysis period: {time_ctx["label"]}

Use these exact filters depending on table alias:
- For {CATALOG}.lakeflow.job_run_timeline alias jr:
  {time_ctx["jr_filter"]}
- For {CATALOG}.lakeflow.job_task_run_timeline alias t:
  {time_ctx["t_filter"]}
- For {CATALOG}.compute.node_timeline alias n:
  {time_ctx["n_filter"]}
- For {CATALOG}.billing.usage alias u:
  {time_ctx["u_filter"]}
"""

    prompt = f"""You are a Databricks SQL expert.

{SCHEMA_CONTEXT.format(CATALOG=CATALOG)}

{time_rules}

USER QUESTION
=============
{question}

OUTPUT
======
Return ONLY raw SQL. No explanation. No markdown. No backticks.
The SQL must reference tables as {CATALOG}.<schema>.<table>.
"""
    raw = _call_llm(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=900, temperature=0.1
    )
    return _clean_sql(raw)


def _parse_sql_error(raw_error: str) -> str:
    """Extract the meaningful line from a Spark exception."""
    priority = [
        r"AnalysisException[:\s]+(.+?)(?:\n|$)",
        r"ParseException[:\s]+(.+?)(?:\n|$)",
        r"SparkException[:\s]+(.+?)(?:\n|$)",
        r"Column '([^']+)' does not exist",
        r"Table or view not found[:\s]+([^\n;]+)",
        r"UNRESOLVED_COLUMN[\s\S]*?`([^`]+)`",
    ]
    for pat in priority:
        m = re.search(pat, raw_error, re.IGNORECASE)
        if m:
            return m.group(1).strip()[:400]
    for line in raw_error.splitlines():
        line = line.strip()
        if line and not line.startswith("at ") and len(line) > 15:
            return line[:400]
    return raw_error[:400]


def run_dynamic_sql(question: str, max_retries: int = 3):
    """
    Generate SQL with LLM, execute on Spark, retry up to max_retries with error feedback.
    Returns (df, row_count, columns, sql_used) or (None, 0, [], sql_used_or_error).
    """
    last_error = None
    sql = None
    for attempt in range(1, max_retries + 1):
        q = question if attempt == 1 else (
            f"{question}\n\nPrevious SQL failed with error:\n{last_error}\n"
            f"Fix the SQL and retry. Reference tables as {CATALOG}.<schema>.<table>."
        )
        sql = generate_sql(q)
        print(f"  [SQL-Gen attempt {attempt}] generated SQL:\n{sql}\n")
        try:
            df = spark.sql(sql)
            row_count = df.count()
            return df, row_count, df.columns, sql
        except Exception as e:
            last_error = str(e)
            print(f"  [SQL-Gen attempt {attempt}] failed: {last_error[:300]}")
            if attempt < max_retries:
                time.sleep(1)

    err_summary = _parse_sql_error(str(last_error) or "")
    print(f"  [SQL-Gen] giving up after {max_retries} attempts: {err_summary}")
    return None, 0, [], f"-- SQL-Gen failed: {err_summary}\n-- Last SQL:\n{sql or '(none)'}"


print("Dynamic SQL generation module ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 — LLM helpers (intent classifier, next-step decider, insight generator)

# COMMAND ----------

def _call_llm(messages: list, max_tokens: int = 200, temperature: float = 0.0) -> str:
    """Calls LLM endpoint, robust to response format variations."""
    try:
        response = llm_client.predict(
            endpoint=LLM_ENDPOINT,
            inputs={"messages": messages, "max_tokens": max_tokens, "temperature": temperature}
        )
        for accessor in [
            lambda r: r.choices[0].message.content,
            lambda r: r['choices'][0]['message']['content'],
            lambda r: r.choices[0]['message']['content'],
            lambda r: r['candidates'][0]['content']['parts'][0]['text'],
        ]:
            try:
                val = accessor(response)
                if val: return val
            except Exception:
                continue
        return str(response)
    except Exception as e:
        return f"LLM_ERROR: {str(e)}"


def classify_intent_keyword(question: str) -> dict:
    """Keyword fallback classifier."""
    question_lower = question.lower()
    best_match, best_score = None, 0
    for template in QUERY_TEMPLATES:
        score = sum(1 for kw in template["keywords"] if kw in question_lower)
        if any(c.isdigit() for c in question) and template["intent"] == "job_status":
            score += 2
        if score > best_score:
            best_score, best_match = score, template
    if best_match is None:
        best_match = next(t for t in QUERY_TEMPLATES if t["intent"] == "workspace_summary")
    return best_match


def classify_intent(question: str) -> dict:
    """
    LLM-based classifier with keyword fallback.
    Returns one of the 28 template dicts, OR a sentinel dict with intent='dynamic_sql'
    when SQL-gen is enabled and no template fits the question well.
    """
    prompt = f"""You are an intent classifier for a Databricks observability system.
Given a user question, pick the SINGLE best matching intent from the list below.

Available intents:
{INTENT_LIST}

Rules:
1. Reply with ONLY the intent name — nothing else
2. Failures/errors → job_failures or repeated_failures
3. Cost spike/anomaly → cost_anomaly
4. General cost → cost_by_team / cost_trend / cost_by_sku
5. Speed → slowest_jobs
6. Vague → workspace_summary
7. If NONE of the listed intents fit the question well, reply with: dynamic_sql
8. Reply with ONLY the intent name. No explanation.

User question: "{question}"
Intent:"""

    result = _call_llm([{"role": "user", "content": prompt}], max_tokens=50, temperature=0.0)
    intent_name = result.strip().lower().replace('"', '').replace("'", "").split('\n')[0].strip()

    # Exact template match
    for template in QUERY_TEMPLATES:
        if template["intent"] == intent_name:
            return template

    # LLM said "dynamic_sql" — use SQL-gen path (only if enabled)
    if intent_name == "dynamic_sql" and ENABLE_SQL_GEN:
        return {
            "intent": "dynamic_sql",
            "description": "Dynamic SQL generated against system catalog",
            "keywords": [],
            "query": None,  # No template; SQL is generated at runtime
        }

    # Fallback: keyword classifier (returns a real template)
    return classify_intent_keyword(question)


def decide_next_step(question: str, steps_taken: list) -> str:
    """Asks LLM if more data is needed; returns 'FINAL' or next intent name."""
    steps_summary = ""
    for step in steps_taken:
        steps_summary += f"\nStep {step['step']}: '{step['intent']}' returned {step['row_count']} rows.\n"
        if step['data']:
            preview = json.dumps(step['data'][:3], indent=2, default=str)
            if len(preview) > 500:
                preview = preview[:500] + "..."
            steps_summary += f"Preview: {preview}\n"

    used = [s['intent'] for s in steps_taken]
    available = [t for t in QUERY_TEMPLATES if t['intent'] not in used]
    if not available:
        return "FINAL"

    available_list = "\n".join([f"- {t['intent']}: {t['description']}" for t in available])
    prompt = f"""You are a Databricks observability agent doing multi-step root cause analysis.

User's question: "{question}"

Steps completed:
{steps_summary}

Available next queries:
{available_list}

Rules:
1. If data is sufficient → reply: FINAL
2. If more data needed for root cause → reply with intent name
3. Only chain for cross-domain analysis (e.g., cost spike → which team → which jobs)
4. Simple lookups don't need chaining
5. Reply with ONLY one word: "FINAL" or an intent name.

Decision:"""

    result = _call_llm([{"role": "user", "content": prompt}], max_tokens=50, temperature=0.0)
    decision = result.strip().lower().replace('"', '').replace("'", "").split('\n')[0].strip()
    if decision == "final":
        return "FINAL"
    for template in available:
        if template["intent"] == decision:
            return decision
    return "FINAL"


def generate_insight_json(question: str, steps_taken: list) -> dict:
    """Asks LLM for structured insight with severity, confidence, recommendations."""
    all_data_summary = ""
    for step in steps_taken:
        all_data_summary += f"\n--- {step['description']} ({step['row_count']} rows) ---\n"
        all_data_summary += json.dumps(step['data'][:MAX_ROWS_FOR_LLM], indent=2, default=str)

    system_prompt = """You are a senior Databricks Operations Analyst. Return ONLY a valid JSON object — no markdown, no code fences, no extra text.

Required JSON schema:
{
  "insight": "2-4 sentence root cause analysis citing specific numbers from the data",
  "severity": "CRITICAL" | "HIGH" | "MEDIUM" | "LOW" | "INFO",
  "confidence": 0.0-1.0,
  "recommendations": ["action 1", "action 2", "action 3"]
}

Severity rules:
- CRITICAL: production outage, repeated failures >5, cost spike >100%, security issue
- HIGH: degradation, cost spike 30-100%, failures 2-5, SLA breach >2x baseline
- MEDIUM: anomalies, cost spike 10-30%, idle clusters, oversized resources
- LOW: optimization opportunities, minor inefficiencies
- INFO: healthy / informational only

Confidence rules:
- 0.9+: clear data, strong signal
- 0.7-0.9: good data, some inference
- 0.5-0.7: limited data, partial answer
- <0.5: very uncertain

Use ONLY numbers from the provided data. No fabrication."""

    user_prompt = f"""Question: {question}

Data from {len(steps_taken)} step(s):
{all_data_summary}

Return ONLY the JSON object."""

    result = _call_llm(
        [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        max_tokens=800, temperature=0.1
    )

    # Strip markdown fences if model added them
    cleaned = result.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[1] if "\n" in cleaned else cleaned
        if cleaned.endswith("```"):
            cleaned = cleaned[:-3]
        if cleaned.startswith("json"):
            cleaned = cleaned[4:]
        cleaned = cleaned.strip()

    try:
        parsed = json.loads(cleaned)
        return {
            "insight": parsed.get("insight", "No insight generated."),
            "severity": parsed.get("severity", "INFO").upper(),
            "confidence": float(parsed.get("confidence", 0.5)),
            "recommendations": parsed.get("recommendations", [])
        }
    except Exception as e:
        return {
            "insight": f"Insight parsing failed. Raw response: {result[:300]}",
            "severity": "INFO",
            "confidence": 0.0,
            "recommendations": []
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 — Query executor with timeout, caching, error handling

# COMMAND ----------

# In-session cache: intent+days → (df, row_count, columns, timestamp)
_QUERY_CACHE = {}
CACHE_TTL_SEC = 300  # 5 minutes


def execute_query(template: dict, days: int) -> tuple:
    """Executes a templated SQL query with caching, timeout, and AQE enabled."""
    cache_key = f"{template['intent']}::{days}::{CATALOG}"
    now = time.time()

    # Cache hit
    if cache_key in _QUERY_CACHE:
        cached_df, cached_rc, cached_cols, cached_ts = _QUERY_CACHE[cache_key]
        if now - cached_ts < CACHE_TTL_SEC:
            return cached_df, cached_rc, cached_cols

    query = template["query"].format(CATALOG=CATALOG, DAYS=days)

    # Wrap execution
    start = time.time()
    try:
        df = spark.sql(query)
        # Materialize once into a small pandas-like collect for row count + display
        df = df.cache()  # cache for downstream reuse
        row_count = df.count()
        columns = df.columns
        if (time.time() - start) > QUERY_TIMEOUT_SEC:
            print(f"  WARNING: query took {time.time()-start:.1f}s (over {QUERY_TIMEOUT_SEC}s soft limit)")
        _QUERY_CACHE[cache_key] = (df, row_count, columns, now)
        return df, row_count, columns
    except Exception as e:
        print(f"  Query error: {str(e)[:300]}")
        return None, 0, []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 — REST API error enrichment

# COMMAND ----------

def get_actual_errors(run_ids: list, max_runs: int = 10) -> dict:
    """Fetches actual error messages from /api/2.1/jobs/runs/get."""
    headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
    error_lookup = {}
    for run_id in run_ids[:max_runs]:
        try:
            r = requests.get(f"{WORKSPACE_URL}/api/2.1/jobs/runs/get",
                             headers=headers, params={"run_id": run_id}, timeout=10)
            if r.status_code != 200:
                error_lookup[str(run_id)] = {"actual_error": f"API status {r.status_code}", "error_trace": None}
                continue
            run_data = r.json()
            state = run_data.get("state", {})
            state_message = state.get("state_message", "")
            task_errors = []
            for task in run_data.get("tasks", []):
                ts = task.get("state", {})
                tr = task.get("run_output", {})
                if ts.get("result_state") in ("FAILED", "ERROR", "TIMED_OUT"):
                    task_errors.append({
                        "task_key": task.get("task_key", "unknown"),
                        "error": ts.get("state_message", ""),
                        "error_trace": tr.get("error", "") or tr.get("error_trace", "")
                    })
            if task_errors:
                best = max(task_errors, key=lambda x: len(x.get("error_trace", "") or ""))
                trace = best.get("error_trace") or best.get("error") or state_message
            else:
                trace = state_message
            if trace and len(trace) > 1000:
                trace = trace[:1000] + "... [truncated]"
            error_lookup[str(run_id)] = {"actual_error": state_message, "error_trace": trace}
        except Exception as e:
            error_lookup[str(run_id)] = {"actual_error": f"Fetch failed: {str(e)[:200]}", "error_trace": None}
    return error_lookup


def enrich_failure_data(data: list) -> list:
    """Attaches actual errors to failed job rows."""
    run_ids = list({str(row.get("run_id", "")) for row in data if row.get("run_id")})
    if not run_ids:
        return data
    print(f"  Fetching errors for {len(run_ids)} failed runs...")
    lookup = get_actual_errors(run_ids, max_runs=10)
    for row in data:
        rid = str(row.get("run_id", ""))
        info = lookup.get(rid, {"actual_error": "Not fetched", "error_trace": None})
        row["actual_error"] = info["actual_error"]
        row["error_trace"] = info["error_trace"]
    return data

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 — Severity scoring (rule-based fallback for LLM output)

# COMMAND ----------

def compute_rule_based_severity(intent: str, data: list) -> str:
    """Rule-based severity calculation as a sanity check on the LLM severity."""
    if not data:
        return "INFO"
    thresh = SEVERITY_THRESHOLDS.get(intent, {})

    try:
        if intent == "cost_anomaly":
            max_z = max((abs(float(r.get("z_score", 0) or 0)) for r in data), default=0)
            if max_z >= 4: return "CRITICAL"
            if max_z >= 3: return "HIGH"
            if max_z >= 2: return "MEDIUM"
        if intent == "repeated_failures":
            max_streak = max((int(r.get("consecutive_failures", 0) or 0) for r in data), default=0)
            if max_streak >= 10: return "CRITICAL"
            if max_streak >= 5:  return "HIGH"
            if max_streak >= 3:  return "MEDIUM"
        if intent == "sla_breach":
            max_mult = max((float(r.get("multiplier", 0) or 0) for r in data), default=0)
            if max_mult >= 5: return "CRITICAL"
            if max_mult >= 3: return "HIGH"
            if max_mult >= 2: return "MEDIUM"
        if intent == "failure_rate":
            max_rate = max((float(r.get("failure_rate_pct", 0) or 0) for r in data), default=0)
            if max_rate >= 50: return "CRITICAL"
            if max_rate >= 25: return "HIGH"
            if max_rate >= 10: return "MEDIUM"
        if intent == "idle_clusters":
            count = len(data)
            if count >= 20: return "HIGH"
            if count >= 5:  return "MEDIUM"
            return "LOW"
        if intent == "oversized_clusters":
            if len(data) >= 10: return "HIGH"
            if len(data) >= 3:  return "MEDIUM"
            return "LOW"
        if intent == "job_failures":
            if len(data) >= 20: return "HIGH"
            if len(data) >= 5:  return "MEDIUM"
            return "LOW"
    except Exception:
        pass
    return "LOW"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 — Result persistence (all steps, not just last)

# COMMAND ----------

def save_all_results(steps_taken: list, question: str):
    """
    Saves every step's data to hive_metastore.default.agent_last_results.
    Uses pandas for type-safe conversion (Spark inference fails on None-heavy columns).
    Logs every failure loudly so we can diagnose.
    """
    if not steps_taken:
        print("  save_all_results: no steps to save, skipping.")
        return

    import pandas as pd

    all_rows = []
    for step in steps_taken:
        for row in step["data"]:
            row_copy = {}
            for k, v in row.items():
                # Skip dicts (break CSV); convert lists to comma-joined strings; everything else to string
                if isinstance(v, dict):
                    continue
                elif isinstance(v, list):
                    row_copy[k] = ",".join(str(x) for x in v)
                elif v is None:
                    row_copy[k] = ""
                else:
                    row_copy[k] = str(v)
            row_copy["_step_num"] = str(step["step"])
            row_copy["_intent"] = str(step["intent"])
            row_copy["_question"] = str(question)[:500]
            row_copy["_captured_at"] = datetime.now(timezone.utc).isoformat()
            all_rows.append(row_copy)

    if not all_rows:
        print("  save_all_results: all steps had 0 rows, writing empty marker.")
        # Write a single marker row so download path doesn't break
        all_rows = [{
            "_step_num": "0", "_intent": "no_data",
            "_question": str(question)[:500],
            "_captured_at": datetime.now(timezone.utc).isoformat(),
            "message": "No data rows were returned for this question."
        }]

    try:
        # Normalize keys across all rows
        all_keys = set()
        for r in all_rows:
            all_keys.update(r.keys())
        normalized = [{k: r.get(k, "") for k in sorted(all_keys)} for r in all_rows]

        # Use pandas → Spark (handles None and mixed types safely)
        pdf = pd.DataFrame(normalized).astype(str).fillna("")
        sdf = spark.createDataFrame(pdf)

        (sdf.write
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("hive_metastore.default.agent_last_results"))

        rc = spark.table("hive_metastore.default.agent_last_results").count()
        print(f"  save_all_results: persisted {rc} rows to hive_metastore.default.agent_last_results")
    except Exception as e:
        print(f"  save_all_results FAILED: {type(e).__name__}: {str(e)[:400]}")
        # Last-resort fallback: drop+create via SQL with a friendly row
        try:
            spark.sql("DROP TABLE IF EXISTS hive_metastore.default.agent_last_results")
            spark.sql("""
                CREATE TABLE hive_metastore.default.agent_last_results (
                    message STRING, _question STRING, _captured_at STRING
                ) USING DELTA
            """)
            safe_q = str(question)[:500].replace("'", "''")
            spark.sql(f"""
                INSERT INTO hive_metastore.default.agent_last_results VALUES
                ('Persistence failed - see notebook logs', '{safe_q}',
                 '{datetime.now(timezone.utc).isoformat()}')
            """)
            print("  save_all_results: wrote fallback marker row.")
        except Exception as e2:
            print(f"  save_all_results FALLBACK ALSO FAILED: {str(e2)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 — The Agent

# COMMAND ----------

GREETINGS = {"hi", "hello", "hey", "help", "start", "yo", "good morning", "good evening"}


def ask_agent(question: str, days: int = None) -> dict:
    """Main agent entry. Returns a dict that will be JSON-serialized for Power Automate."""
    days = days or DEFAULT_DAYS
    started_at = time.time()
    timestamp = datetime.now(timezone.utc).isoformat()

    q_clean = (question or "").strip().lower()

    # Greeting short-circuit
    if q_clean in GREETINGS:
        return {
            "status": "success",
            "question": question,
            "timestamp": timestamp,
            "severity": "INFO",
            "confidence": 1.0,
            "insight": "Hello. Ask me about job failures, cost trends, idle clusters, SLA breaches, or workspace health.",
            "recommendations": [],
            "reasoning_chain": [],
            "data": [],
            "metadata": {"lookback_days": days, "catalog": CATALOG,
                         "queries_executed": 0, "execution_time_sec": 0.0,
                         "token_source": TOKEN_SOURCE}
        }

    # Empty/garbage
    if not q_clean or len(q_clean) < 2:
        return {
            "status": "error",
            "question": question,
            "timestamp": timestamp,
            "severity": "INFO",
            "confidence": 0.0,
            "insight": "Empty or invalid question. Ask about failures, cost, clusters, SLA, or warehouse health.",
            "recommendations": [],
            "reasoning_chain": [],
            "data": [],
            "metadata": {"lookback_days": days, "catalog": CATALOG,
                         "queries_executed": 0, "execution_time_sec": 0.0,
                         "token_source": TOKEN_SOURCE}
        }

    steps_taken = []

    # Step 1: classify
    template = classify_intent(question)
    current_intent = template["intent"]

    for step_num in range(1, MAX_REASONING_STEPS + 1):

        # ── Dynamic SQL branch (no pre-built template) ────────────────────────
        if current_intent == "dynamic_sql":
            print(f"Step {step_num}: 'dynamic_sql' — generating SQL on the fly")
            df, row_count, columns, sql_used = run_dynamic_sql(question, max_retries=3)

            if df is None or row_count == 0:
                steps_taken.append({
                    "step": step_num, "intent": "dynamic_sql",
                    "description": f"Dynamic SQL: {sql_used[:200]}",
                    "row_count": 0, "data": [], "columns": []
                })
                break

            data = [row.asDict(recursive=True) for row in df.limit(MAX_ROWS_FOR_LLM).collect()]
            steps_taken.append({
                "step": step_num, "intent": "dynamic_sql",
                "description": f"Dynamic SQL answering: {question[:120]}",
                "row_count": row_count, "data": data, "columns": list(columns),
                "sql_used": sql_used
            })
            # Dynamic SQL is one-shot; don't chain further steps
            break
        # ──────────────────────────────────────────────────────────────────────

        template = next((t for t in QUERY_TEMPLATES if t["intent"] == current_intent), None)
        if template is None:
            break

        print(f"Step {step_num}: '{template['intent']}' — {template['description']}")
        df, row_count, columns = execute_query(template, days)

        if df is None or row_count == 0:
            steps_taken.append({
                "step": step_num, "intent": current_intent,
                "description": template["description"],
                "row_count": 0, "data": [], "columns": []
            })
            # Try next step if available
            if step_num < MAX_REASONING_STEPS:
                next_intent = decide_next_step(question, steps_taken)
                if next_intent == "FINAL":
                    break
                current_intent = next_intent
                continue
            break

        # Collect once (no double scan)
        data = [row.asDict(recursive=True) for row in df.limit(MAX_ROWS_FOR_LLM).collect()]

        if current_intent == "job_failures":
            data = enrich_failure_data(data)
            columns = list(columns) + ["actual_error", "error_trace"]

        steps_taken.append({
            "step": step_num, "intent": current_intent,
            "description": template["description"],
            "row_count": row_count, "data": data, "columns": list(columns)
        })

        if step_num < MAX_REASONING_STEPS:
            nxt = decide_next_step(question, steps_taken)
            if nxt == "FINAL":
                break
            current_intent = nxt
        else:
            break

    # Persist all steps' results
    save_all_results(steps_taken, question)

    # No data at all
    if not steps_taken or all(s["row_count"] == 0 for s in steps_taken):
        return {
            "status": "no_data",
            "question": question,
            "timestamp": timestamp,
            "severity": "INFO",
            "confidence": 0.0,
            "insight": f"No data found in the last {days} days for this question.",
            "recommendations": ["Increase lookback window", "Verify system tables are populated", "Try a broader question"],
            "reasoning_chain": [{"step": s["step"], "intent": s["intent"], "rows": s["row_count"]} for s in steps_taken],
            "data": [],
            "metadata": {"lookback_days": days, "catalog": CATALOG,
                         "queries_executed": len(steps_taken),
                         "execution_time_sec": round(time.time() - started_at, 2),
                         "token_source": TOKEN_SOURCE}
        }

    # Generate insight
    insight_obj = generate_insight_json(question, steps_taken)

    # Reconcile severity: take the more severe of LLM vs rule-based
    severity_rank = {"INFO": 0, "LOW": 1, "MEDIUM": 2, "HIGH": 3, "CRITICAL": 4}
    llm_sev = insight_obj.get("severity", "INFO")
    rule_sev = compute_rule_based_severity(steps_taken[0]["intent"], steps_taken[0]["data"])
    final_sev = llm_sev if severity_rank.get(llm_sev, 0) >= severity_rank.get(rule_sev, 0) else rule_sev

    return {
        "status": "success",
        "question": question,
        "timestamp": timestamp,
        "severity": final_sev,
        "confidence": insight_obj.get("confidence", 0.5),
        "insight": insight_obj.get("insight", ""),
        "recommendations": insight_obj.get("recommendations", []),
        "reasoning_chain": [{"step": s["step"], "intent": s["intent"], "rows": s["row_count"]} for s in steps_taken],
        "data": [{"step": s["step"], "intent": s["intent"], "description": s["description"],
                  "row_count": s["row_count"], "rows": s["data"]} for s in steps_taken],
        "metadata": {
            "lookback_days": days,
            "catalog": CATALOG,
            "queries_executed": len(steps_taken),
            "execution_time_sec": round(time.time() - started_at, 2),
            "token_source": TOKEN_SOURCE
        }
    }


print("Agent v4.2 ready (28 templates + dynamic SQL fallback).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10 — Entry point (single, JSON-only exit)

# COMMAND ----------

question = dbutils.widgets.get("question")
days = int(dbutils.widgets.get("days"))

# Special path: download last results as CSV
if question.strip().upper() == "DOWNLOAD_REPORT":
    table_name = "hive_metastore.default.agent_last_results"
    csv_output = None
    # Check existence WITHOUT try/except around exit
    try:
        pdf = spark.table(table_name).toPandas()
        if pdf.empty:
            csv_output = "message\nNo data available. Please ask a question first, then download."
        else:
            csv_output = pdf.to_csv(index=False)
    except Exception as e:
        # Genuine errors (table missing, permission, etc.) handled here
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e) or "AnalysisException" in str(type(e).__name__):
            csv_output = "message\nNo report available yet. Please ask a question first, then click Download."
        else:
            csv_output = f"message\nUnexpected error: {type(e).__name__}: {str(e)[:300]}"
    
    # Exit OUTSIDE the try/except — NotebookExit won't be caught
    dbutils.notebook.exit(csv_output)

result = ask_agent(question, days=days)
dbutils.notebook.exit(json.dumps(result, default=str))