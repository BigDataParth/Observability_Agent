# Databricks Observability AI Agent

Natural-language operational intelligence over Databricks system tables, delivered through Microsoft Teams via Power Automate.

Ask plain-English questions about a Databricks workspace — job failures, cost, cluster health, SLA breaches — and get an AI-generated insight with severity, confidence, and recommendations, posted back as an Adaptive Card.

**Example questions**
- Why did our jobs fail yesterday?
- Which team is spending the most this month?
- Are any pipelines slower than usual?
- Show me clusters that are doing nothing.
- What's the workspace health right now?

## Architecture

```
Teams message → Power Automate trigger → Databricks job run (notebook)
   → intent classification → templated SQL / dynamic SQL fallback
   → LLM insight (severity, confidence, recommendations)
   → Adaptive Card reply in Teams
   → JSON logged to OneDrive + last result cached for CSV download
```

- **Notebook**: `Observability_AI_Agent_v4_3.py` — 29 pre-validated query templates + dynamic SQL fallback for anything outside them, multi-step reasoning (up to 5 chained queries), rule-based + LLM severity scoring, structured JSON-only output.
- **Flow**: `DBR-AGENT-V5` (Power Automate) — Teams trigger → submits/polls a Databricks job run → parses the JSON result → posts an Adaptive Card with follow-up quick-action buttons (Failed Jobs, Cost Analysis, Cluster Health, Download Report) → loops on button clicks until `DOWNLOAD_REPORT` or `STOP`.

## Prerequisites

Confirm all of these before deploying — most setup failures trace back to one of them.

- Databricks workspace on **Premium or Enterprise** tier (system tables aren't available on Standard).
- **Unity Catalog** enabled.
- System schemas enabled: `system.lakeflow`, `system.billing`, `system.compute`.
- A Databricks-hosted **LLM endpoint** (Foundation Model APIs / AI Gateway). Default: `databricks-claude-haiku-4-5`.
- Permissions:
  - `USE CATALOG` on `system`
  - `SELECT` on `system.lakeflow.*`, `system.billing.*`, `system.compute.*`
  - `CAN QUERY` on the LLM serving endpoint
  - Workspace token generation (for REST API error enrichment)
- Microsoft Teams + Power Automate access (sign in with your org account at [make.powerautomate.com](https://make.powerautomate.com)).

## Setup

### 1. Deploy the notebook

1. Download `Observability_AI_Agent_v4_3.py`.
2. In Databricks: **Workspace → your user folder → Import** → upload the file. The `# MAGIC` markers auto-convert it into an interactive notebook.
3. Attach to a cluster — small all-purpose cluster, DBR 14.x or higher.

### 2. Configure the widgets

| Widget | Default | Notes |
|---|---|---|
| `question` | sample question | overridden by Power Automate per request |
| `days` | `30` | lookback window: 7 / 14 / 30 / 60 / 90 |
| `catalog` | `system` | or `system_catalog` if your workspace differs |
| `secret_scope` / `secret_key` | blank | optional — falls back to the notebook context token if unset |
| `llm_endpoint` | `databricks-claude-haiku-4-5` | or `databricks-claude-sonnet-4-5` / `databricks-meta-llama-3-3-70b-instruct` |
| `max_steps` | `5` | max chained reasoning queries: 1 / 2 / 3 / 5 |
| `enable_sql_gen` | `Yes` | dynamic SQL fallback for questions outside the 29 templates |

### 3. Test in Databricks

1. **Run All**.
2. Confirm the output includes:
   - `Loaded 29 query templates.`
   - `Dynamic SQL generation module ready.`
   - `Agent v4.3 ready (29 templates + dynamic SQL fallback + report titles).`
3. Run a question through the widget (e.g. *"What's the overall health of the workspace?"*) and confirm the returned JSON has `"status": "success"` with an `insight`, `severity`, and `data`.
4. Try 2–3 more questions (failed jobs, team spend, repeated failures) to confirm reliability across intents.

### 4. Prepare for Power Automate

- Copy the notebook path: **Share → copy path** (e.g. `/Workspace/Users/you@company.com/.../Observability_AI_Agent_v4_3`).
- Generate a Databricks **Personal Access Token**: profile icon → **User Settings → Developer → Access Tokens → Generate New Token**. Copy it immediately — it isn't shown again. Store it in a secret scope or secure connection, **not** in source control.
- Note your **workspace URL** (browser address bar) and **cluster ID** (Compute → your cluster → trailing segment of the URL).

### 5. Deploy the Power Automate flow

1. [make.powerautomate.com](https://make.powerautomate.com) → **My Flows → Import → Import Package (Legacy)**.
2. Upload the flow package and configure connections: **Microsoft Teams**, **OneDrive for Business**.
3. Open the imported flow and update the `Init_*` variables (see table below).
4. Update the Teams trigger's target team/channel to your own.
5. **Save**, then **Turn On**.

### 6. Test end-to-end in Teams

1. Post a question in the configured channel.
2. Wait 30–60 seconds (longer on cold-start clusters) for the Adaptive Card reply.
3. Send `DOWNLOAD_REPORT` and confirm a CSV share link comes back.
4. Send a few more diverse questions to confirm reliability.

## Configuration variables (Power Automate flow)

| Variable | Purpose |
|---|---|
| `varWorkspaceUrl` | Databricks workspace URL |
| `varNotebookPath` | Path to the imported notebook |
| `varClusterId` | Existing cluster ID the flow submits runs to |
| `varTeamsGroupId` / `varTeamsChannelId` | Target Teams channel |
| `varTeamsChannelName` | Display name used in the greeting card |
| `varDatabricksToken` | Databricks PAT — keep in a secret/secure connection, never commit it hardcoded in the flow definition |
| `varDefaultDays` | Default lookback window (30) |
| `varLogFolder` | OneDrive folder every run's JSON is logged to (`/Observability_Logs`) |

## Sample questions

**Job failures & errors**
- Why did jobs fail yesterday?
- Show me recent failures with error details
- Are any pipelines repeatedly failing?

**Cost & spending**
- Which team is spending the most?
- Show me daily cost trend for the last 30 days
- Why did cost spike this week?

**Performance & SLA**
- Which jobs are running the slowest?
- Are any pipelines degrading vs their baseline?

**Cluster health**
- Show me idle or unused clusters
- Which clusters are oversized?

**General health**
- What's the overall workspace health?
- What's the failure rate trend?

## Troubleshooting

| Issue | Fix |
|---|---|
| Catalog `'system'` not found | Update the `catalog` widget to match your workspace (e.g. `system_catalog`). |
| Endpoint not found for LLM | Verify the endpoint name under Databricks Serving; update `llm_endpoint`. |
| Permission denied on system tables | Ask an account admin to grant `SELECT` on `system.lakeflow`, `system.billing`, `system.compute`. |
| REST API errors in error enrichment | PAT token may be expired — regenerate and update the Power Automate flow. |
| Power Automate flow times out | Increase the HTTP action timeout; use a running all-purpose cluster instead of a job-cluster cold start. |
| Teams bot doesn't respond | Check the flow run history in Power Automate and inspect the failed step. |
| LLM returns a nonsense intent | Re-run the notebook; verify the LLM endpoint is healthy in Databricks Serving. |
| Empty results returned | Increase the lookback window (`days` widget), e.g. 60 or 90. |
| Query takes too long | Reduce `MAX_ROWS_FOR_LLM` or `max_steps`. |

## Notes

- Every run's full structured JSON is logged to OneDrive under `varLogFolder`; the last run's flattened results are cached in `hive_metastore.default.agent_last_results` for the `DOWNLOAD_REPORT` CSV export.
- The Teams card's quick-action buttons (Failed Jobs, Cost Analysis, Cluster Health, Download Report) re-invoke the notebook with the button's intent as the `question` parameter, up to 10 follow-up loops per conversation.
