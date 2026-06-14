# Blast Radius Prophet 🔴

> **Blast Radius Prophet** is an AI agent for Splunk Observability that learns your real service dependency graph from live telemetry and, at the first signs of anomalies, predicts the incident's blast radius, who will be affected, and the best mitigation — before a SEV is declared.

---

## The Problem

Most teams only understand the impact of an incident *after* it happens. A database slows down silently. Fifteen minutes later, thousands of users hit error screens. The on-call engineer gets paged, panics, spends an hour diagnosing, then drafts a manual alert.

**Splunk already has the signals 5–15 minutes before users notice.** Nobody is connecting them. Blast Radius Prophet does.

---

## How It Works

Three AI agents run in sequence, all powered by the **Splunk MCP Server** (`splunk_run_query` tool):

```
Scout Agent        → detects anomalies in service metrics
Architect Agent    → infers live dependency graph from logs (no CMDB needed)
Comm-Officer Agent → pre-drafts Slack alert + incident ticket + mitigation
```

The key innovation: the service dependency graph is inferred entirely from **actual observed traffic patterns** in Splunk logs using SPL — not from stale CMDBs or manually maintained runbooks.

A human operator reviews the pre-drafted alert and approves or dismisses with one click (**human-in-the-loop**).

---

## Architecture

See `architecture_diagram.png` at the root of this repo.

```
Splunk Enterprise (local)
        ↓  REST API
Splunk MCP Server (Splunkbase app)
        ↓  StreamableHTTP
mcp-remote (npx bridge)
        ↓  stdio
Python mcp library (ClientSession)
        ↓
Scout Agent  →  Architect Agent  →  Comm-Officer Agent
        ↓
Flask Backend (port 5001)
        ↓  JSON API
Browser UI (human-in-the-loop approval)
```

---

## Splunk AI Capabilities Used

| Capability | How used |
|---|---|
| **Splunk MCP Server** | All agent queries via `splunk_run_query` tool over StreamableHTTP |
| **Splunk Python SDK** | Connection management and search execution |
| **SPL queries** | Dependency graph inference + anomaly detection |

### Key SPL Queries

**Dependency graph inference (Architect Agent):**
```spl
index=main sourcetype=microservice:json earliest=-60d
| stats count by service, upstream_service, downstream_service
| where count > 5
| sort -count
```

**Anomaly detection (Scout Agent):**
```spl
index=main sourcetype=microservice:metrics earliest=-60d
| stats avg(avg_latency_ms) as latency,
        avg(error_rate) as error_rate,
        avg(cpu_pct) as cpu
  by service
| sort -error_rate
```

---

## Demo Scenario

**Services:** `web → api → [auth, payments, checkout] → db`

**Before (Old Way):**
- DB degrades silently
- 15 min later: ~38,000 APAC users see errors
- On-call paged, spends an hour diagnosing
- Manual Slack message sent 45 min after first signal

**After (Blast Radius Prophet):**
- Scout detects DB anomaly at T+0 via MCP splunk_run_query
- Architect infers graph from logs: db → [payments, checkout] → api → web
- ~38,000 APAC users at risk identified in seconds
- Comm-Officer pre-drafts Slack alert + incident ticket
- Human approves with one click — before any user notices

---

## Setup & Run

### Prerequisites
- Splunk Enterprise (free trial + Developer License)
- Splunk MCP Server app (Splunkbase app ID: 7931)
- Python 3.8+
- Node.js (for mcp-remote bridge)

### Installation

```bash
git clone https://github.com/YOUR_USERNAME/blast-radius-prophet
cd blast-radius-prophet
pip install -r requirements.txt
```

### Splunk Setup

1. Install Splunk Enterprise and apply Developer License
2. Install Splunk MCP Server from Splunkbase (app ID: 7931)
3. Create role `mcp_user` with capabilities `mcp_tool_execute` + `mcp_tool_admin`
4. Generate MCP Encrypted Token from within the MCP Server app
5. Enable HEC and ingest sample data:

```bash
python3 scripts/generate_mock_data.py
```

### Run

```bash
# Save your MCP token
echo 'YOUR_MCP_ENCRYPTED_TOKEN' > mcp_token.txt

# Run agents directly
python3 agents/blast_radius_prophet_mcp.py

# Or run Flask backend + UI
python3 backend/app.py
# Open http://localhost:5001
```

---

## Track

**Observability** — Splunk Agentic Ops Hackathon 2026

---

## License

MIT — see LICENSE file.
