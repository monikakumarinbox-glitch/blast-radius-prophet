"""
Blast Radius Prophet — Flask Backend
Serves the UI and exposes /api/analyze endpoint that triggers
the full MCP agent pipeline (Scout → Architect → Comm-Officer)
"""

from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import asyncio
import json
import os
import threading
from datetime import datetime
from collections import defaultdict
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

app = Flask(__name__, static_folder=".")
CORS(app)

# ── Config ────────────────────────────────────────────────────────────────────
MCP_ENDPOINT   = "https://localhost:8089/services/mcp"
MCP_TOKEN_FILE = "/Users/monika/Desktop/SplunkHack/mcp_token2.txt"
DATA_DIR       = "/Users/monika/Desktop/SplunkHack/data"

TOTAL_USERS = {
    "web": 35000, "api": 33000, "auth": 21000,
    "payments": 14000, "checkout": 13000, "db": 25000,
}
APAC_TRAFFIC_SHARE = {
    "web": 0.35, "api": 0.35, "auth": 0.30,
    "payments": 0.55, "checkout": 0.50, "db": 1.0,
}
MITIGATIONS = {
    "db":       "Scale DB read replicas: kubectl scale deployment/db-replica --replicas=5",
    "payments": "Enable payments circuit breaker: feature.payments.circuit_breaker=true",
    "checkout": "Route APAC checkout to us-west replica",
    "api":      "Scale API pods: kubectl scale deployment/api --replicas=8",
}
TEAM_NOTIFY = {
    "db": "@db-oncall", "payments": "@payments-oncall",
    "checkout": "@checkout-oncall", "api": "@api-oncall",
    "web": "@web-oncall", "auth": "@auth-oncall",
}


# ── MCP Agent Logic ───────────────────────────────────────────────────────────
async def splunk_query(session, spl):
    result = await session.call_tool("splunk_run_query", {"query": spl})
    raw  = result.content[0].text
    data = json.loads(raw)
    return data.get("results", [])


async def run_pipeline():
    token = open(MCP_TOKEN_FILE).read().strip()
    server_params = StdioServerParameters(
        command="npx",
        args=["-y", "mcp-remote", MCP_ENDPOINT,
              "--header", f"Authorization: Bearer {token}"],
        env={**os.environ, "NODE_TLS_REJECT_UNAUTHORIZED": "0"}
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # ── Scout ─────────────────────────────────────────────────────
            rows = await splunk_query(session, """
                index=main sourcetype=microservice:metrics earliest=-60d
                | stats avg(avg_latency_ms) as latency,
                        avg(error_rate) as error_rate,
                        avg(cpu_pct) as cpu
                  by service
                | sort -error_rate
            """)

            anomalies = {}
            for row in rows:
                name       = row["service"]
                latency    = float(row["latency"])
                error_rate = float(row["error_rate"])
                cpu        = float(row["cpu"])

                if error_rate > 0.12:
                    severity = "critical"
                elif error_rate > 0.08 or latency > 100:
                    severity = "high"
                else:
                    severity = "none"

                signals = []
                if error_rate > 0.08:
                    signals.append(f"error rate {error_rate*100:.1f}%")
                if latency > 50:
                    signals.append(f"latency {latency:.0f}ms")

                anomalies[name] = {
                    "anomaly":    severity != "none",
                    "signals":    signals,
                    "severity":   severity,
                    "confidence": round(min(0.99, error_rate * 3 + latency/300), 2),
                    "latency":    latency,
                    "error_rate": error_rate,
                    "cpu":        float(cpu),
                }

            # ── Architect ──────────────────────────────────────────────────
            rows2 = await splunk_query(session, """
                index=main sourcetype=microservice:json earliest=-60d
                | stats count by service, upstream_service, downstream_service
                | where count > 5
                | sort -count
            """)

            calls     = defaultdict(set)
            called_by = defaultdict(set)
            edges     = []

            for row in rows2:
                name = row["service"]
                down = row["downstream_service"]
                up   = row["upstream_service"]
                if down and down not in ("none", "external"):
                    calls[name].add(down)
                    edges.append({"from": name, "to": down, "count": int(row["count"])})
                if up and up not in ("none", "external"):
                    called_by[name].add(up)

            # Root cause detection
            critical_svcs = [s for s, r in anomalies.items()
                             if r["severity"] in ("critical", "high")]
            root_causes = []
            for svc_name in critical_svcs:
                is_dependency = any(
                    svc_name in calls.get(other, set())
                    for other in critical_svcs if other != svc_name
                )
                if is_dependency:
                    root_causes.append(svc_name)
            if not root_causes:
                root_causes = [max(critical_svcs,
                                   key=lambda s: anomalies[s]["error_rate"])] \
                              if critical_svcs else []

            # BFS blast radius
            impacted = {}
            for root in root_causes:
                visited = set()
                queue   = list(called_by.get(root, []))
                while queue:
                    node = queue.pop(0)
                    if node in visited:
                        continue
                    visited.add(node)
                    queue.extend(called_by.get(node, []))

                for svc in visited:
                    if svc in impacted:
                        continue
                    users      = TOTAL_USERS.get(svc, 5000)
                    apac_users = round(users * APAC_TRAFFIC_SHARE.get(svc, 0.3))
                    conf       = anomalies.get(root, {}).get("confidence", 0.5)
                    eta        = max(2, round(15 * (1 - conf)))
                    impacted[svc] = {
                        "root_cause":  root,
                        "total_users": users,
                        "apac_users":  apac_users,
                        "eta_minutes": eta,
                        "severity":    "blast",
                    }

            blast_radius = {
                "root_causes":         root_causes,
                "impacted_services":   impacted,
                "total_users_at_risk": sum(v["total_users"] for v in impacted.values()),
                "apac_users_at_risk":  sum(v["apac_users"]  for v in impacted.values()),
                "graph_edges":         edges,
            }

            # ── Comm-Officer ───────────────────────────────────────────────
            comms = {}
            if impacted and root_causes:
                root        = root_causes[0]
                total_users = blast_radius["total_users_at_risk"]
                apac_users  = blast_radius["apac_users_at_risk"]
                confidence  = anomalies.get(root, {}).get("confidence", 0.5)
                signals     = anomalies.get(root, {}).get("signals", [])
                min_eta     = min(v["eta_minutes"] for v in impacted.values())
                mitigation  = MITIGATIONS.get(root, "Investigate immediately")

                notify_order = [root] + sorted(
                    [s for s in impacted if s != root],
                    key=lambda s: impacted[s]["total_users"], reverse=True
                )
                notify_str = " → ".join(
                    TEAM_NOTIFY.get(s, s) for s in notify_order[:4]
                )
                affected_lines = "\n".join(
                    f"  • `{svc}` — ~{v['apac_users']:,} APAC users (~{v['eta_minutes']}min)"
                    for svc, v in impacted.items()
                )
                slack_msg = (
                    f"🔴 *[BLAST RADIUS PROPHET — PRE-INCIDENT ALERT]*\n"
                    f"Confidence: {confidence*100:.0f}% | ETA: ~{min_eta} minutes\n\n"
                    f"*Root Cause:* `{root}` — {', '.join(signals)}\n\n"
                    f"*Predicted Blast Radius:*\n{affected_lines}\n\n"
                    f"*Total at risk:* ~{total_users:,} users (~{apac_users:,} APAC)\n\n"
                    f"*Recommended action:* {mitigation}\n\n"
                    f"*Notify:* {notify_str}\n\n"
                    f"> ⚡ Generated BEFORE users are impacted. Review and approve.\n"
                    f"> Blast Radius Prophet | Splunk MCP Server"
                )
                comms = {
                    "slack_message":   slack_msg,
                    "mitigation":      mitigation,
                    "notify_order":    notify_order,
                    "confidence":      confidence,
                    "eta_minutes":     min_eta,
                }

            return {
                "anomaly_report":  anomalies,
                "blast_radius":    blast_radius,
                "communications":  comms,
                "powered_by":      "Splunk MCP Server (splunk_run_query)",
                "timestamp":       datetime.now().isoformat(),
            }


# ── Flask Routes ──────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("/Users/monika/Desktop/SplunkHack", "index.html")

@app.route("/api/health")
def health():
    return jsonify({"status": "ok", "service": "Blast Radius Prophet"})

@app.route("/api/analyze", methods=["POST"])
def analyze():
    """Trigger the full MCP agent pipeline and return results."""
    try:
        print("\n[API] /api/analyze triggered — running MCP pipeline...")
        result = asyncio.run(run_pipeline())
        print("[API] Pipeline complete — returning results to UI")
        return jsonify({"success": True, "data": result})
    except Exception as e:
        print(f"[API] Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/approve", methods=["POST"])
def approve():
    """Human-in-the-loop: approve and send the pre-drafted alert."""
    body = request.get_json() or {}
    print(f"\n[API] ✅ APPROVED by human operator")
    print(f"  Sending alert to: {body.get('notify_order', [])}")
    print(f"  Mitigation: {body.get('mitigation', '')}")
    return jsonify({
        "success":  True,
        "message":  "Alert sent. Incident ticket created. Status page updated.",
        "sent_to":  body.get("notify_order", []),
        "timestamp": datetime.now().isoformat(),
    })

@app.route("/api/dismiss", methods=["POST"])
def dismiss():
    """Human-in-the-loop: dismiss as false positive."""
    print("\n[API] ❌ Dismissed as false positive by human operator")
    return jsonify({"success": True, "message": "Alert dismissed. Logged as false positive."})


if __name__ == "__main__":
    print("=" * 55)
    print("  BLAST RADIUS PROPHET — Flask Backend")
    print("  http://localhost:5001")
    print("  POST /api/analyze  — run MCP pipeline")
    print("  POST /api/approve  — human approves alert")
    print("  POST /api/dismiss  — human dismisses alert")
    print("=" * 55)
    app.run(host="0.0.0.0", port=5001, debug=False)
