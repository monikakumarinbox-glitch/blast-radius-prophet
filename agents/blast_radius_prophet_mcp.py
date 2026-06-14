"""
Blast Radius Prophet — MCP Version
All Splunk queries go through the official Splunk MCP Server
via mcp-remote bridge using the Python mcp library.
"""

import asyncio
import json
import os
from collections import defaultdict
from datetime import datetime
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ── Config ───────────────────────────────────────────────────────────────────
MCP_ENDPOINT  = "https://localhost:8089/services/mcp"
MCP_TOKEN_FILE = "/Users/monika/Desktop/SplunkHack/mcp_token2.txt"

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


# ── MCP Query Helper ──────────────────────────────────────────────────────────
async def splunk_query(session: ClientSession, spl: str) -> list:
    """Call splunk_run_query MCP tool and return parsed results."""
    result = await session.call_tool("splunk_run_query", {"query": spl})
    raw = result.content[0].text
    data = json.loads(raw)
    return data.get("results", [])


# ── Agent 1: Scout ────────────────────────────────────────────────────────────
async def scout_agent(session: ClientSession) -> dict:
    print("\n[Scout] Scanning metrics via Splunk MCP splunk_run_query...")

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
        if cpu > 50:
            signals.append(f"CPU {cpu:.0f}%")

        confidence = min(0.99, error_rate * 3 + (latency / 300))

        anomalies[name] = {
            "anomaly":    severity != "none",
            "signals":    signals,
            "severity":   severity,
            "confidence": round(confidence, 2),
            "latency":    latency,
            "error_rate": error_rate,
        }

        icon = "⚠ " if severity != "none" else "✓ "
        print(f"  {icon} {name}: latency={latency:.0f}ms, "
              f"error={error_rate*100:.1f}%, severity={severity}")

    return anomalies


# ── Agent 2: Architect ────────────────────────────────────────────────────────
async def architect_agent(session: ClientSession, anomalies: dict) -> dict:
    print("\n[Architect] Inferring dependency graph via Splunk MCP...")

    rows = await splunk_query(session, """
        index=main sourcetype=microservice:json earliest=-60d
        | stats count by service, upstream_service, downstream_service
        | where count > 5
        | sort -count
    """)

    print(f"  Graph edges from MCP: {len(rows)} observed call patterns")

    calls     = defaultdict(set)
    called_by = defaultdict(set)

    for row in rows:
        name = row["service"]
        down = row["downstream_service"]
        up   = row["upstream_service"]
        if down and down not in ("none", "external"):
            calls[name].add(down)
        if up and up not in ("none", "external"):
            called_by[name].add(up)

    print(f"  Inferred graph: {dict({k: list(v) for k,v in calls.items()})}")

    # Find true root cause: critical service that others depend on
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

    print(f"  Root cause(s): {root_causes}")

    # BFS upstream to find all impacted services
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

        for affected_svc in visited:
            if affected_svc in impacted:
                continue
            users      = TOTAL_USERS.get(affected_svc, 5000)
            apac_users = round(users * APAC_TRAFFIC_SHARE.get(affected_svc, 0.3))
            conf       = anomalies.get(root, {}).get("confidence", 0.5)
            eta        = max(2, round(15 * (1 - conf)))

            impacted[affected_svc] = {
                "root_cause":  root,
                "total_users": users,
                "apac_users":  apac_users,
                "eta_minutes": eta,
            }
            print(f"  → {affected_svc} at risk: "
                  f"~{apac_users:,} APAC users (~{eta}min ETA)")

    total_users = sum(v["total_users"] for v in impacted.values())
    apac_total  = sum(v["apac_users"]  for v in impacted.values())
    print(f"  Total: {len(impacted)} services, "
          f"~{total_users:,} users, ~{apac_total:,} APAC")

    return {
        "root_causes":         root_causes,
        "impacted_services":   impacted,
        "total_users_at_risk": total_users,
        "apac_users_at_risk":  apac_total,
    }


# ── Agent 3: Comm-Officer ─────────────────────────────────────────────────────
async def comm_officer_agent(anomalies: dict, blast_radius: dict) -> dict:
    print("\n[Comm-Officer] Drafting pre-incident communications...")

    if not blast_radius.get("impacted_services"):
        print("  Nothing to draft — no blast radius detected.")
        return {}

    root        = blast_radius["root_causes"][0]
    impacted    = blast_radius["impacted_services"]
    total_users = blast_radius["total_users_at_risk"]
    apac_users  = blast_radius["apac_users_at_risk"]
    confidence  = anomalies.get(root, {}).get("confidence", 0.5)
    signals     = anomalies.get(root, {}).get("signals", [])
    min_eta     = min(v["eta_minutes"] for v in impacted.values())
    mitigation  = MITIGATIONS.get(root, "Investigate root cause immediately")

    notify_order = [root] + sorted(
        [s for s in impacted if s != root],
        key=lambda s: impacted[s]["total_users"],
        reverse=True
    )
    notify_str = " → ".join(TEAM_NOTIFY.get(s, s) for s in notify_order[:4])

    affected_lines = "\n".join(
        f"  • `{svc}` — ~{v['apac_users']:,} APAC users (~{v['eta_minutes']}min)"
        for svc, v in impacted.items()
    )

    slack_msg = f"""🔴 *[BLAST RADIUS PROPHET — PRE-INCIDENT ALERT]*
Confidence: {confidence*100:.0f}% | ETA to user impact: ~{min_eta} minutes

*Root Cause:* `{root}` — {', '.join(signals)}

*Predicted Blast Radius:*
{affected_lines}

*Total at risk:* ~{total_users:,} users (~{apac_users:,} APAC)

*Recommended action:* {mitigation}

*Notify:* {notify_str}

> ⚡ Generated BEFORE users are impacted. Review and approve.
> Blast Radius Prophet | Splunk MCP Server"""

    print(slack_msg)
    return {
        "slack_message": slack_msg,
        "mitigation":    mitigation,
        "notify_order":  notify_order,
    }


# ── Orchestrator ──────────────────────────────────────────────────────────────
async def run_blast_radius_prophet():
    print("=" * 60)
    print("  BLAST RADIUS PROPHET — MCP MODE")
    print("  Powered by Splunk MCP Server (splunk_run_query)")
    print("=" * 60)

    token = open(MCP_TOKEN_FILE).read().strip()

    server_params = StdioServerParameters(
        command="npx",
        args=[
            "-y", "mcp-remote",
            MCP_ENDPOINT,
            "--header", f"Authorization: Bearer {token}"
        ],
        env={**os.environ, "NODE_TLS_REJECT_UNAUTHORIZED": "0"}
    )

    print("\nConnecting to Splunk MCP Server...")
    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Verify available tools
            tools = await session.list_tools()
            tool_names = [t.name for t in tools.tools]
            print(f"✓ MCP connected — {len(tool_names)} tools available")
            print(f"  Using: splunk_run_query for all agent queries")

            # Run three agents
            start = datetime.now()
            anomalies    = await scout_agent(session)
            blast_radius = await architect_agent(session, anomalies)
            comms        = await comm_officer_agent(anomalies, blast_radius)
            elapsed      = (datetime.now() - start).total_seconds()

            print("\n" + "=" * 60)
            print("  PIPELINE COMPLETE (via Splunk MCP)")
            print(f"  Root cause:       {blast_radius.get('root_causes', [])}")
            print(f"  Services at risk: {list(blast_radius.get('impacted_services', {}).keys())}")
            print(f"  Users at risk:    ~{blast_radius.get('total_users_at_risk', 0):,}")
            print(f"  APAC at risk:     ~{blast_radius.get('apac_users_at_risk', 0):,}")
            print(f"  Analysis time:    {elapsed:.1f}s")
            print("=" * 60)

            # Save output for Flask/UI consumption
            output = {
                "anomaly_report":  anomalies,
                "blast_radius":    blast_radius,
                "communications":  comms,
                "analysis_time_s": round(elapsed, 1),
                "powered_by":      "Splunk MCP Server (splunk_run_query)"
            }
            with open("/Users/monika/Desktop/SplunkHack/data/prophet_output_mcp.json", "w") as f:
                json.dump(output, f, indent=2)
            print("\nOutput saved to data/prophet_output_mcp.json")

            return output


if __name__ == "__main__":
    asyncio.run(run_blast_radius_prophet())
