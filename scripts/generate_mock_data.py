"""
Blast Radius Prophet — Mock Data Generator
Generates realistic Splunk-ingestible logs for a 6-service microservices stack.
Produces two phases: NORMAL traffic and DEGRADED (DB anomaly) traffic.
"""

import json
import random
import time
from datetime import datetime, timedelta

# ── Service topology (the "real" graph we want the agent to infer) ──────────
# web → api → auth        (auth path)
#           → payments → db  (payment path)
#           → checkout → db  (checkout path)

SERVICES = ["web", "api", "auth", "payments", "checkout", "db"]

CALL_GRAPH = {
    "web":      ["api"],
    "api":      ["auth", "payments", "checkout"],
    "auth":     [],
    "payments": ["db"],
    "checkout": ["db"],
    "db":       [],
}

REGIONS = {
    "web":      ["us-west", "apac", "emea"],
    "api":      ["us-west", "apac", "emea"],
    "auth":     ["us-west", "apac"],
    "payments": ["apac", "us-west"],
    "checkout": ["apac", "emea"],
    "db":       ["apac"],          # single-region DB — blast radius is APAC-heavy
}

BASE_LATENCY = {   # ms
    "web":      45,
    "api":      60,
    "auth":     30,
    "payments": 80,
    "checkout": 75,
    "db":       20,
}

BASE_ERROR_RATE = {   # probability 0–1
    "web":      0.005,
    "api":      0.008,
    "auth":     0.003,
    "payments": 0.01,
    "checkout": 0.01,
    "db":       0.002,
}

TRAFFIC_RPS = {   # requests/second
    "web":      150,
    "api":      140,
    "auth":     90,
    "payments": 60,
    "checkout": 55,
    "db":       110,   # db gets hits from both payments + checkout
}


def splunk_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000+0000")


def jitter(base: float, pct: float = 0.15) -> float:
    return base * (1 + random.uniform(-pct, pct))


def generate_service_log(
    service: str,
    dt: datetime,
    degraded: bool = False,
    degradation_pct: float = 0.0,   # 0.0 → 1.0 ramp
) -> dict:
    """Generate one log event for a service at a given time."""

    downstream = CALL_GRAPH[service]
    region = random.choice(REGIONS[service])

    # ── latency model ─────────────────────────────────────────────────────
    base_lat = BASE_LATENCY[service]
    if degraded and service == "db":
        # DB latency ramps from base → 10x over degradation_pct
        multiplier = 1 + degradation_pct * 9
        latency = jitter(base_lat * multiplier, 0.2)
    elif degraded and service in ("payments", "checkout"):
        # Cascade: payments/checkout slow as DB backs up
        multiplier = 1 + degradation_pct * 4
        latency = jitter(base_lat * multiplier, 0.2)
    elif degraded and service == "api":
        multiplier = 1 + degradation_pct * 1.5
        latency = jitter(base_lat * multiplier, 0.15)
    else:
        latency = jitter(base_lat)

    # ── error rate model ─────────────────────────────────────────────────
    base_err = BASE_ERROR_RATE[service]
    if degraded and service == "db":
        error_prob = min(0.95, base_err + degradation_pct * 0.6)
    elif degraded and service in ("payments", "checkout"):
        error_prob = min(0.7, base_err + degradation_pct * 0.35)
    else:
        error_prob = base_err

    status = "error" if random.random() < error_prob else "ok"
    status_code = random.choice([500, 503, 504]) if status == "error" else 200

    # ── build the log event ───────────────────────────────────────────────
    event = {
        "time":             splunk_timestamp(dt),
        "source":           f"/var/log/{service}/app.log",
        "sourcetype":       "microservice:json",
        "index":            "main",
        "event": {
            "timestamp":        splunk_timestamp(dt),
            "service":          service,
            "upstream_service": random.choice([s for s, ds in CALL_GRAPH.items() if service in ds]) if any(service in ds for ds in CALL_GRAPH.values()) else "external",
            "downstream_service": random.choice(downstream) if downstream else "none",
            "region":           region,
            "latency_ms":       round(latency, 2),
            "status":           status,
            "status_code":      status_code,
            "request_id":       f"req-{random.randint(100000, 999999)}",
            "rps":              round(jitter(TRAFFIC_RPS[service], 0.1), 1),
            "error_message":    random.choice([
                "connection timeout", "pool exhausted", "query too slow"
            ]) if status == "error" else "",
        }
    }
    return event


def generate_metrics_event(service: str, dt: datetime, degraded: bool = False, degradation_pct: float = 0.0) -> dict:
    """Generate a metrics event (for time-series model input)."""
    base_lat = BASE_LATENCY[service]
    base_err = BASE_ERROR_RATE[service]

    if degraded and service == "db":
        avg_latency = base_lat * (1 + degradation_pct * 9)
        error_rate  = min(0.95, base_err + degradation_pct * 0.6)
        cpu_pct     = min(98, 25 + degradation_pct * 70)
        conn_pool_used = min(100, 30 + degradation_pct * 68)
    elif degraded and service in ("payments", "checkout"):
        avg_latency = base_lat * (1 + degradation_pct * 4)
        error_rate  = min(0.7, base_err + degradation_pct * 0.35)
        cpu_pct     = min(85, 20 + degradation_pct * 40)
        conn_pool_used = min(90, 20 + degradation_pct * 50)
    else:
        avg_latency = jitter(base_lat, 0.1)
        error_rate  = jitter(base_err, 0.2)
        cpu_pct     = jitter(22, 0.2)
        conn_pool_used = jitter(25, 0.15)

    return {
        "time":       splunk_timestamp(dt),
        "sourcetype": "microservice:metrics",
        "index":      "metrics",
        "event": {
            "timestamp":       splunk_timestamp(dt),
            "service":         service,
            "avg_latency_ms":  round(avg_latency, 2),
            "error_rate":      round(error_rate, 4),
            "cpu_pct":         round(cpu_pct, 1),
            "conn_pool_used_pct": round(conn_pool_used, 1),
            "rps":             round(jitter(TRAFFIC_RPS[service], 0.1), 1),
        }
    }


def generate_dataset(
    start_time: datetime,
    normal_minutes: int = 15,
    degraded_minutes: int = 15,
    events_per_minute: int = 30,
) -> list:
    """
    Generate a full dataset:
      - normal_minutes of clean traffic
      - degraded_minutes of ramping DB degradation
    """
    events = []
    current = start_time

    print(f"Generating {normal_minutes}min normal + {degraded_minutes}min degraded traffic...")

    # ── Phase 1: Normal ───────────────────────────────────────────────────
    for minute in range(normal_minutes):
        for _ in range(events_per_minute):
            service = random.choice(SERVICES)
            dt = current + timedelta(seconds=random.randint(0, 59))
            events.append(generate_service_log(service, dt, degraded=False))
            events.append(generate_metrics_event(service, dt, degraded=False))
        current += timedelta(minutes=1)
        if minute % 5 == 0:
            print(f"  Normal phase: {minute}/{normal_minutes} min")

    # ── Phase 2: Degraded (ramp) ──────────────────────────────────────────
    for minute in range(degraded_minutes):
        degradation_pct = (minute + 1) / degraded_minutes   # 0 → 1 ramp
        for _ in range(events_per_minute):
            service = random.choice(SERVICES)
            dt = current + timedelta(seconds=random.randint(0, 59))
            events.append(generate_service_log(service, dt, degraded=True, degradation_pct=degradation_pct))
            events.append(generate_metrics_event(service, dt, degraded=True, degradation_pct=degradation_pct))
        current += timedelta(minutes=1)
        if minute % 5 == 0:
            print(f"  Degraded phase: {minute}/{degraded_minutes} min (intensity={degradation_pct:.0%})")

    print(f"Total events generated: {len(events)}")
    return events


def write_splunk_hec_file(events: list, path: str):
    """Write events as newline-delimited JSON for Splunk HEC bulk ingest."""
    with open(path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"Written to {path}")


def write_summary(events: list, path: str):
    """Write a human-readable summary of what was generated."""
    from collections import Counter
    service_counts = Counter()
    error_counts   = Counter()
    for e in events:
        ev = e.get("event", {})
        svc = ev.get("service", "?")
        if e.get("sourcetype") == "microservice:json":
            service_counts[svc] += 1
            if ev.get("status") == "error":
                error_counts[svc] += 1

    with open(path, "w") as f:
        f.write("=== Blast Radius Prophet — Mock Data Summary ===\n\n")
        f.write(f"Total events: {len(events)}\n\n")
        f.write("Service log counts:\n")
        for svc in SERVICES:
            total = service_counts.get(svc, 0)
            errs  = error_counts.get(svc, 0)
            rate  = (errs / total * 100) if total else 0
            f.write(f"  {svc:12s}: {total:5d} events, {errs:4d} errors ({rate:.1f}%)\n")
        f.write("\nDependency graph (ground truth):\n")
        for svc, deps in CALL_GRAPH.items():
            f.write(f"  {svc} → {deps or ['(leaf)']}\n")
    print(f"Summary written to {path}")


if __name__ == "__main__":
    random.seed(42)
    start = datetime(2026, 6, 11, 8, 0, 0)   # demo starts at 08:00

    events = generate_dataset(
        start_time=start,
        normal_minutes=15,
        degraded_minutes=15,
        events_per_minute=40,
    )

    write_splunk_hec_file(events, "/Users/monika/Desktop/SplunkHack/data/mock_logs.jsonl")
    write_summary(events, "/Users/monika/Desktop/SplunkHack/data/summary.txt")
    print("\nDone. Use mock_logs.jsonl for Splunk HEC ingest or local simulation.")
