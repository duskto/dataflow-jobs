"""Prove scraping, firing, Alertmanager delivery and automatic recovery in the test stack."""

import argparse
import json
import time
from pathlib import Path

import httpx

from scripts.celery_benchmark import compose, verification_env


def poll(predicate, timeout=100):
    started = time.monotonic()
    while time.monotonic() - started < timeout:
        try:
            if predicate():
                return round(time.monotonic() - started, 2)
        except httpx.HTTPError:
            pass
        time.sleep(2)
    raise TimeoutError("Monitoring condition did not become true")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="dataflow-verification")
    parser.add_argument("--port", type=int, default=18000)
    parser.add_argument("--output", type=Path, default=Path("docs/monitoring-verification.json"))
    args = parser.parse_args()
    env = verification_env(args.port)
    compose(args.project, env, "--profile", "monitoring", "up", "-d", "--no-build", "prometheus")
    report = {}
    with httpx.Client(trust_env=False, timeout=10) as client:
        prometheus = "http://127.0.0.1:19090"
        manager = "http://127.0.0.1:19093"
        api = f"http://127.0.0.1:{args.port}/api/v1"

        def query(expression):
            response = client.get(prometheus + "/api/v1/query", params={"query": expression})
            response.raise_for_status()
            return response.json()["data"]["result"]

        poll(
            lambda: (
                query('up{job="dataflow"}') and query('up{job="dataflow"}')[0]["value"][1] == "1"
            )
        )
        report["scrape_up"] = True
        hist = query("dataflow_attempt_duration_seconds_count")
        report["recorded_attempts"] = int(hist[0]["value"][1])
        assert report["recorded_attempts"] > 0
        active = client.get(api + "/ops").json()["jobs"]
        assert all(active[state] == 0 for state in ("QUEUED", "RUNNING", "RETRY_WAIT"))
        try:
            compose(args.project, env, "stop", "worker")
            report["worker_offline_detection_seconds"] = poll(
                lambda: client.get(api + "/ops").json()["components_online"]["worker"] == 0
            )

            def firing():
                alerts = client.get(prometheus + "/api/v1/alerts").json()["data"]["alerts"]
                return any(
                    a["labels"].get("code") == "NO_WORKERS" and a["state"] == "firing"
                    for a in alerts
                )

            report["prometheus_firing_wait_seconds"] = poll(firing)
            report["alertmanager_delivery_wait_seconds"] = poll(
                lambda: any(
                    a["labels"].get("code") == "NO_WORKERS"
                    for a in client.get(manager + "/api/v2/alerts").json()
                )
            )
            print(
                "PASS: worker outage detected; Prometheus firing; Alertmanager received alert",
                flush=True,
            )
        finally:
            compose(args.project, env, "start", "worker")
        report["recovery_seconds"] = poll(
            lambda: (
                query('dataflow_alert_active{code="NO_WORKERS"}')
                and query('dataflow_alert_active{code="NO_WORKERS"}')[0]["value"][1] == "0"
            )
        )
        poll(
            lambda: (
                not any(
                    a["labels"].get("code") == "NO_WORKERS"
                    for a in client.get(prometheus + "/api/v1/alerts").json()["data"]["alerts"]
                )
            )
        )
        report["alert_resolved"] = True
        report["external_notifications_sent"] = False
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS: worker recovery automatically resolved alert; {args.output}")


if __name__ == "__main__":
    main()
