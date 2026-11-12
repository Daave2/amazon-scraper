"""Check successful scheduled scraper jobs and alert Google Chat if none completed.

The scraper runner returns a failing job when any expected store submission is
missing after its automatic retry. We deliberately check job conclusions via the
GitHub REST API instead of downloading artifacts: artifact ZIP redirects can
fail authentication and previously caused misleading 0/100 alerts.
"""

import json
import os
import sys
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

UK = ZoneInfo("Europe/London")
TIMEOUT = 30


def github_api(path):
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    req = urllib.request.Request(
        f"https://api.github.com/repos/{repo}{path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "overnight-scraper-monitor",
        },
    )
    with urllib.request.urlopen(req, timeout=TIMEOUT) as response:
        return json.load(response)


def scheduled_runs_today(today):
    """Return all scheduled workflow executions created on the UK calendar day."""
    found = []
    for page in range(1, 6):
        response = github_api(
            f"/actions/workflows/run-scraper.yml/runs"
            f"?event=schedule&per_page=100&page={page}"
        )
        batch = response.get("workflow_runs", [])
        if not batch:
            break
        seen_older_day = False
        for run in batch:
            created = datetime.fromisoformat(
                run["created_at"].replace("Z", "+00:00")
            ).astimezone(UK)
            if created.date() == today:
                found.append(run)
            elif created.date() < today:
                seen_older_day = True
        if seen_older_day or len(batch) < 100:
            break
    return found


def scraper_job(run_id):
    """Return the scraper job, excluding the dedicated monitoring-only job."""
    for page in range(1, 4):
        response = github_api(f"/actions/runs/{run_id}/jobs?per_page=100&page={page}")
        batch = response.get("jobs", [])
        for job in batch:
            if job.get("name") == "scrape-and-submit":
                return job
        if len(batch) < 100:
            break
    return None


def notify(message):
    url = os.environ.get("CHAT_WEBHOOK_URL", "").strip()
    if not url:
        raise RuntimeError("CHAT_WEBHOOK_URL is missing")
    request = urllib.request.Request(
        url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json", "User-Agent": "scraper-health-check"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
        if not 200 <= response.status < 300:
            raise RuntimeError(f"Google Chat webhook returned {response.status}")
    print("Google Chat health alert sent", flush=True)


def audit():
    day = datetime.now(UK).date()
    scheduled = scheduled_runs_today(day)
    results = []
    for run in scheduled:
        job = scraper_job(run["id"])
        if job is None:
            # The 05:30 monitor is supposed to have no scrape job.
            continue
        results.append(
            {
                "run_number": run["run_number"],
                "conclusion": job.get("conclusion"),
                "status": job.get("status"),
            }
        )

    success = [r for r in results if r["conclusion"] == "success"]
    print(f"UK date: {day}", flush=True)
    print(f"Scheduled scraper jobs found: {len(results)}", flush=True)
    print(f"Scheduled scrapes completed successfully: {len(success)}", flush=True)
    print(f"Scraper jobs: {results}", flush=True)

    if success:
        print("At least one completed full-store scrape; no alert.", flush=True)
        return

    summary = "No automatically scheduled scraper job has completed successfully."
    if not results:
        summary = "No automatically scheduled scraper run has appeared."
    statuses = ", ".join(
        f"#{r['run_number']}: {r['conclusion'] or r['status'] or 'unknown'}"
        for r in results
    ) or "No scheduled scrape runs recorded"
    notify(
        f"🚨 Overnight Amazon scraper alert — {day.strftime('%d %b %Y')}\n"
        f"{summary}\n"
        f"Scheduled job status: {statuses}\n"
        f"Please check the data before using the morning update.\n"
        f"https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/workflows/run-scraper.yml"
    )


def main():
    try:
        audit()
    except Exception as error:
        print(f"Monitoring failed: {type(error).__name__}: {error}", file=sys.stderr)
        try:
            notify(
                "🚨 Overnight scraper monitoring error — could not check scheduled jobs.\n"
                f"{type(error).__name__}: {error}\n"
                f"https://github.com/{os.environ.get('GITHUB_REPOSITORY', 'Daave2/amazon-scraper')}/actions"
            )
        finally:
            raise


if __name__ == "__main__":
    main()
