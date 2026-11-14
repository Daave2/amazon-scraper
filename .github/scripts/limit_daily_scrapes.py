"""Allow at most two full scheduled scrapes per UK day in the overnight window.

The workflow is triggered every hour in UTC, but GitHub may deliver schedule
events hours late. Scrape only when the actual UK clock is 01:00–05:29.
Only the first two registered runs created in that UK window count.
Manual workflow_dispatch runs remain unrestricted.
"""
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

UK = ZoneInfo("Europe/London")
SCRAPER_RUN_NAME = "Scheduled scraper"
MAX_DAILY_RUNS = 2
EARLIEST_SCRAPE_HOUR_UK = 1
LATEST_SCRAPE_HOUR_UK = 5
LATEST_SCRAPE_MINUTE_UK = 29


def in_overnight_window(when):
    return (
        EARLIEST_SCRAPE_HOUR_UK <= when.hour <= LATEST_SCRAPE_HOUR_UK
        and (when.hour != LATEST_SCRAPE_HOUR_UK or when.minute <= LATEST_SCRAPE_MINUTE_UK)
    )


def scheduled_runs():
    repository = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    all_runs = []

    for page in range(1, 6):
        params = urllib.parse.urlencode(
            {"event": "schedule", "per_page": 100, "page": page}
        )
        url = (
            f"https://api.github.com/repos/{repository}/actions/"
            f"workflows/run-scraper.yml/runs?{params}"
        )
        request = urllib.request.Request(
            url,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "overnight-scraper-gate",
            },
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            data = json.load(response)

        batch = data.get("workflow_runs", [])
        all_runs.extend(batch)
        if len(batch) < 100:
            break
    return all_runs


def uk_run_time(run):
    """Timestamp when GitHub created the scheduled workflow run, in UK time."""
    created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00"))
    return created.astimezone(UK)


def is_eligible(run, day):
    when = uk_run_time(run)
    return (
        run.get("display_title") == SCRAPER_RUN_NAME
        and when.date() == day
        and in_overnight_window(when)
    )


def set_gate(should_run):
    result = "true" if should_run else "false"
    with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as handle:
        handle.write(f"run_scrape={result}\n")


def main():
    if os.environ["GITHUB_EVENT_NAME"] == "workflow_dispatch":
        print("Manual run: unrestricted full scrape")
        set_gate(True)
        return

    if os.environ["GITHUB_EVENT_NAME"] != "schedule":
        raise RuntimeError("Unexpected workflow trigger")

    run_id = int(os.environ["GITHUB_RUN_ID"])
    runs = scheduled_runs()
    current = next((run for run in runs if run["id"] == run_id), None)
    if current is None:
        raise RuntimeError("Current run not found in GitHub API; refusing an uncounted scrape")
    if current.get("display_title") != SCRAPER_RUN_NAME:
        raise RuntimeError("Current run is not labelled as a scheduled scraper run")

    current_time = uk_run_time(current)
    actual_time = datetime.now(UK)
    day = current_time.date()
    if not in_overnight_window(current_time) or not in_overnight_window(actual_time) or day != actual_time.date():
        print(
            f"Scheduled run {run_id}: created {current_time.isoformat()}, "
            f"gate executed {actual_time.isoformat()}; outside 01:00–05:29 UK "
            "or moved into another UK day. Skipping without consuming a slot."
        )
        set_gate(False)
        return

    candidates = sorted(
        (run for run in runs if is_eligible(run, day)),
        key=lambda run: (run["created_at"], run["id"]),
    )
    first_two_ids = [run["id"] for run in candidates[:MAX_DAILY_RUNS]]
    should_run = run_id in first_two_ids

    print(
        f"UK day {day}; scheduled scraper run {run_id}; "
        f"registered runs today: {len(candidates)}; "
        f"first two run IDs: {first_two_ids}; "
        f"full scrape: {should_run}"
    )
    set_gate(should_run)


if __name__ == "__main__":
    main()
