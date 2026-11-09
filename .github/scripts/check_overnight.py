"""Alert Google Chat when overnight scraper runs or store submissions are missing."""

import csv
import io
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

UK = ZoneInfo("Europe/London")
SLOTS = {"15 1 * * *": "01:15", "15 4 * * *": "04:15"}
TIMEOUT = 35


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    """Do not forward GitHub API authorization to artifact storage hosts."""

    def redirect_request(self, request, fp, code, msg, headers, newurl):
        if urllib.parse.urlsplit(request.full_url).hostname != urllib.parse.urlsplit(newurl).hostname:
            headers = {k: v for k, v in headers.items() if k.lower() not in ("authorization", "cookie")}
        return super().redirect_request(request, fp, code, msg, headers, newurl)


opener = urllib.request.build_opener(SafeRedirect())


def request(url, *, payload=None, bearer=None):
    headers = {"User-Agent": "overnight-scraper-monitor", "Accept": "application/vnd.github+json"}
    if bearer:
        headers["Authorization"] = f"Bearer {bearer}"
        headers["X-GitHub-Api-Version"] = "2022-11-28"
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=payload, headers=headers)
    with opener.open(req, timeout=TIMEOUT) as response:
        return response.read()


def github_api(path):
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    base = f"https://api.github.com/repos/{repo}"
    return json.loads(request(base + path, bearer=token))


def expected_stores():
    with open("urls.csv", encoding="utf-8-sig", newline="") as handle:
        return {
            row["store_name"].strip().casefold(): row["store_name"].strip()
            for row in csv.DictReader(handle)
            if row.get("store_name", "").strip()
        }


def recent_scheduled_runs(day):
    found = []
    for page in range(1, 6):
        result = github_api(
            f"/actions/workflows/run-scraper.yml/runs?event=schedule&per_page=100&page={page}"
        )
        batch = result.get("workflow_runs", [])
        if not batch:
            break
        old = False
        for run in batch:
            created = datetime.fromisoformat(run["created_at"].replace("Z", "+00:00")).astimezone(UK)
            if created.date() == day:
                found.append(run)
            elif created.date() < day:
                old = True
        if old or len(batch) < 100:
            break
    return found


def read_report(run_id):
    listing = github_api(f"/actions/runs/{run_id}/artifacts?per_page=100")
    for artifact in listing.get("artifacts", []):
        if artifact.get("name") != f"scraper-status-{run_id}" or artifact.get("expired"):
            continue
        url = f"https://api.github.com/repos/{os.environ['GITHUB_REPOSITORY']}/actions/artifacts/{artifact['id']}/zip"
        archive = request(url, bearer=os.environ["GH_TOKEN"])
        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            matches = [name for name in zf.namelist() if name.endswith("scraper-status.json")]
            if not matches:
                raise ValueError(f"Status artifact for run {run_id} has no report")
            return json.loads(zf.read(matches[0]))
    return None


def alert(message):
    webhook = os.environ.get("CHAT_WEBHOOK_URL", "").strip()
    if not webhook:
        raise RuntimeError("CHAT_WEBHOOK_URL is not configured")
    request(webhook, payload=json.dumps({"text": message}).encode("utf-8"))
    print("Google Chat alert sent")


def audit():
    day = datetime.now(UK).date()
    expected = expected_stores()
    if not expected:
        raise ValueError("No expected stores found in urls.csv")

    slots = {key: [] for key in SLOTS}
    reported = set()
    for run in recent_scheduled_runs(day):
        try:
            report = read_report(run["id"])
        except (OSError, ValueError, KeyError, zipfile.BadZipFile) as exc:
            print(f"Cannot read report for run {run['id']}: {exc}")
            continue
        if report is None:
            continue
        cron = report.get("scheduled_cron")
        if cron not in slots or report.get("uk_date") != day.isoformat() or report.get("event") != "schedule":
            continue
        slots[cron].append((run, report))
        reported.update(str(name).strip().casefold() for name in report.get("submitted_stores", []))

    missing = sorted(set(expected) - reported)
    absent = [SLOTS[cron] for cron, runs in slots.items() if not runs]
    # Re-runs of the same scheduled slot are acceptable: any successful attempt counts.
    all_failed = all(
        runs and not any(run.get("conclusion") == "success" for run, _ in runs)
        for runs in slots.values()
    )
    all_failed = bool(all_failed and all(slots.values()))

    print(f"UK date: {day}; confirmed stores: {len(expected) - len(missing)}/{len(expected)}")
    print("Scheduled slots with reports:", {SLOTS[key]: len(value) for key, value in slots.items()})
    if not absent and not missing and not all_failed:
        print("Overnight scraper healthy; no Google Chat alert")
        return

    lines = [
        f"🚨 Overnight Amazon scraper alert — {day.strftime('%d %b %Y')}",
        f"Verified submissions: {len(expected) - len(missing)}/{len(expected)} stores",
    ]
    for cron, label in SLOTS.items():
        runs = slots[cron]
        if not runs:
            lines.append(f"{label}: no verifiable scheduled run by the morning check")
        else:
            results = ", ".join(f"#{run['run_number']} {run.get('conclusion') or run.get('status')}" for run, _ in runs)
            lines.append(f"{label}: {results}")
    if all_failed:
        lines.append("Both scraper workflow runs finished without a successful GitHub result.")
    if missing:
        store_list = ", ".join(expected[key] for key in missing[:20])
        if len(missing) > 20:
            store_list += f" (+{len(missing) - 20} more)"
        lines.append(f"No successful form submission recorded for: {store_list}")
    lines.append(f"See: https://github.com/{os.environ['GITHUB_REPOSITORY']}/actions/workflows/run-scraper.yml")
    alert("\n".join(lines))


def main():
    try:
        audit()
    except Exception as exc:
        print(f"Monitoring error: {type(exc).__name__}: {exc}", file=sys.stderr)
        try:
            alert(
                f"🚨 Overnight scraper monitoring couldn't verify today's runs: "
                f"{type(exc).__name__}: {exc}\n"
                f"https://github.com/{os.environ.get('GITHUB_REPOSITORY', 'Daave2/amazon-scraper')}/actions"
            )
        finally:
            raise


if __name__ == "__main__":
    main()
