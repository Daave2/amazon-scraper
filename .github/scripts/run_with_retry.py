"""Run the scraper and retry once when any store was not submitted.

The scraper can log an internal error yet return exit code 0, so use the
successfully submitted store names in output/submissions.jsonl as the test.
"""
import csv
import json
import os
import subprocess
import sys
import time


def expected_stores():
    with open("urls.csv", encoding="utf-8-sig", newline="") as source:
        stores = {
            row["store_name"].strip().casefold()
            for row in csv.DictReader(source)
            if row.get("store_name", "").strip()
        }
    if not stores:
        raise RuntimeError("No stores found in urls.csv")
    return stores


def submitted_stores():
    path = "output/submissions.jsonl"
    stores = set()
    if not os.path.exists(path):
        return stores
    with open(path, encoding="utf-8") as source:
        for line in source:
            try:
                record = json.loads(line)
                name = record.get("store", "")
                if isinstance(name, str) and name.strip():
                    stores.add(name.strip().casefold())
            except (ValueError, AttributeError):
                continue
    return stores


def main():
    expected = expected_stores()
    print(f"Expected stores: {len(expected)}", flush=True)

    for attempt in range(1, 3):
        print(f"Scraper attempt {attempt}/2", flush=True)
        try:
            process = subprocess.run([sys.executable, "scraper.py"], timeout=25 * 60, check=False)
            exit_code = process.returncode
        except subprocess.TimeoutExpired:
            exit_code = 124
            print("Scraper attempt timed out after 25 minutes", flush=True)

        submitted = submitted_stores()
        missing = sorted(expected - submitted)
        print(
            f"Attempt {attempt}: exit code={exit_code}, "
            f"verified submissions={len(expected) - len(missing)}/{len(expected)}",
            flush=True,
        )
        if not missing and exit_code == 0:
            print("All stores were submitted. No retry needed.", flush=True)
            return 0

        if missing:
            print("Missing stores: " + ", ".join(missing[:20]), flush=True)
        if attempt == 1:
            print("Retrying the full scrape after 20 seconds...", flush=True)
            time.sleep(20)

    print("Scraper still incomplete after two attempts; marking this job failed.", flush=True)
    return 1


if __name__ == "__main__":
    sys.exit(main())
