# Amazon Seller Central Scraper

This repository contains an asynchronous Playwright-based scraper for collecting metrics from Amazon Seller Central dashboards. The results are posted directly to a Google Form so they can be aggregated in Google Sheets.

The code can be run locally or through the provided GitHub Actions workflow.

## Features

- Automates the sign-in flow for Seller Central including two-step verification
- Collects metrics for multiple stores listed in `urls.csv`
- Posts metrics to a configurable Google Form
- Supports configurable concurrency and automatic adjustments based on system load
- Produces structured logs in `output/` and rotates `app.log`

## Requirements

- Python 3.11
- Playwright with Chromium browsers
- See `requirements.txt` for the full list of Python packages

## Setup

1. Install Python dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy the example configuration and edit it with your credentials:

   ```bash
   cp config.example.json config.json
   # then edit config.json
   ```

   Important fields include your Seller Central login details, the target Google Form URL, and concurrency settings. The example file contains all available keys.

3. Populate `urls.csv` with the stores you want to scrape. Each row uses the following columns:
   `merchant_id,new_id,store_name,marketplace_id`.

## Running Locally

Execute the scraper from the command line:

```bash
python scraper.py
```

Logs and submission data will be saved under the `output/` directory.

## GitHub Actions Workflow

The workflow defined in `.github/workflows/run-scraper.yml` installs dependencies, builds a `config.json` from repository secrets, and runs the scraper on a schedule. It checks the current UK time against `UK_TARGET_HOURS` to decide whether to proceed with a run.

Artifacts such as logs are uploaded for each run and kept for seven days.

## Configuration Reference

Key options from `config.example.json`:

- `login_email` / `login_password` – Seller Central credentials
- `otp_secret_key` – secret for generating two-step verification codes
- `form_url` – Google Form to submit scraped metrics
- `initial_concurrency` – number of concurrent browser workers
- `num_form_submitters` – number of HTTP workers sending form data
- `auto_concurrency` – optional automatic scaling of concurrency limits. When enabled, the scraper adjusts `concurrency_limit` between `min_concurrency` and `max_concurrency` based on CPU and memory load.

See the example file for full details.

## Notes

The repository excludes `config.json`, `state.json`, and `output/` from version control. These files may contain sensitive information or large log data. Ensure you keep your credentials secure.

