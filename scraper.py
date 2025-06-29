# =======================================================================================
#               AMAZON SELLER CENTRAL SCRAPER (CI/CD / COMMAND-LINE VERSION)
# =======================================================================================
# This version is optimized for automated environments like GitHub Actions.
# It includes robust, stabilized login handling for anti-bot pages and race conditions.
# It runs the core data collection and submission process once, then exits.
#
# To run:
# 1. Ensure `config.json` and `urls.csv` are in the same directory.
# 2. Ensure all dependencies from requirements.txt are installed.
# 3. Execute from your terminal: python scraper.py
# =======================================================================================

import logging
import urllib.parse
from datetime import datetime
from pytz import timezone
from playwright.async_api import (
    async_playwright,
    Browser,
    BrowserContext,
    Page,
    TimeoutError,
    expect,
    Error as PlaywrightError,
)
import os
import csv
import json
import asyncio
from asyncio import Queue
from threading import Lock
from typing import Dict
import pyotp
from logging.handlers import RotatingFileHandler
import re
import psutil
import random

#######################################################################
#                            CONFIG & CONSTANTS
#######################################################################

LOCAL_TIMEZONE = timezone('Europe/London')

try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    print("FATAL: config.json not found. Please create it before running.")
    exit(1)
except json.JSONDecodeError:
    print("FATAL: config.json is not valid JSON. Please fix it.")
    exit(1)

DEBUG_MODE      = config.get('debug', False)
FORM_URL        = config['form_url']
LOGIN_URL       = config['login_url']

INITIAL_CONCURRENCY = config.get('initial_concurrency', 8)
NUM_FORM_SUBMITTERS = config.get('num_form_submitters', 4)

LOG_FILE        = os.path.join('output', 'submissions.log')
JSON_LOG_FILE   = os.path.join('output', 'submissions.jsonl')
STORAGE_STATE   = 'state.json'
OUTPUT_DIR      = 'output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_TIMEOUT    = config.get('page_timeout_ms', 90000)
WAIT_TIMEOUT    = config.get('element_wait_timeout_ms', 20000)
ACTION_TIMEOUT = int(PAGE_TIMEOUT / 3)
WORKER_RETRY_COUNT = 3

RESOURCE_BLOCKLIST = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "adservice.google.com", "facebook.net", "fbcdn.net", "analytics.tiktok.com",
]

#######################################################################
#                             APP SETUP & LOGGING
#######################################################################

def setup_logging():
    app_logger = logging.getLogger('app')
    app_logger.setLevel(logging.INFO)
    app_file = RotatingFileHandler('app.log', maxBytes=10**7, backupCount=5)
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    app_file.setFormatter(fmt)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    app_logger.addHandler(app_file)
    app_logger.addHandler(console)
    return app_logger

app_logger = setup_logging()

#######################################################################
#                      GLOBALS
#######################################################################

log_lock      = Lock()
progress_lock = Lock()
urls_data     = []
progress      = {"current": 0, "total": 0, "lastUpdate": "N/A"}
run_failures  = []
start_time    = None

playwright = None
browser = None

concurrency_limit = INITIAL_CONCURRENCY
active_workers_count = 0
concurrency_condition = asyncio.Condition()

#######################################################################
#                          UTILITIES
#######################################################################

async def _save_screenshot(page: Page | None, prefix: str):
    if not page or page.is_closed():
        app_logger.warning(f"Cannot save screenshot '{prefix}': Page is closed or unavailable.")
        return
    try:
        safe_prefix = re.sub(r'[\\/*?:"<>|]', "_", prefix)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = os.path.join(OUTPUT_DIR, f"{safe_prefix}_{timestamp}.png")
        await page.screenshot(path=path, full_page=True, timeout=15000)
        app_logger.info(f"Screenshot saved for debugging: {path}")
    except Exception as e:
        app_logger.error(f"Failed to save screenshot with prefix '{prefix}': {e}")

def load_default_data():
    global urls_data
    urls_data.clear()
    try:
        with open('urls.csv', 'r', newline='') as f:
            reader = csv.reader(f)
            header = next(reader)
            for i, row in enumerate(reader):
                if not row or len(row) < 4:
                    app_logger.warning(f"Skipping malformed row {i+2} in urls.csv: {row}")
                    continue
                urls_data.append({
                    'merchant_id': row[0].strip(),
                    'store_name': row[2].strip(),
                    'marketplace_id': row[3].strip()
                })
        app_logger.info(f"{len(urls_data)} stores loaded from urls.csv")
    except FileNotFoundError:
        app_logger.error("FATAL: 'urls.csv' not found. Please ensure the file exists and is named correctly (all lowercase).")
        raise
    except StopIteration:
        app_logger.error("Failed to load urls.csv: File is empty or has no header.")
    except Exception:
        app_logger.exception("An error occurred while loading urls.csv")

def ensure_storage_state():
    if not os.path.exists(STORAGE_STATE) or os.path.getsize(STORAGE_STATE)==0:
        return False
    try:
        with open(STORAGE_STATE) as f: data=json.load(f)
        if (not isinstance(data, dict) or "cookies" not in data or not isinstance(data["cookies"], list) or not data["cookies"]):
            return False
        return True
    except json.JSONDecodeError:
        return False

#######################################################################
#                     AUTHENTICATION & SESSION PRIMING
#######################################################################
async def check_if_login_needed(page: Page, test_url: str) -> bool:
    app_logger.info(f"Verifying session status by navigating to: {test_url}")
    try:
        response = await page.goto(test_url, timeout=PAGE_TIMEOUT, wait_until="load")
        await page.wait_for_timeout(3000)
        current_url = page.url
        app_logger.debug(f"URL after navigation check: {current_url}")

        if "signin" in current_url.lower() or "/ap/" in current_url:
            app_logger.warning("Session check failed: Redirected to a login page.")
            return True # LOGIN NEEDED

        if response and not response.ok:
            app_logger.warning(f"Session check failed: Navigation returned non-OK status: {response.status}.")
            return True # LOGIN NEEDED

        dashboard_element_selector = "#content > div > div.mainAppContainerExternal > div.css-6pahkd.action-bar-container > div > div.filterbar-right-slot > kat-button:nth-child(2) > button"
        try:
            dashboard_element = page.locator(dashboard_element_selector)
            await expect(dashboard_element).to_be_visible(timeout=WAIT_TIMEOUT)
            app_logger.info("Session check successful: Found key dashboard element.")
            return False # LOGIN NOT NEEDED
        except TimeoutError:
            app_logger.warning("Session check failed: Key dashboard element was not found.")
            await _save_screenshot(page, "login_check_fail_no_element")
            return True # LOGIN NEEDED

    except Exception as e:
        app_logger.error(f"Unexpected error during session check: {e}", exc_info=DEBUG_MODE)
        await _save_screenshot(page, "login_check_fail_unexpected")
        return True # LOGIN NEEDED

async def perform_login_and_otp(page: Page) -> bool:
    app_logger.info(f"Navigating to login page: {LOGIN_URL}")
    try:
        await page.goto(LOGIN_URL, timeout=PAGE_TIMEOUT, wait_until="load")
        app_logger.info("Initial page loaded.")

        # --- Handle the "Continue shopping" interstitial page ---
        try:
            continue_button = page.get_by_role('button', name='Continue shopping')
            await expect(continue_button).to_be_visible(timeout=5000)
            app_logger.info("Interstitial 'Continue shopping' page detected. Clicking to proceed...")
            await continue_button.click()
            # Wait for the real login page to appear after the click
            await expect(page.get_by_label("Email or mobile phone number")).to_be_visible(timeout=15000)
            app_logger.info("Main login form is now visible.")
        except TimeoutError:
            app_logger.info("No interstitial page found, proceeding directly to login form.")

        # --- Perform Login Steps with Robust Waits ---
        # Step 1: Fill Email and click Continue
        await page.get_by_label("Email or mobile phone number").fill(config['login_email'])
        await page.get_by_label("Continue").click()

        # Step 2: Wait for Password field, then fill and click Sign in
        app_logger.info("Waiting for password field...")
        password_field = page.get_by_label("Password")
        await expect(password_field).to_be_visible(timeout=10000)
        await password_field.fill(config['login_password'])
        
        app_logger.info("Password entered. Clicking Sign-in and waiting for navigation...")
        # This is a critical step. We click and then immediately wait for the page to change.
        await page.get_by_label("Sign in").click()
        
        # --- Handle Post-Login Page (OTP or Dashboard) ---
        app_logger.info("Waiting for page to settle after sign-in...")

        # We wait for one of two things: the OTP input field, or a known element on the dashboard.
        # This is much more stable than checking the URL or title during a navigation.
        otp_selector = 'input[id*="otp"]'
        dashboard_selector = "#content > div > div.mainAppContainerExternal" # A stable container on the dashboard

        # Wait for either the OTP page or the dashboard to load.
        await page.wait_for_selector(f"{otp_selector}, {dashboard_selector}", timeout=30000)
        app_logger.info("Page has settled. Checking for OTP requirement...")

        # Now, check if the OTP field is actually visible.
        otp_field = page.locator(otp_selector)
        if await otp_field.is_visible():
            app_logger.info("Two-Step Verification (OTP) is required.")
            if not config.get('otp_secret_key'):
                app_logger.error("OTP is required, but 'otp_secret_key' is not configured.")
                await _save_screenshot(page, "login_fail_otp_no_key")
                return False
                
            otp_code = pyotp.TOTP(config['otp_secret_key']).now()
            await otp_field.fill(otp_code)
            
            remember_device_checkbox = page.locator("input[type='checkbox'][name='rememberDevice']")
            # THIS IS THE FIX: is_visible() does not take a timeout argument.
            if await remember_device_checkbox.is_visible():
                await remember_device_checkbox.check()
            
            # The submit button can have different labels
            await page.get_by_role("button", name="Sign in").click()
            app_logger.info("OTP submitted.")
        else:
            app_logger.info("OTP not required. Logged in directly.")

        # --- Final Verification ---
        # Wait for a definitive element on the dashboard to confirm successful login
        app_logger.info("Verifying successful login by looking for dashboard content...")
        final_dashboard_check = page.locator(dashboard_selector)
        await expect(final_dashboard_check).to_be_visible(timeout=30000)

        if "signin" in page.url.lower() or "/ap/" in page.url:
            app_logger.error("Login failed: Ended up on a sign-in page unexpectedly.")
            await _save_screenshot(page, "login_fail_final_check")
            return False
        
        app_logger.info("Login process appears fully successful.")
        return True
    except Exception as e:
        app_logger.critical(f"Critical error during login process: {e}", exc_info=DEBUG_MODE)
        await _save_screenshot(page, "login_critical_failure")
        return False


async def prime_master_session() -> bool:
    global browser
    app_logger.info("Priming master session")
    ctx = None
    try:
        if not browser or not browser.is_connected(): return False
        ctx = await browser.new_context()
        ctx.set_default_navigation_timeout(PAGE_TIMEOUT)
        ctx.set_default_timeout(ACTION_TIMEOUT)
        await ctx.route("**/*", lambda route: route.abort() if route.request.resource_type in ("image", "stylesheet", "font", "media") else route.continue_())
        page = await ctx.new_page()
        if not await perform_login_and_otp(page): return False
        storage = await ctx.storage_state()
        with open(STORAGE_STATE, 'w') as f: json.dump(storage, f)
        app_logger.info(f"Login successful. Auth state saved to '{STORAGE_STATE}'.")
        return True
    except Exception as e:
        app_logger.exception(f"Priming failed with an unexpected error: {e}")
        return False
    finally:
        if ctx: await ctx.close()

#######################################################################
#                  OPTIMIZED ARCHITECTURE: WORKERS & LOGGING
#######################################################################

def log_submission(data: Dict[str,str]):
    with log_lock:
        current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = {'timestamp': current_timestamp, **data}
        fieldnames = ['timestamp','store','orders','units','fulfilled','uph','inf','found','cancelled','lates','time_available']
        new_csv = not os.path.exists(LOG_FILE)
        try:
            with open(LOG_FILE,'a',newline='', encoding='utf-8') as f:
                w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
                if new_csv: w.writeheader()
                w.writerow(log_entry)
        except IOError as e: app_logger.error(f"Error writing to CSV log file {LOG_FILE}: {e}")
        try:
            with open(JSON_LOG_FILE, 'a', encoding='utf-8') as f: f.write(json.dumps(log_entry) + '\n')
        except IOError as e: app_logger.error(f"Error writing to JSON log file {JSON_LOG_FILE}: {e}")

async def form_submitter_worker(queue: Queue, storage_template: Dict, worker_id: int):
    log_prefix = f"[FormSubmitter-{worker_id}]"
    app_logger.info(f"{log_prefix} Starting up...")
    ctx: BrowserContext = None
    page: Page = None
    try:
        ctx = await browser.new_context(storage_state=storage_template)
        page = await ctx.new_page()
        while True:
            form_data = None
            try:
                form_data = await queue.get()
                store_name = form_data.get('store', 'Unknown')
                await page.goto(FORM_URL, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
                labels = {
                    'store': "Field 2", 'orders': "Field 3", 'units': "Field 4", 'fulfilled': "Field 5",
                    'uph': "Field 6", 'inf': "Field 7", 'found': "Field 8", 'cancelled': "Field 9",
                    'lates': "Field 10", 'time_available': "Field 12"
                }
                for key, label in labels.items():
                    await page.get_by_label(label, exact=True).fill(form_data.get(key, ""))
                await page.get_by_label("Submit", exact=True).click()
                await expect(page.locator("text='Your response has been recorded.'")).to_be_visible(timeout=WAIT_TIMEOUT)
                log_submission(form_data)
                with progress_lock:
                    progress["current"] += 1
                    progress["lastUpdate"] = f"{datetime.now().strftime('%H:%M')} [Submitted] {store_name}"
            except asyncio.CancelledError:
                break
            except Exception as e:
                failed_store = form_data.get('store', 'Unknown') if form_data else "Unknown"
                app_logger.error(f"{log_prefix} Error submitting for {failed_store}: {e}", exc_info=DEBUG_MODE)
                run_failures.append(f"{failed_store} (Submit Fail)")
            finally:
                if form_data: queue.task_done()
    finally:
        if page and not page.is_closed(): await page.close()
        if ctx: await ctx.close()
        app_logger.info(f"{log_prefix} Shut down.")

async def data_collector_worker(browser: Browser, store_info: Dict[str,str], storage_template: Dict, queue: Queue):
    merchant_id = store_info['merchant_id']
    store_name  = store_info['store_name']

    for attempt in range(WORKER_RETRY_COUNT):
        ctx: BrowserContext = None
        try:
            marketplace_id = store_info['marketplace_id']
            if not marketplace_id:
                app_logger.error(f"Skipping {store_name}: marketplace_id is missing.")
                run_failures.append(f"{store_name} (Missing MKID)")
                return

            ctx  = await browser.new_context(storage_state=storage_template)
            ctx.set_default_navigation_timeout(PAGE_TIMEOUT)
            ctx.set_default_timeout(ACTION_TIMEOUT)

            async def block_resources(route):
                if (any(domain in route.request.url for domain in RESOURCE_BLOCKLIST) or
                        route.request.resource_type in ("image", "stylesheet", "font", "media")):
                    await route.abort()
                else:
                    await route.continue_()
            await ctx.route("**/*", block_resources)

            page = await ctx.new_page()

            dash_url = f"https://sellercentral.amazon.co.uk/snowdash?ref_=mp_home_logo_xx&cor=mmp_EU&mons_sel_dir_mcid={merchant_id}&mons_sel_mkid={marketplace_id}"
            await page.goto(dash_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")

            refresh_button_selector = "#content > div > div.mainAppContainerExternal > div.css-6pahkd.action-bar-container > div > div.filterbar-right-slot > kat-button:nth-child(2) > button"

            METRICS_TIMEOUT = 40_000
            async with page.expect_response(lambda r: "summationMetrics" in r.url and r.status == 200, timeout=METRICS_TIMEOUT) as resp_info:
                refresh_button = page.locator(refresh_button_selector)
                await expect(refresh_button).to_be_visible(timeout=WAIT_TIMEOUT)
                await refresh_button.click()
            api_data = await (await resp_info.value).json()

            formatted_lates = "0 %"
            try:
                body_first_row = page.locator("#content kat-table kat-table-body kat-table-row").first
                await body_first_row.wait_for(state="attached", timeout=WAIT_TIMEOUT)
                lates_cell = body_first_row.locator("kat-table-cell").nth(10)
                cell_text  = (await lates_cell.text_content() or "").strip()
                if re.fullmatch(r"\d+(\.\d+)?\s*%", cell_text):
                    formatted_lates = cell_text
            except Exception:
                app_logger.warning(f"[{store_name}] Could not scrape 'Lates' data, defaulting to 0 %")

            milliseconds_from_api = float(api_data.get('TimeAvailable_V2', 0.0))
            total_seconds = int(milliseconds_from_api / 1000)
            total_minutes, _ = divmod(abs(total_seconds), 60)
            total_hours, remaining_minutes = divmod(total_minutes, 60)
            formatted_time_available = f"{total_hours}:{remaining_minutes:02d}"

            form_data = {
                'store': store_name, 'orders': str(api_data.get('OrdersShopped_V2', 0)),
                'units': str(api_data.get('RequestedQuantity_V2', 0)), 'fulfilled': str(api_data.get('PickedUnits_V2', 0)),
                'uph': f"{api_data.get('AverageUPH_V2', 0.0):.0f}", 'inf': f"{api_data.get('ItemNotFoundRate_V2', 0.0):.1f} %",
                'found': f"{api_data.get('ItemFoundRate_V2', 0.0):.1f} %", 'cancelled': str(api_data.get('ShortedUnits_V2', 0)),
                'lates': formatted_lates, 'time_available': formatted_time_available
            }

            await queue.put(form_data)
            app_logger.info(f"[{store_name}] Data collection complete. Added to submission queue.")
            return

        except Exception as e:
            app_logger.warning(f"Data Collector for {store_name} failed on attempt {attempt + 1}: {e}", exc_info=False)
            if attempt < WORKER_RETRY_COUNT - 1: await asyncio.sleep(2)
            else:
                app_logger.error(f"Data Collector for {store_name} FAILED after all attempts.")
                run_failures.append(f"{store_name} (Collect Fail)")
        finally:
            if ctx: await ctx.close()

#######################################################################
#                  MAIN PROCESS LOOP & ORCHESTRATION
#######################################################################

async def managed_worker(store_item: Dict, browser: Browser, storage_template: Dict, queue: Queue):
    global active_workers_count
    await asyncio.sleep(random.uniform(0.1, 1.0))
    async with concurrency_condition:
        await concurrency_condition.wait_for(lambda: active_workers_count < concurrency_limit)
        active_workers_count += 1
    try:
        await data_collector_worker(browser, store_item, storage_template, queue)
    finally:
        async with concurrency_condition:
            active_workers_count -= 1
            concurrency_condition.notify()

async def process_urls():
    global progress, start_time, run_failures, browser, active_workers_count

    app_logger.info(f"Job 'process_urls' started with collector concurrency limit of {concurrency_limit}.")
    run_failures = []
    
    load_default_data()
    if not urls_data:
        app_logger.error("No URLs to process. Aborting job.")
        return

    login_is_required = True
    if ensure_storage_state():
        app_logger.info("Existing auth state file found. Verifying session is still active...")
        temp_context = None
        try:
            first_store = urls_data[0]
            test_dash_url = f"https://sellercentral.amazon.co.uk/snowdash?ref_=mp_home_logo_xx&cor=mmp_EU&mons_sel_dir_mcid={first_store['merchant_id']}&mons_sel_mkid={first_store['marketplace_id']}"
            with open(STORAGE_STATE) as f: storage_for_check = json.load(f)
            temp_context = await browser.new_context(storage_state=storage_for_check)
            temp_page = await temp_context.new_page()
            if not await check_if_login_needed(temp_page, test_dash_url):
                app_logger.info("Session verification successful. Skipping login.")
                login_is_required = False
            else:
                app_logger.warning("Session has expired or is invalid. A new login is required.")
        except Exception as e:
            app_logger.error(f"An error occurred during session verification. Forcing re-login. Error: {e}", exc_info=DEBUG_MODE)
        finally:
            if temp_context: await temp_context.close()
    else:
        app_logger.info("No existing auth state file found. Login is required.")

    if login_is_required:
        app_logger.info("Attempting to prime a new master session...")
        if not await prime_master_session():
            app_logger.error("Critical: Session priming failed. Aborting job.")
            return

    async with concurrency_condition: active_workers_count = 0
    with open(STORAGE_STATE) as f: storage_template = json.load(f)
    submission_queue = Queue()

    app_logger.info(f"Starting {NUM_FORM_SUBMITTERS} form submitter worker(s).")
    form_submitter_tasks = [
        asyncio.create_task(form_submitter_worker(submission_queue, storage_template, i+1))
        for i in range(NUM_FORM_SUBMITTERS)
    ]

    with progress_lock: progress = {"current": 0, "total": len(urls_data), "lastUpdate": "N/A"}
    start_time = datetime.now()

    collector_tasks = [managed_worker(si, browser, storage_template, submission_queue) for si in urls_data]
    await asyncio.gather(*collector_tasks)

    app_logger.info("All data collectors finished. Waiting for submission queue to empty...")
    await submission_queue.join()

    app_logger.info("Cancelling form submitter workers...")
    for task in form_submitter_tasks: task.cancel()
    await asyncio.gather(*form_submitter_tasks, return_exceptions=True)

    elapsed = (datetime.now() - start_time).total_seconds()
    app_logger.info(f"Processing finished. Processed {progress['current']}/{progress['total']} in {elapsed:.2f}s")
    if run_failures:
        app_logger.warning(f"Completed with {len(run_failures)} issue(s): {', '.join(run_failures)}")
    else:
        app_logger.info("Completed successfully.")

#######################################################################
#                         MAIN EXECUTION BLOCK
#######################################################################

async def main():
    """
    Main function to orchestrate the entire scraping process for a single run.
    """
    global playwright, browser
    app_logger.info("Starting up in single-run mode...")

    try:
        playwright = await async_playwright().start()
        # For CI/CD environments like GitHub Actions, headless must be True.
        # For local debugging, you can set it to False in your config.json.
        browser = await playwright.chromium.launch(headless=not DEBUG_MODE)
        app_logger.info("Browser launched successfully.")
        
        await process_urls()

    except Exception as e:
        app_logger.critical(f"A critical error occurred in the main execution block: {e}", exc_info=True)
    finally:
        app_logger.info("Task finished. Initiating shutdown...")
        if browser and browser.is_connected():
            await browser.close()
            app_logger.info("Browser instance closed.")
        if playwright:
            await playwright.stop()
            app_logger.info("Playwright stopped.")
        app_logger.info("Run complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        app_logger.info("Script interrupted by user. Exiting.")