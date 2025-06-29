import logging
import signal
import urllib.parse
from datetime import datetime, timedelta
from pytz import timezone
from quart import Quart, jsonify, render_template, request, Response
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.jobstores.base import JobLookupError
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
import io
import asyncio
from asyncio import Queue
from threading import Lock
from typing import Dict, List
import pyotp
from copy import deepcopy
from logging.handlers import RotatingFileHandler
import re
import psutil
import uuid
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
SECRET_KEY      = config['secret_key']

AUTO_CONFIG = config.get('auto_concurrency', {})
AUTO_ENABLED = AUTO_CONFIG.get('enabled', False)
MIN_CONCURRENCY = AUTO_CONFIG.get('min_concurrency', 1)
MAX_CONCURRENCY = AUTO_CONFIG.get('max_concurrency', 16)
CPU_UPPER_THRESHOLD = AUTO_CONFIG.get('cpu_upper_threshold', 80)
CPU_LOWER_THRESHOLD = AUTO_CONFIG.get('cpu_lower_threshold', 40)
MEM_UPPER_THRESHOLD = AUTO_CONFIG.get('mem_upper_threshold', 85)
CHECK_INTERVAL = AUTO_CONFIG.get('check_interval_seconds', 10)
COOLDOWN_SECONDS = AUTO_CONFIG.get('cooldown_seconds', 20)
INITIAL_CONCURRENCY = config.get('initial_concurrency', 15)
NUM_FORM_SUBMITTERS = config.get('num_form_submitters', 4)

SCHEDULE_FILE   = 'schedules.json'
LOG_FILE        = os.path.join('output', 'submissions.log')
JSON_LOG_FILE   = os.path.join('output', 'submissions.jsonl')
STORAGE_STATE   = 'state.json'
OUTPUT_DIR      = 'output'
os.makedirs(OUTPUT_DIR, exist_ok=True)

PAGE_TIMEOUT    = config.get('page_timeout_ms', 60000)
# ✅ FIXED: Lowered general wait timeout now that specific long waits are handled separately.
WAIT_TIMEOUT    = config.get('element_wait_timeout_ms', 15000)
ACTION_TIMEOUT = int(PAGE_TIMEOUT / 3)
WORKER_RETRY_COUNT = 3

RESOURCE_BLOCKLIST = [
    "google-analytics.com", "googletagmanager.com", "doubleclick.net",
    "adservice.google.com", "facebook.net", "fbcdn.net", "analytics.tiktok.com",
]

#######################################################################
#                             APP SETUP & LOGGING
#######################################################################

app = Quart(__name__)
app.secret_key = SECRET_KEY

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
    logging.getLogger('quart.app').setLevel(logging.WARNING)
    logging.getLogger('hypercorn.access').disabled = True
    logging.getLogger('quart.serving').setLevel(logging.WARNING)
    logging.getLogger('werkzeug').setLevel(logging.ERROR)
    return app_logger

app_logger = setup_logging()

#######################################################################
#                      GLOBALS & SHUTDOWN HANDLING
#######################################################################

log_lock      = Lock()
progress_lock = Lock()
urls_data     = []
stop_flag     = False
progress      = {"current": 0, "total": 0, "lastUpdate": "N/A"}
run_failures  = []
shutdown_evt  = asyncio.Event()
last_run_time = None
next_run_time = None
start_time    = None

playwright = None
browser = None

concurrency_limit = INITIAL_CONCURRENCY
active_workers_count = 0
concurrency_condition = asyncio.Condition()
auto_concurrency_enabled = AUTO_ENABLED
last_adjustment_time = datetime.now()


def handle_shutdown_signal():
    app_logger.info("Shutdown signal received")
    shutdown_evt.set()

for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, lambda s,f: handle_shutdown_signal())

async def shutdown():
    global playwright, browser
    app_logger.info("Initiating graceful shutdown...")
    shutdown_evt.set()
    await asyncio.sleep(1)
    if 'scheduler' in globals() and scheduler.running:
        scheduler.shutdown(wait=False)
        app_logger.info("Scheduler shut down.")
    
    if browser and browser.is_connected():
        await browser.close()
        app_logger.info("Persistent browser instance closed.")
    if playwright:
        await playwright.stop()
        app_logger.info("Playwright stopped.")

#######################################################################
#                       AUTO-CONCURRENCY MANAGER
#######################################################################
async def auto_adjust_concurrency():
    global concurrency_limit, last_adjustment_time
    while not shutdown_evt.is_set():
        await asyncio.sleep(CHECK_INTERVAL)
        if not auto_concurrency_enabled or active_workers_count == 0: continue
        if (datetime.now() - last_adjustment_time).total_seconds() < COOLDOWN_SECONDS: continue
        cpu_percent, mem_percent = psutil.cpu_percent(), psutil.virtual_memory().percent
        current_limit = concurrency_limit
        if mem_percent > MEM_UPPER_THRESHOLD:
            if current_limit > MIN_CONCURRENCY:
                concurrency_limit = max(MIN_CONCURRENCY, current_limit - 1)
                app_logger.info(f"[AutoConcurrency] High Memory ({mem_percent}%)! Decreasing concurrency to {concurrency_limit}.")
        elif cpu_percent > CPU_UPPER_THRESHOLD:
            if current_limit > MIN_CONCURRENCY:
                concurrency_limit = max(MIN_CONCURRENCY, current_limit - 1)
                app_logger.info(f"[AutoConcurrency] High CPU ({cpu_percent}%)! Decreasing concurrency to {concurrency_limit}.")
        elif cpu_percent < CPU_LOWER_THRESHOLD:
            if current_limit < MAX_CONCURRENCY:
                concurrency_limit = min(MAX_CONCURRENCY, current_limit + 1)
                app_logger.info(f"[AutoConcurrency] Low CPU ({cpu_percent}%)! Increasing concurrency to {concurrency_limit}.")
        if concurrency_limit != current_limit:
            last_adjustment_time = datetime.now()
            async with concurrency_condition: concurrency_condition.notify_all()

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

def update_last_run_time():
    global last_run_time
    last_run_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

def update_next_run_time():
    global next_run_time
    if 'scheduler' in globals() and scheduler.running:
        jobs = scheduler.get_jobs()
        if jobs:
            next_times = [job.next_run_time for job in jobs if job.next_run_time]
            if next_times:
                nrt = min(next_times)
                next_run_time = nrt.astimezone(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
            else: next_run_time = None
        else: next_run_time = None

#######################################################################
#                     AUTHENTICATION & SESSION PRIMING
#######################################################################
async def check_if_login_needed(page: Page, test_url: str) -> bool:
    """
    Actively checks if the current session is valid by navigating to a protected page.
    Returns True if login is needed, False otherwise.
    """
    app_logger.info(f"Verifying session status by navigating to: {test_url}")
    try:
        response = await page.goto(test_url, timeout=PAGE_TIMEOUT, wait_until="load")
        await page.wait_for_timeout(3000) # Give it a moment to settle
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

    except TimeoutError:
        app_logger.error(f"Timeout during session check navigation to {test_url}.")
        await _save_screenshot(page, "login_check_fail_nav_timeout")
        return True # LOGIN NEEDED
    except PlaywrightError as e:
        app_logger.error(f"Playwright navigation error during session check: {e}", exc_info=DEBUG_MODE)
        await _save_screenshot(page, "login_check_fail_nav_error")
        return True # LOGIN NEEDED
    except Exception as e:
        app_logger.error(f"Unexpected error during session check: {e}", exc_info=DEBUG_MODE)
        await _save_screenshot(page, "login_check_fail_unexpected")
        return True # LOGIN NEEDED

async def perform_login_and_otp(page: Page) -> bool:
    app_logger.info(f"Navigating to login page: {LOGIN_URL}")
    try:
        await page.goto(LOGIN_URL, timeout=PAGE_TIMEOUT, wait_until="load")
        app_logger.info("Login page loaded.")
        email_selector, password_selector = "input#ap_email", "input#ap_password"
        signin_button_selector, continue_button_selector = "input#signInSubmit", "input#continue"
        otp_input_selector, remember_device_selector = "input#auth-mfa-otpcode", "input[name='rememberDevice']"
        otp_submit_button_selector = "input#auth-signin-button"

        try:
            email_field = page.locator(email_selector)
            await expect(email_field).to_be_visible(timeout=WAIT_TIMEOUT)
            await email_field.fill(config['login_email'])
            await page.wait_for_timeout(500)
            continue_button = page.locator(continue_button_selector)
            password_field_check = page.locator(password_selector)
            if await continue_button.is_visible(timeout=2000):
                async with page.expect_navigation(timeout=PAGE_TIMEOUT, wait_until="load"): await continue_button.click()
            elif await password_field_check.is_visible(timeout=1000): pass
            else:
                await expect(email_field).to_be_enabled(timeout=ACTION_TIMEOUT)
                async with page.expect_navigation(timeout=PAGE_TIMEOUT, wait_until="load"): await email_field.press("Enter")
        except Exception as e:
            app_logger.error(f"Error during email input step: {e}", exc_info=DEBUG_MODE)
            await _save_screenshot(page, "login_fail_email_step_exception")
            return False

        try:
            password_field = page.locator(password_selector)
            await expect(password_field).to_be_visible(timeout=WAIT_TIMEOUT)
            await password_field.fill(config['login_password'])
            await page.wait_for_timeout(500)
            signin_button = page.locator(signin_button_selector)
            await expect(signin_button).to_be_visible(timeout=WAIT_TIMEOUT)
            await expect(signin_button).to_be_enabled(timeout=ACTION_TIMEOUT)
            async with page.expect_navigation(timeout=PAGE_TIMEOUT, wait_until="load"): await signin_button.click()
        except Exception as e:
            app_logger.error(f"Error during password input or sign-in click: {e}", exc_info=DEBUG_MODE)
            await _save_screenshot(page, "login_fail_password_step")
            return False

        try:
            otp_input_field = page.locator(otp_input_selector)
            await expect(otp_input_field).to_be_visible(timeout=30000)
            if not config['otp_secret_key']:
                app_logger.error("OTP is required, but 'otp_secret_key' is not configured.")
                await _save_screenshot(page, "login_fail_otp_no_key")
                return False
            otp_code = pyotp.TOTP(config['otp_secret_key']).now()
            await otp_input_field.fill(otp_code)
            await page.wait_for_timeout(500)
            remember_locator = page.locator(remember_device_selector)
            if await remember_locator.is_visible(timeout=5000) and not await remember_locator.is_checked(): await remember_locator.check()
            
            otp_submit_locator = page.locator(otp_submit_button_selector)
            try:
                await expect(otp_submit_locator).to_be_visible(timeout=5000)
                await expect(otp_submit_locator).to_be_enabled(timeout=ACTION_TIMEOUT)
                async with page.expect_navigation(timeout=PAGE_TIMEOUT, wait_until="load"): await otp_submit_locator.click()
            except TimeoutError:
                await expect(otp_input_field).to_be_enabled(timeout=ACTION_TIMEOUT)
                async with page.expect_navigation(timeout=PAGE_TIMEOUT, wait_until="load"): await otp_input_field.press("Enter")
        except TimeoutError: pass
        except Exception as e:
            app_logger.error(f"Error during OTP step: {e}", exc_info=DEBUG_MODE)
            await _save_screenshot(page, "login_fail_otp_step")
            return False

        await page.wait_for_timeout(5000)
        if "signin" in page.url.lower() or "/ap/" in page.url:
            app_logger.error("Login failed: Still on a sign-in page after all steps.")
            await _save_screenshot(page, "login_fail_stuck_on_signin")
            return False
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
                app_logger.info(f"{log_prefix} Shutdown signal received.")
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
        page: Page = None
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
            
            # --- wait for metrics (with your more robust patch) ---
            METRICS_TIMEOUT = 40_000   # ms
            async with page.expect_response(
                    lambda r: "summationMetrics" in r.url.split("?")[0] and r.status == 200,
                    timeout=METRICS_TIMEOUT) as resp_info:
                refresh_button = page.locator(refresh_button_selector)
                await expect(refresh_button).to_be_visible(timeout=WAIT_TIMEOUT)
                await refresh_button.click()
            api_data = await (await resp_info.value).json()
            # --------------------------------------------------------

            # --- scrape 'Lates' (with your more robust patch) ---
            formatted_lates = "0 %"
            try:
                body_first_row = page.locator(
                    "#content kat-table kat-table-body kat-table-row"
                ).first
                # wait for the row *to exist* (not necessarily visible)
                await body_first_row.wait_for(state="attached", timeout=WAIT_TIMEOUT)

                lates_cell = body_first_row.locator("kat-table-cell").nth(10)
                cell_text  = (await lates_cell.text_content() or "").strip()

                if re.fullmatch(r"\d+(\.\d+)?\s*%", cell_text):
                    formatted_lates = cell_text
                else:
                    # covers "", "-", "N/A", etc.
                    formatted_lates = "0 %"
            except TimeoutError:
                app_logger.info(f"[{store_name}] No body rows for 'Lates' → default 0 %")
            except Exception as e:
                # This exception is now less likely but kept for safety.
                app_logger.warning(f"[{store_name}] Unexpected error scraping 'Lates': {e} – default 0 %")
            # -----------------------------------------------------

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
        if stop_flag or shutdown_evt.is_set(): return
        await data_collector_worker(browser, store_item, storage_template, queue)
    finally:
        async with concurrency_condition:
            active_workers_count -= 1
            concurrency_condition.notify()

async def process_urls():
    global stop_flag, progress, start_time, run_failures, browser, active_workers_count
    
    app_logger.info(f"Job 'process_urls' started with collector concurrency limit of {concurrency_limit}.")
    stop_flag = False
    run_failures = []
    
    load_default_data()
    if not urls_data:
        app_logger.error("No URLs to process. Aborting job.")
        return

    # --- NEW, ROBUST LOGIN VERIFICATION ---
    login_is_required = True # Assume login is needed by default
    if ensure_storage_state():
        app_logger.info("Existing auth state file found. Verifying session is still active...")
        temp_context = None
        try:
            # Build a test URL using the first available store's data
            first_store = urls_data[0]
            test_dash_url = f"https://sellercentral.amazon.co.uk/snowdash?ref_=mp_home_logo_xx&cor=mmp_EU&mons_sel_dir_mcid={first_store['merchant_id']}&mons_sel_mkid={first_store['marketplace_id']}"

            with open(STORAGE_STATE) as f:
                storage_for_check = json.load(f)

            temp_context = await browser.new_context(storage_state=storage_for_check)
            temp_page = await temp_context.new_page()

            # check_if_login_needed returns True if login is needed, False if session is OK
            if not await check_if_login_needed(temp_page, test_dash_url):
                app_logger.info("Session verification successful. Skipping login.")
                login_is_required = False
            else:
                app_logger.warning("Session has expired or is invalid. A new login is required.")
        
        except Exception as e:
            app_logger.error(f"An error occurred during session verification. Forcing re-login. Error: {e}", exc_info=DEBUG_MODE)
            login_is_required = True
        finally:
            if temp_context:
                await temp_context.close()
    else:
        app_logger.info("No existing auth state file found. Login is required.")
        login_is_required = True

    if login_is_required:
        app_logger.info("Attempting to prime a new master session...")
        if not await prime_master_session():
            app_logger.error("Critical: Session priming failed. Aborting job.")
            return
    # --- END OF LOGIN VERIFICATION ---

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
    update_last_run_time()

    collector_tasks = [managed_worker(si, browser, storage_template, submission_queue) for si in urls_data]
    await asyncio.gather(*collector_tasks)

    app_logger.info("All data collectors finished. Waiting for submission queue to empty...")
    await submission_queue.join()
    
    app_logger.info("Cancelling form submitter workers...")
    for task in form_submitter_tasks: task.cancel()
    await asyncio.gather(*form_submitter_tasks, return_exceptions=True)

    update_next_run_time()
    elapsed = (datetime.now() - start_time).total_seconds()
    app_logger.info(f"Processing finished. Processed {progress['current']}/{progress['total']} in {elapsed:.2f}s")
    if run_failures:
        app_logger.warning(f"Completed with {len(run_failures)} issue(s): {', '.join(run_failures)}")
    else:
        app_logger.info("Completed successfully.")

#######################################################################
#                         SCHEDULER & ROUTES
#######################################################################
def save_schedules():
    if 'scheduler' in globals() and scheduler.running:
        schedules = []
        for job in scheduler.get_jobs():
            schedules.append({
                'id': job.id, 'name': job.name,
                'datetime': job.next_run_time.isoformat() if job.next_run_time else None,
                'repeat_daily': isinstance(job.trigger, IntervalTrigger)
            })
        with open(SCHEDULE_FILE,'w') as f: json.dump(schedules, f)

def load_schedules():
    if os.path.exists(SCHEDULE_FILE):
        schedules = []
        try:
            with open(SCHEDULE_FILE) as f: schedules = json.load(f)
            for sch in schedules:
                if sch.get('datetime'):
                    aware_dt = datetime.fromisoformat(sch['datetime'])
                    naive_dt = aware_dt.replace(tzinfo=None)
                    if sch.get('repeat_daily'):
                        trigger = IntervalTrigger(days=1, start_date=naive_dt)
                    else:
                        trigger = DateTrigger(run_date=naive_dt)
                    scheduler.add_job(process_urls, trigger, id=sch.get('id'), name=sch.get('name'))
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            sch_info = schedules and sch or {}
            app_logger.exception(f"Could not load schedule '{sch_info.get('id', 'N/A')}'. Error: {e}")

@app.route('/')
async def index(): return await render_template('index.html')

@app.route('/logs')
async def logs_page(): return await render_template('logs.html')

@app.route('/start_now', methods=['POST'])
async def start_now():
    asyncio.create_task(process_urls())
    return jsonify(status="Extraction started")

@app.route('/stop', methods=['POST'])
async def stop_extraction():
    global stop_flag
    stop_flag = True
    app_logger.info('Extraction stop requested by user.')
    return jsonify(status="Stop signal sent")

@app.route('/clear_log', methods=['POST'])
async def clear_log():
    try:
        if os.path.exists('app.log'): open('app.log','w').close()
        return jsonify(status="Log cleared")
    except Exception:
        app_logger.exception("Failed to clear log")
        return jsonify(status="Error clearing log"), 500

@app.route('/progress_status')
async def progress_status():
    with progress_lock:
        pct = (progress["current"]/progress["total"])*100 if progress["total"]>0 else 0
        p_copy = deepcopy(progress)
    return jsonify(progress=p_copy, percentage=pct, latestLog=p_copy["lastUpdate"])

@app.route('/log')
async def log_status():
    try:
        with open('app.log') as f: lines = f.read().splitlines()
        return jsonify(logs=lines[-200:])
    except FileNotFoundError: return jsonify(logs=[])

@app.route('/stats', methods=['GET'])
async def stats():
    update_next_run_time()
    return jsonify({
        "last_run_time": last_run_time, "next_run_time": next_run_time,
        "total_urls": urls_data and len(urls_data) or progress.get("total",0),
        "processed_urls": progress.get("current",0), "run_failures": run_failures
    })

@app.route('/api/concurrency', methods=['GET', 'POST'])
async def manage_concurrency():
    global concurrency_limit, auto_concurrency_enabled
    if request.method == 'POST':
        data = await request.get_json()
        if 'auto_enabled' in data:
            auto_concurrency_enabled = bool(data['auto_enabled'])
            mode = "enabled" if auto_concurrency_enabled else "disabled"
            app_logger.info(f"Auto-concurrency mode {mode} by user.")
            return jsonify(success=True, auto_enabled=auto_concurrency_enabled)
        if auto_concurrency_enabled:
            return jsonify(success=False, message="Cannot set limit manually while auto-mode is enabled."), 400
        new_limit = data.get('limit')
        if not isinstance(new_limit, int) or new_limit <= 0:
            return jsonify(success=False, message="Concurrency limit must be a positive integer."), 400
        old_limit = concurrency_limit
        concurrency_limit = new_limit
        app_logger.info(f"Concurrency limit manually set from {old_limit} to {new_limit} by user.")
        async with concurrency_condition: concurrency_condition.notify_all()
        return jsonify(success=True, new_limit=concurrency_limit)
    else:
        return jsonify(
            limit=concurrency_limit, active=active_workers_count, auto_enabled=auto_concurrency_enabled,
            min_concurrency=MIN_CONCURRENCY, max_concurrency=MAX_CONCURRENCY,
            cpu=psutil.cpu_percent(), memory=psutil.virtual_memory().percent
        )

@app.route('/schedules', methods=['GET'])
async def get_schedules():
    result = []
    if 'scheduler' in globals() and scheduler.running:
        for job in scheduler.get_jobs():
            result.append({
                'id': job.id, 'name': job.name,
                'datetime': job.next_run_time.isoformat() if job.next_run_time else None,
                'repeat_daily': isinstance(job.trigger, IntervalTrigger),
                'paused': not bool(job.next_run_time)
            })
    return jsonify(schedules=result)

@app.route('/add_schedule', methods=['POST'])
async def add_schedule():
    data = await request.get_json()
    name = data.get('name', 'Untitled Job')
    datetime_str = data.get('datetime')
    repeat_daily = data.get('repeat_daily', False)
    if not datetime_str: return jsonify(success=False, message="Datetime is required."), 400
    try:
        naive_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        aware_dt = LOCAL_TIMEZONE.localize(naive_dt)
        if aware_dt < datetime.now(LOCAL_TIMEZONE):
            return jsonify(success=False, message="Scheduled time cannot be in the past."), 400
    except ValueError: return jsonify(success=False, message="Invalid datetime format."), 400
    job_id = uuid.uuid4().hex
    trigger = IntervalTrigger(days=1, start_date=naive_dt) if repeat_daily else DateTrigger(run_date=naive_dt)
    scheduler.add_job(process_urls, trigger, id=job_id, name=name)
    app_logger.info(f"Added schedule '{name}' (ID: {job_id}) for {aware_dt}.")
    save_schedules()
    update_next_run_time()
    return jsonify(success=True, id=job_id)

@app.route('/delete_schedule/<job_id>', methods=['POST'])
async def delete_schedule(job_id):
    try:
        scheduler.remove_job(job_id)
        app_logger.info(f"Deleted schedule with ID: {job_id}.")
        save_schedules()
        update_next_run_time()
        return jsonify(success=True)
    except JobLookupError:
        return jsonify(success=False, message="Job not found."), 404

@app.route('/edit_schedule/<job_id>', methods=['POST'])
async def edit_schedule(job_id):
    data = await request.get_json()
    name = data.get('name')
    datetime_str = data.get('datetime')
    repeat_daily = data.get('repeat_daily')
    if not all([name, datetime_str, repeat_daily is not None]):
        return jsonify(success=False, message="Missing required fields."), 400
    try:
        naive_dt = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        aware_dt = LOCAL_TIMEZONE.localize(naive_dt)
        if aware_dt < datetime.now(LOCAL_TIMEZONE) and not repeat_daily:
            return jsonify(success=False, message="Scheduled time for one-off jobs cannot be in the past."), 400
    except ValueError: return jsonify(success=False, message="Invalid datetime format."), 400
    try:
        trigger = IntervalTrigger(days=1, start_date=naive_dt) if repeat_daily else DateTrigger(run_date=naive_dt)
        scheduler.modify_job(job_id, trigger=trigger, name=name)
        app_logger.info(f"Modified schedule '{name}' (ID: {job_id}).")
        save_schedules()
        update_next_run_time()
        return jsonify(success=True)
    except JobLookupError:
        return jsonify(success=False, message="Job not found."), 404

@app.route('/api/logs', methods=['GET'])
async def get_logs():
    if os.path.exists(JSON_LOG_FILE):
        try:
            logs = []
            with open(JSON_LOG_FILE, 'r', encoding='utf-8') as f:
                for line in f:
                    if line.strip(): logs.append(json.loads(line))
            return jsonify(logs[::-1])
        except Exception as e: app_logger.warning(f"Could not read {JSON_LOG_FILE} ({e})")
    try:
        if not os.path.exists(LOG_FILE): return jsonify([])
        with open(LOG_FILE, 'r', encoding='utf-8') as f:
            return jsonify(list(csv.DictReader(f))[::-1])
    except Exception as e:
        app_logger.error(f"Fallback to CSV failed. Could not read any log files: {e}")
        return jsonify([])

@app.before_serving
async def startup():
    global scheduler, playwright, browser
    app_logger.info("Starting up...")
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch(headless=not DEBUG_MODE)
    app_logger.info("Browser launched successfully.")
    asyncio.create_task(auto_adjust_concurrency())
    load_default_data()
    app_logger.info("Starting scheduler and loading schedules.")
    scheduler = AsyncIOScheduler(timezone=LOCAL_TIMEZONE)
    load_schedules()
    scheduler.start()
    update_next_run_time()

@app.after_serving
async def shutdown_handler():
    await shutdown()

if __name__ == "__main__":
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
    try:
        app.run(host='0.0.0.0', port=5002)
    except (KeyboardInterrupt, SystemExit):
        pass
