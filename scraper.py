# =======================================================================================
#               AMAZON SELLER CENTRAL SCRAPER (CI/CD / COMMAND-LINE VERSION)
# =======================================================================================
# This version is optimized with direct HTTP form submission and robust,
# patient scraping logic for dynamically loaded content.
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
from typing import Dict, List, Any
import pyotp
from logging.handlers import RotatingFileHandler
import re
import psutil
import random

import aiohttp
import aiofiles
import ssl
import certifi
import io

# Use UK timezone for log timestamps
LOCAL_TIMEZONE = timezone('Europe/London')


class LocalTimeFormatter(logging.Formatter):
    """Formatter that converts timestamps to ``LOCAL_TIMEZONE``."""

    def converter(self, ts: float):
        dt = datetime.fromtimestamp(ts, LOCAL_TIMEZONE)
        return dt.timetuple()

#######################################################################
#                             APP SETUP & LOGGING
#######################################################################

def setup_logging():
    """Configure application logging to file and console.

    Returns:
        Logger: Configured logger instance used throughout the app.
    """
    app_logger = logging.getLogger('app')
    app_logger.setLevel(logging.INFO)
    app_file = RotatingFileHandler('app.log', maxBytes=10**7, backupCount=5)
    fmt = LocalTimeFormatter('%(asctime)s %(levelname)s %(message)s')
    app_file.setFormatter(fmt)
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    app_logger.addHandler(app_file)
    app_logger.addHandler(console)
    return app_logger

app_logger = setup_logging()

#######################################################################
#                            CONFIG & CONSTANTS
#######################################################################

try:
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
except FileNotFoundError:
    app_logger.critical("config.json not found. Please create it before running.")
    exit(1)
except json.JSONDecodeError:
    app_logger.critical("config.json is not valid JSON. Please fix it.")
    exit(1)

DEBUG_MODE      = config.get('debug', False)
LOGIN_URL       = config['login_url']
CHAT_WEBHOOK_URL = config.get('chat_webhook_url')
CHAT_BATCH_SIZE  = config.get('chat_batch_size', 100)
STORE_PREFIX_RE  = re.compile(r"^morrisons\s*-\s*", re.I)
BULLET           = " \u2022 "

def sanitize_store_name(name: str) -> str:
    """Trim standard prefix from store names for chat display."""
    return STORE_PREFIX_RE.sub("", name).strip()

def build_metric_line(pairs: List[tuple]) -> str:
    """Return metrics formatted as bold bullet-separated pairs."""
    return BULLET.join(f"*{label}:* {value}" for label, value in pairs)

FORM_POST_URL = "https://docs.google.com/forms/d/e/1FAIpQLScg_jnxbuJsPs4KejUaVuu-HfMQKA3vSXZkWaYh-P_lbjE56A/formResponse"
FIELD_MAP = {
    'store':          'entry.117918617',
    'orders':         'entry.128719511',
    'units':          'entry.66444552',
    'fulfilled':      'entry.2093280675',
    'uph':            'entry.316694141',
    'inf':            'entry.909185879',
    'found':          'entry.637588300',
    'cancelled':      'entry.1775576921',
    'lates':          'entry.2130893076',
    'time_available': 'entry.1823671734',
}

INITIAL_CONCURRENCY = config.get('initial_concurrency', 10)
NUM_FORM_SUBMITTERS = config.get('num_form_submitters', 2)

AUTO_CONF = config.get('auto_concurrency', {})
AUTO_ENABLED = AUTO_CONF.get('enabled', False)
AUTO_MIN_CONCURRENCY = AUTO_CONF.get('min_concurrency', config.get('min_concurrency', 1))
AUTO_MAX_CONCURRENCY = AUTO_CONF.get('max_concurrency', config.get('max_concurrency', INITIAL_CONCURRENCY))
CPU_UPPER_THRESHOLD = AUTO_CONF.get('cpu_upper_threshold', 90)
CPU_LOWER_THRESHOLD = AUTO_CONF.get('cpu_lower_threshold', 65)
MEM_UPPER_THRESHOLD = AUTO_CONF.get('mem_upper_threshold', 90)
CHECK_INTERVAL = AUTO_CONF.get('check_interval_seconds', 5)
COOLDOWN_SECONDS = AUTO_CONF.get('cooldown_seconds', 15)

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
#                      GLOBALS
#######################################################################

log_lock      = asyncio.Lock()
progress_lock = Lock()
urls_data     = []
progress      = {"current": 0, "total": 0, "lastUpdate": "N/A"}
run_failures  = []
start_time    = None

pending_chat_entries: List[Dict[str, str]] = []
pending_chat_lock = asyncio.Lock()
chat_batch_count = 0

playwright = None
browser = None

concurrency_limit = INITIAL_CONCURRENCY
active_workers_count = 0
concurrency_condition = asyncio.Condition()

last_concurrency_change = 0.0

#######################################################################
#                          UTILITIES
#######################################################################

async def _save_screenshot(page: Page | None, prefix: str):
    """Save a full-page screenshot for debugging purposes.

    Args:
        page (Page | None): Playwright page to capture.
        prefix (str): Prefix used when naming the screenshot file.

    Returns:
        None
    """
    if not page or page.is_closed():
        app_logger.warning(f"Cannot save screenshot '{prefix}': Page is closed or unavailable.")
        return
    try:
        safe_prefix = re.sub(r'[\\/*?:"<>|]', "_", prefix)
        timestamp = datetime.now(LOCAL_TIMEZONE).strftime("%Y%m%d_%H%M%S")
        path = os.path.join(OUTPUT_DIR, f"{safe_prefix}_{timestamp}.png")
        await page.screenshot(path=path, full_page=True, timeout=15000)
        app_logger.info(f"Screenshot saved for debugging: {path}")
    except Exception as e:
        app_logger.error(f"Failed to save screenshot with prefix '{prefix}': {e}")

def load_default_data():
    """Load store details from ``urls.csv`` into ``urls_data``.

    Returns:
        None
    """
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
    except Exception:
        app_logger.exception("An error occurred while loading urls.csv")

def ensure_storage_state():
    """Validate that ``state.json`` exists and contains cookies.

    Returns:
        bool: ``True`` if the saved state is usable, ``False`` otherwise.
    """
    if not os.path.exists(STORAGE_STATE) or os.path.getsize(STORAGE_STATE) == 0:
        return False
    try:
        with open(STORAGE_STATE) as f:
            data = json.load(f)
        if (
            not isinstance(data, dict)
            or "cookies" not in data
            or not isinstance(data["cookies"], list)
            or not data["cookies"]
        ):
            return False
        return True
    except json.JSONDecodeError:
        return False

async def auto_concurrency_manager():
    """Dynamically adjust worker concurrency based on system load.

    The manager periodically checks CPU and memory usage and raises or lowers
    the global ``concurrency_limit`` within the range configured in
    ``config.json``. Updates are throttled by ``COOLDOWN_SECONDS`` to avoid
    rapid fluctuations. All waiting workers are notified when the limit
    changes.
    """
    global concurrency_limit, last_concurrency_change
    if not AUTO_ENABLED:
        return
    app_logger.info(
        f"Auto-concurrency enabled with range {AUTO_MIN_CONCURRENCY}-{AUTO_MAX_CONCURRENCY}"
    )
    while True:
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent
        now = asyncio.get_event_loop().time()
        if now - last_concurrency_change >= COOLDOWN_SECONDS:
            if (cpu > CPU_UPPER_THRESHOLD or mem > MEM_UPPER_THRESHOLD) and concurrency_limit > AUTO_MIN_CONCURRENCY:
                concurrency_limit -= 1
                last_concurrency_change = now
                app_logger.info(
                    f"Auto-concurrency: decreased to {concurrency_limit} (CPU {cpu:.1f}%, MEM {mem:.1f}%)"
                )
            elif cpu < CPU_LOWER_THRESHOLD and mem < MEM_UPPER_THRESHOLD and concurrency_limit < AUTO_MAX_CONCURRENCY:
                concurrency_limit += 1
                last_concurrency_change = now
                app_logger.info(
                    f"Auto-concurrency: increased to {concurrency_limit} (CPU {cpu:.1f}%, MEM {mem:.1f}%)"
                )
            if concurrency_limit > AUTO_MAX_CONCURRENCY:
                concurrency_limit = AUTO_MAX_CONCURRENCY
            if concurrency_limit < AUTO_MIN_CONCURRENCY:
                concurrency_limit = AUTO_MIN_CONCURRENCY
            async with concurrency_condition:
                concurrency_condition.notify_all()
        await asyncio.sleep(CHECK_INTERVAL)

#######################################################################
#                     AUTHENTICATION & SESSION PRIMING
#######################################################################
async def check_if_login_needed(page: Page, test_url: str) -> bool:
    """Check whether the current session is authenticated.

    Args:
        page (Page): Page used to perform the check.
        test_url (str): URL expected to load when logged in.

    Returns:
        bool: ``True`` if a login is required, ``False`` otherwise.
    """
    app_logger.info(f"Verifying session status by navigating to: {test_url}")
    try:
        response = await page.goto(test_url, timeout=PAGE_TIMEOUT, wait_until="load")
        await page.wait_for_timeout(3000)
        current_url = page.url
        if "signin" in current_url.lower() or "/ap/" in current_url:
            return True
        if response and not response.ok:
            return True
        dashboard_element_selector = "#content > div > div.mainAppContainerExternal > div.css-6pahkd.action-bar-container > div > div.filterbar-right-slot > kat-button:nth-child(2) > button"
        await expect(page.locator(dashboard_element_selector)).to_be_visible(timeout=WAIT_TIMEOUT)
        app_logger.info("Session check successful.")
        return False
    except Exception as e:
        app_logger.error(f"Error during session check: {e}", exc_info=DEBUG_MODE)
        return True

async def perform_login_and_otp(page: Page) -> bool:
    """Log in to Seller Central and handle OTP if prompted.

    This routine navigates to the login page, fills in the credentials from the
    configuration file and, if two-factor authentication is enabled, submits the
    current TOTP value. When the dashboard or account picker page becomes
    visible the function returns ``True``. Any unexpected issue results in a
    screenshot and ``False`` is returned.

    Args:
        page (Page): Playwright page instance used for the login flow.

    Returns:
        bool: ``True`` on a successful login, ``False`` otherwise.
    """
    app_logger.info(f"Navigating to login page: {LOGIN_URL}")
    try:
        await page.goto(LOGIN_URL, timeout=PAGE_TIMEOUT, wait_until="load")
        app_logger.info("Initial page loaded. Determining login flow...")

        continue_shopping_selector = 'button:has-text("Continue shopping")'
        email_field_selector = 'input#ap_email'

        await page.wait_for_selector(f"{continue_shopping_selector}, {email_field_selector}", state="visible", timeout=15000)
        
        if await page.locator(continue_shopping_selector).is_visible():
            app_logger.info("Flow: Interstitial 'Continue shopping' page detected. Clicking it.")
            await page.locator(continue_shopping_selector).click()
            await expect(page.locator(email_field_selector)).to_be_visible(timeout=15000)
        else:
            app_logger.info("Flow: Login form with email field loaded directly.")
        
        await page.get_by_label("Email or mobile phone number").fill(config['login_email'])
        await page.get_by_label("Continue").click()

        password_field = page.get_by_label("Password")
        await expect(password_field).to_be_visible(timeout=10000)
        await password_field.fill(config['login_password'])
        await page.get_by_label("Sign in").click()
        
        otp_selector = 'input[id*="otp"]'
        dashboard_selector = "#content > div > div.mainAppContainerExternal"
        await page.wait_for_selector(f"{otp_selector}, {dashboard_selector}", timeout=30000)

        otp_field = page.locator(otp_selector)
        if await otp_field.is_visible():
            app_logger.info("Two-Step Verification (OTP) is required.")
            otp_code = pyotp.TOTP(config['otp_secret_key']).now()
            await otp_field.fill(otp_code)
            if await page.locator("input[type='checkbox'][name='rememberDevice']").is_visible():
                await page.locator("input[type='checkbox'][name='rememberDevice']").check()
            await page.get_by_role("button", name="Sign in").click()

        account_picker_selector = 'h1:has-text("Select an account")'
        await page.wait_for_selector(f"{dashboard_selector}, {account_picker_selector}", timeout=30000)
        
        app_logger.info("Login process appears fully successful.")
        return True
    except Exception as e:
        app_logger.critical(f"Critical error during login process: {e}", exc_info=DEBUG_MODE)
        await _save_screenshot(page, "login_critical_failure")
        return False

async def prime_master_session() -> bool:
    """Authenticate once and persist the storage state for workers.

    Returns:
        bool: ``True`` if login succeeds and state is saved, ``False`` otherwise.
    """
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

async def post_to_chat_webhook(entries: List[Dict[str, str]]):
    """Send a detailed, table-formatted card message to the Google Chat webhook."""
    if not CHAT_WEBHOOK_URL or not entries:
        return
    try:
        global chat_batch_count
        chat_batch_count += 1
        batch_header_text = datetime.now(LOCAL_TIMEZONE).strftime(
            "%A %d %B, %H:%M"
        )
        card_subtitle = f"{batch_header_text}  Batch {chat_batch_count} ({len(entries)} stores)"

        # Sort entries alphabetically by sanitized store name for all sections
        sorted_entries = sorted(
            entries,
            key=lambda e: sanitize_store_name(e.get("store", ""))
        )

        # --- 1. Build the Grid/Table Widget for Key Metrics ---
        # The grid widget requires a flat list of items. We add headers first, then each row's cells.
        grid_items = [
            # Header Row - Using bold markdown-style formatting
            {"title": "*Store*", "textAlignment": "START"},
            {"title": "*UPH*", "textAlignment": "CENTER"},
            {"title": "*Lates*", "textAlignment": "CENTER"},
            {"title": "*INF*", "textAlignment": "CENTER"},
        ]

        # Add a row for each store with the key metrics
        for entry in sorted_entries:
            # Clean up values for a neater display in the table
            uph = entry.get("uph", "N/A")
            # Ensure "Lates" and "INF" always have a value and consistent spacing
            lates = (entry.get("lates", "0.0 %") or "0.0 %").replace(" %", "%")
            inf = (entry.get("inf", "0.0 %") or "0.0 %").replace(" %", "%")

            grid_items.extend([
                {"title": sanitize_store_name(entry.get("store", "N/A")), "textAlignment": "START"},
                {"title": uph, "textAlignment": "CENTER"},
                {"title": lates, "textAlignment": "CENTER"},
                {"title": inf, "textAlignment": "CENTER"},
            ])
        
        # This is the section that contains the grid widget.
        table_section = {
            "header": "Key Performance Indicators",
            "collapsible": False, # Keep the main table visible
            "uncollapsibleWidgetsCount": 1,
            "widgets": [{
                "grid": {
                    "title": "Performance Summary",
                    "columnCount": 4, # As defined by our headers
                    "borderStyle": {
                        "type": "STROKE",
                        "cornerRadius": 4
                    },
                    "items": grid_items
                }
            }]
        }

        # --- 2. Build the original full-detail collapsible widgets ---
        # This provides a more compact way to show the full details upon expansion.
        detail_widgets = []
        for entry in sorted_entries:
            # Consolidate all metrics into a single 'decoratedText' widget per store
            # using HTML line breaks for structure. This is more efficient.
            full_details_text = (
                f'{build_metric_line([("Orders", entry.get("orders", "N/A")), ("Units", entry.get("units", "N/A")), ("Fulfilled", entry.get("fulfilled", "N/A"))])}<br>'
                f'{build_metric_line([("UPH", entry.get("uph", "N/A")), ("INF", entry.get("inf", "N/A")), ("Found", entry.get("found", "N/A"))])}<br>'
                f'{build_metric_line([("Cancelled", entry.get("cancelled", "N/A")), ("Lates", entry.get("lates", "N/A")), ("Avail", entry.get("time_available", "N/A"))])}'
            )

            detail_widgets.append({
                "decoratedText": {
                    "topLabel": sanitize_store_name(entry.get("store", "Store")),
                    "startIcon": {"knownIcon": "STORE"},
                    "text": full_details_text
                }
            })
        
        # This section contains the full details list and is collapsible.
        details_section = {
            "header": "Full Details (All Stores)",
            "collapsible": True,
            "uncollapsibleWidgetsCount": 0, # Hide all widgets until expanded
            "widgets": detail_widgets
        }

        # --- 3. Assemble the final payload with both the table and the details ---
        payload = {
            "cardsV2": [{
                "cardId": f"batch-summary-{chat_batch_count}",
                "card": {
                    "header": {
                        "title": "Seller Central Metrics Report",
                        "subtitle": card_subtitle,
                        "imageUrl": "https://i.imgur.com/u0e3d2x.png", # Amazon 'a' logo
                        "imageType": "CIRCLE"
                    },
                    # The sections are rendered in order. Table first, then details.
                    "sections": [table_section, details_section],
                },
            }]
        }
        
        timeout = aiohttp.ClientTimeout(total=20)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(CHAT_WEBHOOK_URL, json=payload) as resp:
                if resp.status != 200:
                    error_text = await resp.text()
                    app_logger.error(
                        f"Chat webhook post failed. Status: {resp.status}. Response: {error_text}"
                    )
    except Exception as e:
        app_logger.error(f"Error posting to chat webhook: {e}", exc_info=DEBUG_MODE)


async def add_to_pending_chat(entry: Dict[str, str]):
    """Accumulate chat entries and send in batches."""
    if not CHAT_WEBHOOK_URL:
        return
    async with pending_chat_lock:
        pending_chat_entries.append(entry)
        if len(pending_chat_entries) >= CHAT_BATCH_SIZE:
            entries_to_send = pending_chat_entries[:CHAT_BATCH_SIZE]
            del pending_chat_entries[:CHAT_BATCH_SIZE]
            await post_to_chat_webhook(entries_to_send)


async def flush_pending_chat_entries():
    """Send any remaining chat entries."""
    if not CHAT_WEBHOOK_URL:
        return
    async with pending_chat_lock:
        if pending_chat_entries:
            entries = pending_chat_entries[:]
            pending_chat_entries.clear()
            await post_to_chat_webhook(entries)


async def log_submission(data: Dict[str,str]):
    """Write a submission entry to CSV and JSON logs.

    Args:
        data (Dict[str, str]): Form fields that were sent to Google Forms.

    Returns:
        None
    """
    async with log_lock:
        current_timestamp = datetime.now(LOCAL_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
        log_entry = {'timestamp': current_timestamp, **data}
        fieldnames = ['timestamp','store','orders','units','fulfilled','uph','inf','found','cancelled','lates','time_available']
        new_csv = not os.path.exists(LOG_FILE)
        try:
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames, extrasaction='ignore')
            if new_csv:
                writer.writeheader()
            writer.writerow(log_entry)
            async with aiofiles.open(LOG_FILE, 'a', newline='', encoding='utf-8') as f:
                await f.write(csv_buffer.getvalue())
        except IOError as e:
            app_logger.error(f"Error writing to CSV log file {LOG_FILE}: {e}")
        try:
            async with aiofiles.open(JSON_LOG_FILE, 'a', encoding='utf-8') as f:
                await f.write(json.dumps(log_entry) + '\n')
        except IOError as e:
            app_logger.error(f"Error writing to JSON log file {JSON_LOG_FILE}: {e}")
        await add_to_pending_chat(log_entry)

async def http_form_submitter_worker(queue: Queue, worker_id: int):
    """Submit queued form data via HTTP.

    Args:
        queue (Queue): Queue containing form dictionaries to submit.
        worker_id (int): Identifier used for logging output.

    Returns:
        None
    """
    log_prefix = f"[HTTP-Submitter-{worker_id}]"
    app_logger.info(f"{log_prefix} Starting up...")
    timeout = aiohttp.ClientTimeout(total=20)
    ssl_context = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_context)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            form_data = None
            try:
                form_data = await queue.get()
                store_name = form_data.get('store', 'Unknown')
                payload = {FIELD_MAP[key]: value for key, value in form_data.items() if key in FIELD_MAP}
                async with session.post(FORM_POST_URL, data=payload) as resp:
                    if resp.status == 200:
                        await log_submission(form_data)
                        with progress_lock:
                            progress["current"] += 1
                            progress["lastUpdate"] = f"{datetime.now(LOCAL_TIMEZONE).strftime('%H:%M')} [Submitted] {store_name}"
                    else:
                        error_text = await resp.text()
                        app_logger.error(f"{log_prefix} Submission for {store_name} failed. Status: {resp.status}. Response: {error_text[:200]}")
                        run_failures.append(f"{store_name} (HTTP Submit Fail {resp.status})")
            except asyncio.CancelledError:
                break
            except Exception as e:
                failed_store = form_data.get('store', 'Unknown') if form_data else "Unknown"
                app_logger.error(f"{log_prefix} Unhandled exception for {failed_store}: {e}", exc_info=DEBUG_MODE)
                run_failures.append(f"{failed_store} (Submit Exception)")
            finally:
                if form_data:
                    queue.task_done()
    app_logger.info(f"{log_prefix} Shut down.")

async def data_collector_worker(browser: Browser, store_info: Dict[str,str], storage_template: Dict, queue: Queue):
    """Collect metrics for a single store and enqueue them for submission.

    A new browser context is created using the provided ``storage_template`` to
    reuse authenticated session data. After navigating to the store's dashboard
    the worker waits for the network response containing the performance
    metrics, parses the required fields and puts a formatted dictionary into the
    submission ``queue``. Failures are retried up to ``WORKER_RETRY_COUNT``
    before the store is marked as failed.

    Args:
        browser (Browser): Shared Playwright browser instance.
        store_info (Dict[str, str]): Mapping containing ``merchant_id``,
            ``store_name`` and ``marketplace_id`` keys for the target store.
        storage_template (Dict): Storage state template with authentication
            cookies.
        queue (Queue): Queue into which collected metrics are placed.
    """
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

            # --- UPDATED LATES SCRAPING LOGIC ---
            formatted_lates = "0 %"
            try:
                # Step 1: Locate the target row and cell.
                header_second_row = page.locator("kat-table-head kat-table-row").nth(1)
                lates_cell = header_second_row.locator("kat-table-cell").nth(10)

                # Step 2: CRUCIAL FIX - Explicitly wait for the cell to be visible.
                # This ensures we don't try to read the value before it's populated by JavaScript.
                await expect(lates_cell).to_be_visible(timeout=10000)

                # Step 3: Get the text from the cell and add a log for debugging.
                # This will show us exactly what the script sees in every run.
                cell_text = (await lates_cell.text_content() or "").strip()
                app_logger.info(f"[{store_name}] Raw 'Lates' text scraped: '{cell_text}'")

                # Step 4: Validate the text format.
                if re.fullmatch(r"\d+(\.\d+)?\s*%", cell_text):
                    formatted_lates = cell_text
                    app_logger.info(f"[{store_name}] Successfully parsed 'Lates' as: {formatted_lates}")
                elif cell_text: # If we got text but it wasn't the right format.
                    app_logger.warning(f"[{store_name}] Scraped 'Lates' value '{cell_text}' but it didn't match format, defaulting to 0 %.")
                else: # If the cell was empty after waiting.
                    app_logger.warning(f"[{store_name}] 'Lates' cell was visible but empty, defaulting to 0 %.")

            except TimeoutError:
                # This will now only trigger if the cell *never* becomes visible.
                app_logger.warning(f"[{store_name}] Timed out waiting for the 'Lates' cell to become visible, defaulting to 0 %.")
            except Exception as e:
                app_logger.error(f"[{store_name}] An unexpected error occurred while scraping 'Lates': {e}", exc_info=DEBUG_MODE)
            # --- END OF UPDATED LATES LOGIC ---

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
    """Run a single data collector while respecting the concurrency limit.

    Args:
        store_item (Dict): Details for the store being processed.
        browser (Browser): Shared browser instance.
        storage_template (Dict): Authentication state template.
        queue (Queue): Queue where collected metrics are enqueued.

    Returns:
        None
    """
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
    """Main orchestration routine for scraping and submission.

    Returns:
        None
    """
    global progress, start_time, run_failures, browser
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
        MAX_LOGIN_ATTEMPTS = 3
        login_successful = False
        for attempt in range(MAX_LOGIN_ATTEMPTS):
            app_logger.info(f"Attempting to prime a new master session (Attempt {attempt + 1}/{MAX_LOGIN_ATTEMPTS})...")
            if await prime_master_session():
                login_successful = True
                break
            if attempt < MAX_LOGIN_ATTEMPTS - 1:
                app_logger.warning(f"Session priming failed on attempt {attempt + 1}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
        
        if not login_successful:
            app_logger.critical(f"Critical: Session priming failed after {MAX_LOGIN_ATTEMPTS} attempts. Aborting job.")
            return

    with open(STORAGE_STATE) as f: storage_template = json.load(f)
    submission_queue = Queue()

    app_logger.info(f"Starting {NUM_FORM_SUBMITTERS} HTTP form submitter worker(s).")
    form_submitter_tasks = [
        asyncio.create_task(http_form_submitter_worker(submission_queue, i + 1))
        for i in range(NUM_FORM_SUBMITTERS)
    ]

    auto_task = None
    if AUTO_ENABLED:
        auto_task = asyncio.create_task(auto_concurrency_manager())

    with progress_lock: progress = {"current": 0, "total": len(urls_data), "lastUpdate": "N/A"}
    start_time = datetime.now(LOCAL_TIMEZONE)

    collector_tasks = [managed_worker(si, browser, storage_template, submission_queue) for si in urls_data]
    await asyncio.gather(*collector_tasks)

    app_logger.info("All data collectors finished. Waiting for submission queue to empty...")
    await submission_queue.join()
    await flush_pending_chat_entries()
    
    app_logger.info("Cancelling form submitter workers...")
    for task in form_submitter_tasks: task.cancel()
    await asyncio.gather(*form_submitter_tasks, return_exceptions=True)

    if auto_task:
        auto_task.cancel()
        await asyncio.gather(auto_task, return_exceptions=True)

    elapsed = (datetime.now(LOCAL_TIMEZONE) - start_time).total_seconds()
    app_logger.info(f"Processing finished. Processed {progress['current']}/{progress['total']} in {elapsed:.2f}s")
    if run_failures:
        app_logger.warning(f"Completed with {len(run_failures)} issue(s): {', '.join(run_failures)}")
    else:
        app_logger.info("Completed successfully.")

#######################################################################
#                         MAIN EXECUTION BLOCK
#######################################################################

async def main():
    """Entry point for running the scraper from the command line.

    Returns:
        None
    """
    global playwright, browser
    app_logger.info("Starting up in single-run mode...")
    try:
        playwright = await async_playwright().start()
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
        await flush_pending_chat_entries()
        app_logger.info("Run complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        app_logger.info("Script interrupted by user. Exiting.")