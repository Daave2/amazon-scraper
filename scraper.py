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

# --- Constants for target-based emojis ---
EMOJI_GREEN_CHECK = "\u2705" # ✅
EMOJI_RED_CROSS = "\u274C"   # ❌
UPH_THRESHOLD = 80
LATES_THRESHOLD = 3.0
INF_THRESHOLD = 2.0

def sanitize_store_name(name: str) -> str:
    """Trim standard prefix from store names for chat display."""
    return STORE_PREFIX_RE.sub("", name).strip()

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

PAGE_TIMEOUT    = config.get('page_timeout_ms', 30000) # Reduced to 30s for fail-fast
WAIT_TIMEOUT    = config.get('element_wait_timeout_ms', 10000) # Reduced to 10s
ACTION_TIMEOUT = int(PAGE_TIMEOUT / 2)
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
failure_timestamps = [] # List of timestamps of recent failures
failure_lock = asyncio.Lock()

# Metrics for Advanced Reporting
metrics = {
    "collection_times": [], # List of (store_name, duration_seconds)
    "submission_times": [], # List of (store_name, duration_seconds)
    "retries": 0,
    "total_orders": 0,
    "total_units": 0,
    "retry_stores": set()
}
metrics_lock = asyncio.Lock()

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
    global concurrency_limit, last_concurrency_change
    if not AUTO_ENABLED:
        return
    app_logger.info(
        f"Auto-concurrency enabled with range {AUTO_MIN_CONCURRENCY}-{AUTO_MAX_CONCURRENCY}"
    )
    while True:
        now = asyncio.get_event_loop().time()
        
        # 1. Check Failure Rate (Error-Aware Scaling)
        async with failure_lock:
            # Keep only failures from last 60s
            while failure_timestamps and now - failure_timestamps[0] > 60:
                failure_timestamps.pop(0)
            recent_failure_count = len(failure_timestamps)
        
        # Estimate current rate (requests per minute) based on concurrency
        # Assuming ~2s per request per worker -> 30 req/min per worker
        estimated_throughput = concurrency_limit * 30 
        failure_rate = recent_failure_count / max(estimated_throughput, 1)
        
        if failure_rate > 0.05: # >5% failure rate
            if now - last_concurrency_change >= COOLDOWN_SECONDS:
                concurrency_limit = max(AUTO_MIN_CONCURRENCY, int(concurrency_limit * 0.5))
                last_concurrency_change = now
                app_logger.warning(
                    f"Auto-concurrency: THROTTLING DOWN to {concurrency_limit} due to high failure rate ({failure_rate:.1%})"
                )
                async with concurrency_condition:
                    concurrency_condition.notify_all()
                await asyncio.sleep(COOLDOWN_SECONDS * 2) # Wait longer to recover
                continue

        # 2. Standard Resource Scaling
        cpu = psutil.cpu_percent(interval=None)
        mem = psutil.virtual_memory().percent
        
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
    app_logger.info(f"Verifying session status by navigating to: {test_url}")
    try:
        # We don't wait for 'load' event to finish because it might take time.
        # We just want to see if we land on login page or dashboard.
        await page.goto(test_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
        
        # Smart wait: Race between Login elements and Dashboard elements
        # If we see login inputs -> Login needed
        # If we see dashboard elements -> Login NOT needed
        
        login_selector = "input#ap_email, input#ap_password, input[name='email']"
        dashboard_selector = "#content > div > div.mainAppContainerExternal"
        
        try:
            # Wait for either to appear
            found = await page.locator(f"{login_selector}, {dashboard_selector}").first.is_visible(timeout=10000)
            if not found:
                # Fallback check on URL if neither appeared quickly
                if "signin" in page.url.lower() or "/ap/" in page.url:
                    return True
                return True # Assume needed if we can't verify dashboard
        except TimeoutError:
             # If timeout, check URL one last time
            if "signin" in page.url.lower() or "/ap/" in page.url:
                return True
            return True

        # If we are here, something is visible. Check what it is.
        if await page.locator(login_selector).first.is_visible():
            app_logger.info("Login form detected.")
            return True
            
        if await page.locator(dashboard_selector).is_visible():
            app_logger.info("Dashboard detected. Session is valid.")
            return False
            
        return True
    except Exception as e:
        app_logger.error(f"Error during session check: {e}", exc_info=DEBUG_MODE)
        return True

async def perform_login_and_otp(page: Page) -> bool:
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
        
        email_locator = page.get_by_label("Email or mobile phone number")
        try:
            await email_locator.fill(config['login_email'])
        except TimeoutError:
            app_logger.warning(
                "Email field label not found or not interactable. Falling back to direct selector.")
            fallback_email_field = page.locator(email_field_selector)
            await expect(fallback_email_field).to_be_visible(timeout=10000)
            await fallback_email_field.fill(config['login_email'])

        continue_locator = page.get_by_label("Continue")
        try:
            await continue_locator.click()
        except TimeoutError:
            app_logger.warning(
                "Continue control not available via label. Using fallback selector.")
            fallback_continue = page.get_by_role("button", name=re.compile("continue", re.I))
            if await fallback_continue.count() == 0:
                fallback_continue = page.locator("input#continue, button#continue, input[name='continue']")
            await expect(fallback_continue.first).to_be_visible(timeout=10000)
            await fallback_continue.first.click()

        password_field = page.get_by_label("Password")
        try:
            await expect(password_field).to_be_visible(timeout=10000)
        except TimeoutError:
            app_logger.warning(
                "Password field not visible after entering email. Attempting to bypass passkey flow.")

            async def _click_if_visible(locator: Any) -> bool:
                try:
                    if locator and await locator.count() > 0:
                        visible_locator = locator.first
                        if await visible_locator.is_visible():
                            await visible_locator.click()
                            return True
                except PlaywrightError as inner_error:
                    app_logger.debug(
                        f"Encountered error while handling alternate sign-in option: {inner_error}",
                        exc_info=DEBUG_MODE,
                    )
                return False

            bypass_attempted = False

            other_ways_button = page.get_by_role("button", name=re.compile("other ways to sign in", re.I))
            if await _click_if_visible(other_ways_button):
                app_logger.info("Clicked 'Other ways to sign in' button to reveal password option.")
                bypass_attempted = True

            if not bypass_attempted:
                passkey_bypass_selectors = [
                    page.get_by_role("button", name=re.compile("use( your)? password", re.I)),
                    page.get_by_role("link", name=re.compile("use( your)? password", re.I)),
                    page.locator("text=/Use (your )?password/i"),
                    page.locator("text=/Sign-in without passkey/i"),
                ]
                for locator in passkey_bypass_selectors:
                    if await _click_if_visible(locator):
                        app_logger.info("Clicked alternate sign-in option to fall back to password entry.")
                        bypass_attempted = True
                        break

            if not bypass_attempted:
                app_logger.warning(
                    "No passkey bypass option detected. Proceeding without additional interaction.")

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

def _format_metric_with_emoji(value_str: str, threshold: float, is_uph: bool = False) -> str:
    """Applies a pass/fail emoji to a metric string based on a threshold."""
    try:
        numeric_value = float(re.sub(r'[^\d.]', '', value_str))
        is_good = (numeric_value >= threshold) if is_uph else (numeric_value <= threshold)
        emoji = EMOJI_GREEN_CHECK if is_good else EMOJI_RED_CROSS
        return f"{emoji} {value_str}"
    except (ValueError, TypeError):
        return value_str # Return as is if not a number

async def post_to_chat_webhook(entries: List[Dict[str, str]]):
    """Send a table-formatted card message with emoji indicators."""
    if not CHAT_WEBHOOK_URL or not entries:
        return
    try:
        global chat_batch_count
        chat_batch_count += 1
        batch_header_text = datetime.now(LOCAL_TIMEZONE).strftime("%A %d %B, %H:%M")
        card_subtitle = f"{batch_header_text}  Batch {chat_batch_count} ({len(entries)} stores)"

        sorted_entries = sorted(entries, key=lambda e: sanitize_store_name(e.get("store", "")))

        # --- Build the Grid/Table Widget with Emoji Indicators ---
        grid_items = [
            {"title": "Store", "textAlignment": "START"},
            {"title": "UPH", "textAlignment": "CENTER"},
            {"title": "Lates", "textAlignment": "CENTER"},
            {"title": "INF", "textAlignment": "CENTER"},
        ]

        for entry in sorted_entries:
            uph_val = entry.get("uph", "N/A")
            lates_val = entry.get("lates", "0.0 %") or "0.0 %"
            inf_val = entry.get("inf", "0.0 %") or "0.0 %"

            # Apply emoji formatting
            formatted_uph = _format_metric_with_emoji(uph_val, UPH_THRESHOLD, is_uph=True)
            formatted_lates = _format_metric_with_emoji(lates_val, LATES_THRESHOLD)
            formatted_inf = _format_metric_with_emoji(inf_val, INF_THRESHOLD)

            grid_items.extend([
                {"title": sanitize_store_name(entry.get("store", "N/A")), "textAlignment": "START"},
                {"title": formatted_uph, "textAlignment": "CENTER"},
                {"title": formatted_lates, "textAlignment": "CENTER"},
                {"title": formatted_inf, "textAlignment": "CENTER"},
            ])
        
        table_section = {
            "header": "Key Performance Indicators",
            "widgets": [{
                "grid": {
                    "title": "Performance Summary",
                    "columnCount": 4,
                    "borderStyle": {"type": "STROKE", "cornerRadius": 4},
                    "items": grid_items
                }
            }]
        }

        # --- Assemble the final payload ---
        payload = {
            "cardsV2": [{
                "cardId": f"batch-summary-{chat_batch_count}",
                "card": {
                    "header": {
                        "title": "Seller Central Metrics Report",
                        "subtitle": card_subtitle,
                        "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": [table_section],
                },
            }]
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
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


async def post_job_summary(total: int, success: int, failures: List[str], duration: float):
    """Send a detailed job summary card to Google Chat with advanced analytics."""
    if not CHAT_WEBHOOK_URL: return
    try:
        status_text = "Job Completed Successfully"
        status_icon = "✅"
        if failures:
            status_text = f"Job Completed with {len(failures)} Failures"
            status_icon = "⚠️"
        
        success_rate = (success / total) * 100 if total > 0 else 0
        throughput_spm = (success / (duration / 60)) if duration > 0 else 0
        
        # Calculate Analytics
        async with metrics_lock:
            coll_times = metrics["collection_times"]
            sub_times = metrics["submission_times"]
            retries = metrics["retries"]
            retry_stores = len(metrics["retry_stores"])
            total_orders = metrics["total_orders"]
            total_units = metrics["total_units"]
            
        avg_coll = sum(t[1] for t in coll_times) / len(coll_times) if coll_times else 0
        avg_sub = sum(t[1] for t in sub_times) / len(sub_times) if sub_times else 0
        
        # P95 Latency
        sorted_coll = sorted([t[1] for t in coll_times])
        p95_coll = sorted_coll[int(len(sorted_coll) * 0.95)] if sorted_coll else 0
        
        fastest_store = min(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        slowest_store = max(coll_times, key=lambda x: x[1]) if coll_times else ("N/A", 0)
        
        # Bottleneck Analysis
        bottleneck_msg = "Balanced Flow"
        if avg_coll > 2.0:
            bottleneck_msg = "🐢 Slow Scraping (Browser Lag)"
        elif avg_sub > 1.0:
            bottleneck_msg = "🐢 Slow Submission (Webhook Lag)"
        elif avg_coll < 1.0 and avg_sub < 0.5:
            bottleneck_msg = "🚀 High Speed (No Bottlenecks)"

        # Sections
        stats_section = {
            "header": "High-Level Stats",
            "widgets": [
                {
                    "decoratedText": {
                        "topLabel": "Throughput",
                        "text": f"{throughput_spm:.1f} stores/min",
                        "startIcon": {"knownIcon": "FLIGHT_DEPARTURE"}
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "Success Rate",
                        "text": f"{success}/{total} ({success_rate:.1f}%)",
                        "startIcon": {"knownIcon": "STAR"}
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "Total Duration",
                        "text": f"{duration:.2f}s",
                        "startIcon": {"knownIcon": "CLOCK"}
                    }
                }
            ]
        }
        
        volume_section = {
            "header": "Business Volume 📦",
            "widgets": [
                {
                    "decoratedText": {
                        "topLabel": "Total Orders",
                        "text": f"{total_orders:,}",
                        "startIcon": {"knownIcon": "SHOPPING_CART"}
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "Total Units",
                        "text": f"{total_units:,}",
                        "startIcon": {"knownIcon": "TICKET"}
                    }
                }
            ]
        }

        resilience_section = {
            "header": "Resilience & Health 🏥",
            "widgets": [
                {
                    "decoratedText": {
                        "topLabel": "Total Retries",
                        "text": str(retries),
                        "startIcon": {"knownIcon": "MEMBERSHIP"} # Best fit for 'repeat'
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "Stores Retried",
                        "text": str(retry_stores),
                        "startIcon": {"knownIcon": "STORE"}
                    }
                }
            ]
        }
        
        speed_section = {
            "header": "Speed Breakdown ⏱️",
            "widgets": [
                {
                    "decoratedText": {
                        "topLabel": "Avg Collection Time",
                        "text": f"{avg_coll:.2f}s (Browser)",
                        "startIcon": {"knownIcon": "DESCRIPTION"}
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "p95 Collection Time",
                        "text": f"{p95_coll:.2f}s",
                        "startIcon": {"knownIcon": "DESCRIPTION"}
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "Bottleneck Status",
                        "text": bottleneck_msg,
                        "startIcon": {"knownIcon": "TRAFFIC"}
                    }
                }
            ]
        }
        
        extremes_section = {
            "header": "Extremes 📉📈",
            "widgets": [
                {
                    "decoratedText": {
                        "topLabel": "Fastest Store",
                        "text": f"{fastest_store[0]} ({fastest_store[1]:.2f}s)",
                        "startIcon": {"knownIcon": "BOLT"}
                    }
                },
                {
                    "decoratedText": {
                        "topLabel": "Slowest Store",
                        "text": f"{slowest_store[0]} ({slowest_store[1]:.2f}s)",
                        "startIcon": {"knownIcon": "SNAIL"}
                    }
                }
            ]
        }
        
        sections = [stats_section, volume_section, resilience_section, speed_section, extremes_section]
        
        if failures:
            # Group failures by type
            failure_counts = {}
            for f in failures:
                # Heuristic: Extract the error part in parentheses or the whole string
                msg = f
                if '(' in f and ')' in f:
                    msg = f[f.rfind('(')+1 : f.rfind(')')]
                failure_counts[msg] = failure_counts.get(msg, 0) + 1
            
            failure_summary = "\n".join([f"• {k}: {v}" for k, v in failure_counts.items()])
            
            failure_list = "\n".join([f"• {f}" for f in failures[:5]])
            if len(failures) > 5:
                failure_list += f"\n...and {len(failures) - 5} more"
            
            failures_section = {
                "header": "Failure Analysis",
                "widgets": [
                    {
                        "textParagraph": {
                            "text": f"<b>Breakdown:</b>\n{failure_summary}"
                        }
                    },
                    {
                        "textParagraph": {
                            "text": f"<font color=\"#FF0000\"><b>Recent Failures:</b>\n{failure_list}</font>"
                        }
                    }
                ]
            }
            sections.append(failures_section)

        payload = {
            "cardsV2": [{
                "cardId": f"job-summary-{int(datetime.now().timestamp())}",
                "card": {
                    "header": {
                        "title": f"{status_icon} {status_text}",
                        "subtitle": datetime.now(LOCAL_TIMEZONE).strftime("%A %d %B, %H:%M"),
                        "imageUrl": "https://i.imgur.com/u0e3d2x.png",
                        "imageType": "CIRCLE"
                    },
                    "sections": sections,
                },
            }]
        }
        
        timeout = aiohttp.ClientTimeout(total=30)
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            async with session.post(CHAT_WEBHOOK_URL, json=payload) as resp:
                if resp.status != 200:
                    app_logger.error(f"Job summary post failed: {resp.status}")

    except Exception as e:
        app_logger.error(f"Error posting job summary: {e}", exc_info=DEBUG_MODE)


async def add_to_pending_chat(entry: Dict[str, str]):
    if not CHAT_WEBHOOK_URL:
        return
    async with pending_chat_lock:
        pending_chat_entries.append(entry)
        if len(pending_chat_entries) >= CHAT_BATCH_SIZE:
            entries_to_send = pending_chat_entries[:CHAT_BATCH_SIZE]
            del pending_chat_entries[:CHAT_BATCH_SIZE]
            await post_to_chat_webhook(entries_to_send)


async def flush_pending_chat_entries():
    if not CHAT_WEBHOOK_URL:
        return
    async with pending_chat_lock:
        if pending_chat_entries:
            entries = pending_chat_entries[:]
            pending_chat_entries.clear()
            await post_to_chat_webhook(entries)


async def log_submission(data: Dict[str,str]):
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
                # The original payload creation using FIELD_MAP is removed as per instruction,
                # assuming form_data is directly suitable for GOOGLE_FORM_URL.
                
                submit_start = asyncio.get_event_loop().time()
                async with session.post(FORM_POST_URL, data=form_data, timeout=10) as resp:
                    if resp.status == 200:
                        await log_submission(form_data)
                        app_logger.info(f"{log_prefix} Submitted data for {form_data.get('store', 'Unknown')}")
                        with progress_lock:
                            progress["current"] += 1
                            progress["lastUpdate"] = datetime.now(LOCAL_TIMEZONE).strftime("%H:%M:%S")
                        
                        submit_duration = asyncio.get_event_loop().time() - submit_start
                        async with metrics_lock:
                            metrics["submission_times"].append((form_data.get('store', 'Unknown'), submit_duration))
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

async def process_single_store(context: BrowserContext, store_info: Dict[str,str], queue: Queue):
    start_ts = asyncio.get_event_loop().time()
    merchant_id = store_info['merchant_id']
    store_name  = store_info['store_name']
    
    for attempt in range(WORKER_RETRY_COUNT):
        # ... (rest of function) ...
        # I need to wrap the logic to capture success time.
        # Since I can't easily wrap the whole function without re-writing it all, 
        # I will just capture the time before the return.
        pass 

    # Wait, I should rewrite the function signature to inject the start time or just measure it inside.
    # Let's replace the whole function to be safe and clean.

    
    for attempt in range(WORKER_RETRY_COUNT):
        # We use a new page for each store, but share the context (cookies/storage)
        page = None
        try:
            marketplace_id = store_info['marketplace_id']
            if not marketplace_id:
                app_logger.error(f"Skipping {store_name}: marketplace_id is missing.")
                run_failures.append(f"{store_name} (Missing MKID)")
                return

            page = await context.new_page()
            
            # Block resources on this page
            async def block_resources(route):
                if (any(domain in route.request.url for domain in RESOURCE_BLOCKLIST) or
                        route.request.resource_type in ("image", "stylesheet", "font", "media")):
                    await route.abort()
                else:
                    await route.continue_()
            await page.route("**/*", block_resources)

            dash_url = f"https://sellercentral.amazon.co.uk/snowdash?ref_=mp_home_logo_xx&cor=mmp_EU&mons_sel_dir_mcid={merchant_id}&mons_sel_mkid={marketplace_id}"
            await page.goto(dash_url, timeout=PAGE_TIMEOUT, wait_until="domcontentloaded")
            
            refresh_button_selector = "#content > div > div.mainAppContainerExternal > div.css-6pahkd.action-bar-container > div > div.filterbar-right-slot > kat-button:nth-child(2) > button"
            METRICS_TIMEOUT = 45_000 # Increased to 45s for better reliability
            
            # Wait for metrics response
            async with page.expect_response(lambda r: "summationMetrics" in r.url and r.status == 200, timeout=METRICS_TIMEOUT) as resp_info:
                refresh_button = page.locator(refresh_button_selector)
                await expect(refresh_button).to_be_visible(timeout=WAIT_TIMEOUT)
                await refresh_button.click()
            
            response = await resp_info.value
            api_data = await response.json()

            formatted_lates = "0 %"
            try:
                header_second_row = page.locator("kat-table-head kat-table-row").nth(1)
                lates_cell = header_second_row.locator("kat-table-cell").nth(10)
                # Short timeout for UI check since we have API data mostly
                if await lates_cell.is_visible(timeout=5000):
                    cell_text = (await lates_cell.text_content() or "").strip()
                    if re.fullmatch(r"\d+(\.\d+)?\s*%", cell_text):
                        formatted_lates = cell_text
            except Exception:
                pass # Non-critical UI scrape

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
            
            duration = asyncio.get_event_loop().time() - start_ts
            async with metrics_lock:
                metrics["collection_times"].append((store_name, duration))
                metrics["total_orders"] += int(api_data.get('OrdersShopped_V2', 0))
                metrics["total_units"] += int(api_data.get('RequestedQuantity_V2', 0))
            
            app_logger.info(f"[{store_name}] Data collection complete ({duration:.2f}s).")
            return # Success, exit loop

        except Exception as e:
            app_logger.warning(f"[{store_name}] Failed attempt {attempt + 1}: {e}")
            if attempt < WORKER_RETRY_COUNT - 1:
                async with metrics_lock:
                    metrics["retries"] += 1
                    metrics["retry_stores"].add(store_name)
                sleep_time = 2 ** attempt
                app_logger.info(f"[{store_name}] Retrying in {sleep_time}s...")
                await asyncio.sleep(sleep_time)
            else:
                run_failures.append(f"{store_name} (Fail)")
                async with failure_lock:
                    failure_timestamps.append(asyncio.get_event_loop().time())
        finally:
            if page: await page.close()


#######################################################################
#                  MAIN PROCESS LOOP & ORCHESTRATION
#######################################################################

async def worker_task(worker_id: int, browser: Browser, storage_template: Dict, job_queue: Queue, submission_queue: Queue):
    global active_workers_count
    app_logger.info(f"[Worker-{worker_id}] Starting up.")
    context = None
    try:
        context = await browser.new_context(storage_state=storage_template)
        context.set_default_navigation_timeout(PAGE_TIMEOUT)
        context.set_default_timeout(ACTION_TIMEOUT)
        
        while True:
            try:
                store_item = job_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            # Enforce Concurrency Limit
            async with concurrency_condition:
                while active_workers_count >= concurrency_limit:
                    # app_logger.debug(f"[Worker-{worker_id}] Waiting for slot...")
                    await concurrency_condition.wait()
                active_workers_count += 1

            try:
                await process_single_store(context, store_item, submission_queue)
            finally:
                async with concurrency_condition:
                    active_workers_count -= 1
                    concurrency_condition.notify_all()
                job_queue.task_done()
            
    except Exception as e:
        app_logger.error(f"[Worker-{worker_id}] Crashed: {e}")
    finally:
        if context: await context.close()
        app_logger.info(f"[Worker-{worker_id}] Shutting down.")

async def process_urls():
    global progress, start_time, run_failures, browser
    # Use configured concurrency or default to 10
    pool_size = config.get('initial_concurrency', 30)
    app_logger.info(f"Job 'process_urls' started with Worker Pool size: {pool_size}")
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
    
    # Queues
    job_queue = Queue()
    submission_queue = Queue()
    
    # Populate Job Queue
    for store in urls_data:
        job_queue.put_nowait(store)
        
    with progress_lock: 
        progress = {"current": 0, "total": len(urls_data), "lastUpdate": "N/A"}
    
    start_time = datetime.now(LOCAL_TIMEZONE)

    # Start Form Submitters
    app_logger.info(f"Starting {NUM_FORM_SUBMITTERS} HTTP form submitter worker(s).")
    form_submitter_tasks = [
        asyncio.create_task(http_form_submitter_worker(submission_queue, i + 1))
        for i in range(NUM_FORM_SUBMITTERS)
    ]
    
    # Start Worker Pool
    app_logger.info(f"Spinning up {pool_size} browser workers...")
    workers = [
        asyncio.create_task(worker_task(i+1, browser, storage_template, job_queue, submission_queue))
        for i in range(pool_size)
    ]
    
    # Wait for all jobs to be processed
    await asyncio.gather(*workers)
    
    app_logger.info("All workers finished. Waiting for submission queue to empty...")
    await submission_queue.join()
    await flush_pending_chat_entries()
    
    app_logger.info("Cancelling form submitter workers...")
    for task in form_submitter_tasks: task.cancel()
    await asyncio.gather(*form_submitter_tasks, return_exceptions=True)

    elapsed = (datetime.now(LOCAL_TIMEZONE) - start_time).total_seconds()
    app_logger.info(f"Processing finished. Processed {progress['current']}/{progress['total']} in {elapsed:.2f}s")
    
    # Send Job Summary
    await post_job_summary(
        total=progress['total'],
        success=progress['current'],
        failures=run_failures,
        duration=elapsed
    )

    if run_failures:
        app_logger.warning(f"Completed with {len(run_failures)} issue(s): {', '.join(run_failures)}")
    else:
        app_logger.info("Completed successfully.")

#######################################################################
#                         MAIN EXECUTION BLOCK
#######################################################################

async def main():
    global playwright, browser
    app_logger.info("Starting up in single-run mode...")
    try:
        playwright = await async_playwright().start()
        browser = await playwright.chromium.launch(
            headless=not DEBUG_MODE,
            args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-accelerated-2d-canvas",
                "--disable-gl-drawing-for-tests",
            ]
        )
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