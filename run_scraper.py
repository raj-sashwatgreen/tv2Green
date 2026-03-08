import sys
import os
import time
import json
import re
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)

# ---------------- CONFIG & RANGE CALCULATION ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE  = int(os.getenv("SHARD_SIZE", "500"))
START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW   = START_ROW + SHARD_SIZE
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")

if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- SETTINGS ---------------- #
BATCH_SIZE = 50
RESTART_EVERY_ROWS = 15
EXPECTED_COUNTS = {"DAILY": 20, "HOURLY": 12}

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing browser...")
    opts = Options()
    opts.page_load_strategy = "normal"
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)

    if os.path.exists("cookies.json"):
        try:
            drv.get("https://in.tradingview.com/")
            time.sleep(2)
            with open("cookies.json", "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            drv.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")
    return drv

driver = None
def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Closing browser...")
            driver.quit()
    except: pass
    driver = None
    time.sleep(3)

# ---------------- SCRAPER HELPERS ---------------- #
def wait_for_page_ready(drv, timeout=25):
    WebDriverWait(drv, timeout).until(lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"])

def get_clean_values_js(drv):
    """Uses JS to find indicator values specifically, ignoring volume/price noise."""
    script = """
    return Array.from(document.querySelectorAll("div[data-name='legend-item-value']"))
        .map(el => el.innerText.trim())
        .filter(txt => {
            // Filter out empty, non-numeric noise, and percentage/volume strings
            if (!txt || txt === '∅' || txt.includes('%') || /[KMB]$/i.test(txt)) return false;
            // Ensure it contains at least one digit
            return /\\d/.test(txt);
        });
    """
    try:
        return drv.execute_script(script)
    except:
        return []

def validate_values(values, url_type):
    if not values: return False
    expected = EXPECTED_COUNTS.get(url_type)
    if len(values) != expected: return False
    # Check if first value is roughly numeric
    try:
        float(values[0].replace(',', ''))
        return True
    except:
        return False

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url, url_type=""):
    if not url: return []
    expected = EXPECTED_COUNTS.get(url_type, "unknown")
    log(f"    📡 Navigating {url_type}: {url}")

    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)
            wait_for_page_ready(drv, timeout=25)

            # Scroll to trigger lazy-loading of legend
            drv.execute_script("window.scrollTo(0, 200);")
            time.sleep(2)

            # Wait for data to stabilize
            final_values = []
            for _ in range(5):
                vals = get_clean_values_js(drv)
                if len(vals) >= expected:
                    final_values = vals
                    break
                time.sleep(1.5)

            if validate_values(final_values, url_type):
                log(f"    📊 Found {len(final_values)} clean values for {url_type}")
                return final_values
            
            log(f"    ⚠️ Attempt {attempt+1} failed validation. Count: {len(final_values)}")
            drv.refresh()
            time.sleep(5)
        except Exception as e:
            log(f"    ❌ ERROR: {str(e)[:100]}")
            restart_driver()
    return []

# ---------------- SHEETS SETUP ---------------- #
def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    return sheet_main, sheet_data

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)
    log(f"✅ Data Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
batch_list, buffered_rows = [], 0
current_date = date.today().strftime("%m/%d/%Y")
prev_daily, prev_hourly, same_count = None, None, 0

def flush_batch():
    global batch_list, buffered_rows, sheet_data
    if not batch_list: return True
    log(f"🚀 UPLOADING BATCH: {buffered_rows} rows...")
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            batch_list, buffered_rows = [], 0
            return True
        except:
            time.sleep(10)
            try: _, sheet_data = connect_sheets()
            except: pass
    return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] {name} ---")
        u_d = url_d_list[i].strip() if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i].strip() if i < len(url_h_list) and url_h_list[i].startswith("http") else None

        vals_d = scrape_tradingview(u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(u_h, "HOURLY") if u_h else []

        if (vals_d == prev_daily and vals_h == prev_hourly) and (vals_d or vals_h):
            same_count += 1
            if same_count >= 2:
                restart_driver()
                same_count = 0
        else: same_count = 0

        prev_daily, prev_hourly = (vals_d.copy(), vals_h.copy())
        combined = vals_d + vals_h
        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [combined] if combined else [[]]})
        buffered_rows += 1

        with open(checkpoint_file, "w") as f: f.write(str(i + 1))
        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()
        if buffered_rows >= BATCH_SIZE: 
            flush_batch()
            restart_driver()
            sheet_main, sheet_data = connect_sheets()
finally:
    if batch_list: flush_batch()
    restart_driver()
