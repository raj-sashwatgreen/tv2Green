import sys
import os
import time
import json
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

EXPECTED_COUNTS = {
    "DAILY": 20,
    "HOURLY": 12
}

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing fresh browser...")
    opts = Options()
    opts.page_load_strategy = "normal"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

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
                    drv.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue

            drv.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return drv

# ---------------- DRIVER HELPERS ---------------- #
driver = None

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Closing browser...")
            driver.quit()
    except Exception as e:
        log(f"⚠️ Browser close issue: {str(e)[:80]}")
    driver = None
    time.sleep(3)

# ---------------- SCRAPER HELPERS ---------------- #
def wait_for_page_ready(drv, timeout=25):
    WebDriverWait(drv, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ["interactive", "complete"]
    )

def get_visible_value_elements(drv):
    elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
    values = []

    for el in elems:
        try:
            if not el.is_displayed():
                continue
            txt = el.text.strip()
            if txt:
                values.append(txt)
        except:
            pass

    return values

def stable_read_values(drv, pause=1.2):
    first = get_visible_value_elements(drv)
    time.sleep(pause)
    second = get_visible_value_elements(drv)

    if first == second and first:
        return first

    return second if len(second) >= len(first) else first

def bs4_fallback_values(drv):
    try:
        soup = BeautifulSoup(drv.page_source, "html.parser")
        raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
        out = []
        for el in raw_values:
            txt = el.get_text(strip=True)
            if txt:
                out.append(txt)
        return out
    except:
        return []

def has_bad_count(values, url_type):
    expected = EXPECTED_COUNTS.get(url_type)
    return len(values) != expected

def looks_like_polluted_values(values, url_type):
    if not values:
        return True

    # DAILY correct values usually should not begin with raw OHLC/header style values
    # Example polluted: 16.85, 16.91, 15.82, 16.02, ∅, -0.52(-3.14%), 110.49K
    joined = " | ".join(values[:8])

    suspicious_tokens = ["%", "K", "M", "B", "∅"]
    suspicious_score = sum(1 for tok in suspicious_tokens if tok in joined)

    # only use this as extra safety, not as main rule
    if suspicious_score >= 2:
        return True

    return False

def validate_values(values, url_type):
    if not values:
        return False

    if has_bad_count(values, url_type):
        return False

    # Extra protection mainly for polluted daily pages
    if url_type == "DAILY" and looks_like_polluted_values(values, url_type):
        return False

    return True

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url, url_type=""):
    if not url:
        return []

    expected = EXPECTED_COUNTS.get(url_type, "unknown")
    log(f"   📡 Navigating {url_type}: {url}")

    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)

            wait_for_page_ready(drv, timeout=25)

            try:
                WebDriverWait(drv, 20).until(
                    lambda d: len(get_visible_value_elements(d)) >= expected
                )
            except TimeoutException:
                log(f"   ⚠️ {url_type} initial wait timeout, trying anyway...")

            drv.execute_script("window.scrollTo(0, 300);")
            time.sleep(1)
            drv.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            final_values = stable_read_values(drv, pause=1.2)

            if not validate_values(final_values, url_type):
                log(f"   ⚠️ {url_type} invalid count or polluted data on attempt {attempt+1}: count={len(final_values)} preview={final_values[:8]}")
                try:
                    drv.refresh()
                    time.sleep(4)
                    wait_for_page_ready(drv, timeout=20)
                    final_values = stable_read_values(drv, pause=1.2)
                except Exception as re:
                    log(f"   ⚠️ Refresh issue for {url_type}: {str(re)[:80]}")

            if not validate_values(final_values, url_type):
                final_values = bs4_fallback_values(drv)

            if validate_values(final_values, url_type):
                log(f"   📊 Found {len(final_values)} correct values for {url_type}")
                log(f"   📝 Data Preview: {final_values[:8]}...")
                return final_values

            log(f"   ⚠️ {url_type} invalid data on attempt {attempt+1}. Expected exactly {expected}, got {len(final_values)}")

        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:120]}")
            restart_driver()
            time.sleep(3)

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
batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")

prev_daily = None
prev_hourly = None
same_count = 0

def flush_batch():
    global batch_list, buffered_rows, sheet_data

    if not batch_list:
        return True

    log(f"🚀 UPLOADING BATCH: Sending {buffered_rows} rows to Google Sheets...")

    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log("✅ SUCCESS: Batch successfully written to Sheet2.")
            batch_list = []
            buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ API Retry {attempt+1}: {str(e)[:120]}")
            time.sleep(8 + (attempt * 5))
            try:
                _, sheet_data = connect_sheets()
            except Exception as inner_e:
                log(f"⚠️ Reconnect failed: {str(inner_e)[:120]}")

    return False

try:
    loop_end = min(END_ROW, len(company_list))

    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] Processing: {name} ---")

        u_d = url_d_list[i].strip() if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i].strip() if i < len(url_h_list) and url_h_list[i].startswith("http") else None

        vals_d = scrape_tradingview(u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(u_h, "HOURLY") if u_h else []

        # repeated stale data protection
        if vals_d == prev_daily and vals_h == prev_hourly and (vals_d or vals_h):
            same_count += 1
            log(f"   ⚠️ Same data repeated for consecutive symbol. Count={same_count}")

            if same_count >= 2:
                log("   ♻️ Repeated same values detected. Restarting browser and retrying...")
                restart_driver()
                vals_d = scrape_tradingview(u_d, "DAILY") if u_d else []
                vals_h = scrape_tradingview(u_h, "HOURLY") if u_h else []
                same_count = 0
        else:
            same_count = 0

        prev_daily = vals_d.copy() if vals_d else []
        prev_hourly = vals_h.copy() if vals_h else []

        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [combined] if combined else [[]]})

        buffered_rows += 1

        if len(vals_d) == 20 and len(vals_h) == 12:
            log("   ✅ Correct count: DAILY=20, HOURLY=12")
        else:
            log(f"   ⚠️ Count mismatch saved: DAILY={len(vals_d)}, HOURLY={len(vals_h)}")

        if combined:
            log(f"   📥 Buffered {len(combined)} values into batch.")
        else:
            log("   ⚠️ No values found. Blank K cell buffered.")

        log(f"📈 Shard Progress: {i+1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        processed_in_this_run = (i - last_i + 1)
        if processed_in_this_run % RESTART_EVERY_ROWS == 0:
            log(f"♻️ Periodic browser restart after {processed_in_this_run} processed rows.")
            restart_driver()

        if buffered_rows >= BATCH_SIZE:
            ok = flush_batch()

            restart_driver()
            try:
                sheet_main, sheet_data = connect_sheets()
                company_list = sheet_main.col_values(1)
                url_d_list = sheet_main.col_values(4)
                url_h_list = sheet_main.col_values(8)
                log("✅ Fresh start ready for next batch.")
            except Exception as e:
                log(f"❌ Failed to reconnect sheets after batch: {e}")
                break

            if not ok:
                log("❌ Batch upload failed after retries.")
                break

        time.sleep(0.5)

finally:
    if batch_list:
        flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
