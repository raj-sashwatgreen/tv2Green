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


# ---------------- CONFIG ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))

START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_day_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 20
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 15

# --- COLUMN MAPPING (Removed Extra Gaps) ---
NAME_COL = "A"
DATE_COL = "B"
DATA_START_COL = "C"
DATA_END_COL = "V" # Column C + 20 columns = Column V

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW


# ---------------- DRIVER ---------------- #
driver = None


def create_driver():
    log(f"🌐 [DAY] [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing browser...")
    opts = Options()
    opts.page_load_strategy = "normal"
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            time.sleep(2)
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
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
            log(f"⚠️ Cookie error: {str(e)[:100]}")
    return drv


def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver


def restart_driver():
    global driver
    try:
        if driver:
            log("♻️ Closing DAY browser...")
            driver.quit()
    except Exception as e:
        log(f"⚠️ Browser close issue: {str(e)[:100]}")
    driver = None
    time.sleep(3)


# ---------------- HELPERS ---------------- #
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


def looks_like_polluted_day(values):
    if not values:
        return True
    joined = " | ".join(values[:8])
    suspicious_tokens = ["%", "K", "M", "B", "∅"]
    suspicious_score = sum(1 for tok in suspicious_tokens if tok in joined)
    return suspicious_score >= 2


def validate_day(values):
    if not values:
        return False
    if len(values) != EXPECTED_COUNT:
        return False
    if looks_like_polluted_day(values):
        return False
    return True


# ---------------- SCRAPER ---------------- #
def scrape_day(url):
    if not url:
        return []

    log(f"    📡 Navigating DAY: {url}")

    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)
            wait_for_page_ready(drv, timeout=25)
            try:
                WebDriverWait(drv, 20).until(
                    lambda d: len(get_visible_value_elements(d)) >= EXPECTED_COUNT
                )
            except TimeoutException:
                log("    ⚠️ DAY initial wait timeout, trying anyway...")

            drv.execute_script("window.scrollTo(0, 300);")
            time.sleep(1)
            drv.execute_script("window.scrollTo(0, 0);")
            time.sleep(2)

            values = stable_read_values(drv, pause=1.2)

            if not validate_day(values):
                log(f"    ⚠️ DAY invalid data on attempt {attempt+1}: count={len(values)}")
                try:
                    drv.refresh()
                    time.sleep(4)
                    wait_for_page_ready(drv, timeout=20)
                    values = stable_read_values(drv, pause=1.2)
                except Exception as e:
                    log(f"    ⚠️ DAY refresh issue: {str(e)[:100]}")

            if not validate_day(values):
                values = bs4_fallback_values(drv)

            if validate_day(values):
                log(f"    📊 Found {len(values)} correct DAY values")
                return values

            log(f"    ⚠️ DAY invalid data on attempt {attempt+1}. Expected {EXPECTED_COUNT}, got {len(values)}")

        except Exception as e:
            log(f"    ❌ DAY ERROR: {str(e)[:120]}")
            restart_driver()
            time.sleep(3)

    return []


# ---------------- SHEETS ---------------- #
def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sheet_main, sheet_data


try:
    sheet_main, sheet_data = connect_sheets()
    company_list = sheet_main.col_values(1)
    url_day_list = sheet_main.col_values(4)   # D column
    log(f"✅ DAY Data Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)


# ---------------- BATCH ---------------- #
batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")
prev_day = None
same_count = 0


def flush_batch():
    global batch_list, buffered_rows, sheet_data

    if not batch_list:
        return True

    log(f"🚀 UPLOADING DAY BATCH: Sending {buffered_rows} rows...")

    for attempt in range(3):
        try:
            # USER_ENTERED handles numbers and dates better than RAW
            sheet_data.batch_update(batch_list, value_input_option="USER_ENTERED")
            log("✅ DAY batch written successfully.")
            batch_list = []
            buffered_rows = 0
            return True
        except Exception as e:
            log(f"⚠️ DAY API Retry {attempt+1}: {str(e)[:120]}")
            time.sleep(8 + (attempt * 5))
            try:
                _, sheet_data = connect_sheets()
            except:
                pass

    return False


# ---------------- MAIN LOOP ---------------- #
try:
    loop_end = min(END_ROW, len(company_list))

    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] DAY Processing: {name} ---")

        u_day = url_day_list[i].strip() if i < len(url_day_list) and url_day_list[i].startswith("http") else None
        vals_day = scrape_day(u_day) if u_day else []

        if vals_day == prev_day and vals_day:
            same_count += 1
            if same_count >= 2:
                restart_driver()
                vals_day = scrape_day(u_day) if u_day else []
                same_count = 0
        else:
            same_count = 0

        prev_day = vals_day.copy() if vals_day else []

        row_idx = i + 1
        
        # Mapping to Columns A, B, and C-V
        batch_list.append({"range": f"{NAME_COL}{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"{DATE_COL}{row_idx}", "values": [[current_date]]})
        
        # Ensure we always send 20 items to prevent #N/A or jagged rows
        final_vals = vals_day if len(vals_day) == EXPECTED_COUNT else [""] * EXPECTED_COUNT
        batch_list.append({"range": f"{DATA_START_COL}{row_idx}:{DATA_END_COL}{row_idx}", "values": [final_vals]})

        buffered_rows += 1

        if len(vals_day) != EXPECTED_COUNT:
            log(f"    ⚠️ DAY count mismatch: {len(vals_day)}. Filled with empty values.")

        log(f"📈 DAY Progress: {i+1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        processed = (i - last_i + 1)
        if processed % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if buffered_rows >= BATCH_SIZE:
            ok = flush_batch()
            restart_driver()
            sheet_main, sheet_data = connect_sheets()
            if not ok: break

        time.sleep(0.5)

finally:
    if batch_list:
        flush_batch()
    restart_driver()
    log("🏁 DAY Shard Completed.")
