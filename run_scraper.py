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

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing fresh browser...")
    opts = Options()
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

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
            log("♻️ Closing browser after batch...")
            driver.quit()
    except Exception as e:
        log(f"⚠️ Browser close issue: {str(e)[:80]}")
    driver = None
    time.sleep(3)

# ---------------- TARGETED EXTRACTION ---------------- #
def extract_from_target_panel(drv):
    selectors = [
        "div[data-name='legend-source-item'] div[class*='valueValue']",
        "div[class*='legend'] div[class*='valueValue']",
        "div[class*='valuesWrapper'] div[class*='valueValue']",
        "div[class*='container'] div[class*='valueValue']",
    ]

    for sel in selectors:
        try:
            elems = drv.find_elements(By.CSS_SELECTOR, sel)
            vals = []
            for el in elems:
                try:
                    if el.is_displayed():
                        txt = el.text.strip()
                        if txt:
                            vals.append(txt)
                except:
                    pass
            if vals:
                return vals
        except:
            pass

    return []

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(url, url_type=""):
    if not url:
        return []

    log(f"   📡 Navigating {url_type}: {url}")

    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)

            try:
                WebDriverWait(drv, 15).until(
                    lambda d: len(d.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")) > 0
                )
            except:
                pass

            drv.execute_script("window.scrollTo(0, 250);")
            time.sleep(0.8)
            drv.execute_script("window.scrollTo(0, 0);")
            time.sleep(3)

            # only necessary change: read from target panel first
            final_values = extract_from_target_panel(drv)

            if not final_values:
                soup = BeautifulSoup(drv.page_source, "html.parser")
                raw_values = soup.select("div[data-name='legend-source-item'] div[class*='valueValue']")
                final_values = [el.get_text(strip=True) for el in raw_values if el.get_text(strip=True)]

            if final_values:
                log(f"   📊 Found {len(final_values)} values for {url_type}")
                log(f"   📝 Data Preview: {final_values[:8]}...")
                return final_values

            log(f"   ⚠️ {url_type} No values found. Refreshing...")
            try:
                drv.refresh()
            except:
                restart_driver()
            time.sleep(3)

        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:100]}")
            restart_driver()
            time.sleep(2)

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
BATCH_SIZE = 50
current_date = date.today().strftime("%m/%d/%Y")

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
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        batch_list.append({"range": f"K{row_idx}", "values": [combined] if combined else [[]]})

        buffered_rows += 1

        if combined:
            log(f"   📥 Buffered {len(combined)} values into batch.")
        else:
            log("   ⚠️ No values found. Blank K cell buffered.")

        log(f"📈 Shard Progress: {i+1}/{loop_end} | Batch Buffer: {buffered_rows}/{BATCH_SIZE}")

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

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

        time.sleep(0.4)

finally:
    if batch_list:
        flush_batch()
    restart_driver()
    log("🏁 Shard Completed.")
