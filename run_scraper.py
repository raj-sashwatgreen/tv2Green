import sys
import os
import time
import json
import random
from datetime import date
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
    last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
else:
    last_i = START_ROW

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing...")
    opts = Options()

    # Faster than "normal" because it doesn't wait for every resource
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(60)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(2)

            with open("cookies.json", "r") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    driver.add_cookie({
                        k: v for k, v in c.items()
                        if k in ("name", "value", "path", "secure", "expiry")
                    })
                except:
                    continue

            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied.")
        except:
            log("⚠️ Cookie error.")
    return driver

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 Navigating {url_type}: {url}")

    for attempt in range(2):
        try:
            driver.get(url)

            # Wait only until DOM is usable / values start appearing
            try:
                WebDriverWait(driver, 15).until(
                    lambda d: (
                        len(d.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")) > 0
                        or d.execute_script("return document.readyState") in ["interactive", "complete"]
                    )
                )
            except:
                pass

            # Very small scroll trigger for lazy-rendered content
            driver.execute_script("window.scrollTo(0, 250);")
            time.sleep(0.8)
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(3)

            # First try selenium elements directly (usually faster)
            elems = driver.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
            final_values = [el.text.strip() for el in elems if el.text and el.text.strip()]

            # Fallback to BeautifulSoup only if needed
            if not final_values:
                soup = BeautifulSoup(driver.page_source, "html.parser")
                raw_values = soup.find_all("div", class_=lambda x: x and "valueValue" in x)
                final_values = [el.get_text(strip=True) for el in raw_values if el.get_text(strip=True)]

            if final_values:
                log(f"   📊 Found {len(final_values)} values for {url_type}")
                log(f"   📝 Data Preview: {final_values[:8]}...")
                return final_values
            else:
                log(f"   ⚠️ {url_type} No values found. Refreshing...")
                driver.refresh()
                time.sleep(3)

        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:50]}")
            time.sleep(2)

    return []

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)

    log(f"✅ Data Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
BATCH_SIZE = 100
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list:
        return

    log(f"🚀 UPLOADING BATCH: Sending {len(batch_list)//3} rows to Google Sheets...")

    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log("✅ SUCCESS: Batch successfully written to Sheet2.")
            batch_list = []
            return
        except Exception as e:
            log(f"⚠️ API Retry {attempt+1}: {str(e)[:80]}")
            time.sleep(8 + (attempt * 5))

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

try:
    for i in range(last_i, min(END_ROW, len(company_list))):
        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] Processing: {name} ---")

        active_driver = ensure_driver()

        u_d = url_d_list[i] if i < len(url_d_list) and url_d_list[i].startswith("http") else None
        u_h = url_h_list[i] if i < len(url_h_list) and url_h_list[i].startswith("http") else None

        vals_d = scrape_tradingview(active_driver, u_d, "DAILY") if u_d else []
        vals_h = scrape_tradingview(active_driver, u_h, "HOURLY") if u_h else []
        combined = vals_d + vals_h

        row_idx = i + 1
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"J{row_idx}", "values": [[current_date]]})
        if combined:
            batch_list.append({"range": f"K{row_idx}", "values": [combined]})
            log(f"   📥 Buffered {len(combined)} values into batch.")

        progress = len(batch_list) // 3
        log(f"📈 Shard Progress: {i+1}/{END_ROW} | Batch Buffer: {progress}/{BATCH_SIZE}")

        if progress >= BATCH_SIZE:
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(0.4)

finally:
    if batch_list:
        flush_batch()
    if driver:
        driver.quit()
    log("🏁 Shard Completed.")
