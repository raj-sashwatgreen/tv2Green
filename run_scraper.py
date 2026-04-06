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

EXPECTED_COUNT = 26
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 20
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3  # Data starts in Column C

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)

STATUS_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT)
SHEET_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1)
BROWSER_URL_COL = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:100]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs)

# ---------------- STATE ---------------- #
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
    log(f"🌐 [DAY Shard {SHARD_INDEX}] Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--incognito")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    cookie_data = {k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")}
                    drv.add_cookie(cookie_data)
                except:
                    continue
            drv.refresh()
            time.sleep(2)
        except:
            pass
    return drv

def ensure_driver():
    global driver
    if driver is None:
        driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try:
            driver.quit()
        except:
            pass
    driver = None

# ---------------- SCRAPER ---------------- #
def get_values(drv):
    try:
        elements = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
        vals = [el.text.strip() for el in elements if el.text.strip()]
        return vals
    except Exception as e:
        log(f"   get_values error: {e}")
        return []

def scrape_day(url):
    if not url:
        return [""] * EXPECTED_COUNT, "Bad URL", "", ""

    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            wait = WebDriverWait(drv, 20)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']")))
            time.sleep(2)
            vals = get_values(drv)

            if len(vals) < EXPECTED_COUNT:
                for scroll_y in [500, 1000, 1500, 2000]:
                    drv.execute_script(f"window.scrollTo(0, {scroll_y});")
                    time.sleep(1.5)
                    new_vals = get_values(drv)
                    if len(new_vals) > len(vals):
                        vals = new_vals
                    if len(vals) >= EXPECTED_COUNT:
                        break

            browser_url = drv.current_url
            found_count = len(vals)
            if found_count >= EXPECTED_COUNT:
                log(f"   Found {found_count} values")
                return vals[:EXPECTED_COUNT], "OK", url, browser_url
            else:
                log(f"   ⚠️ Only found {found_count} values")
                padded_vals = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                return padded_vals, f"Only {found_count} Found", url, browser_url
        except Exception as e:
            log(f"   ❌ Scrape Attempt {attempt + 1} Failed: {str(e)[:50]}")
            restart_driver()
    return [""] * EXPECTED_COUNT, "Failed", url, ""

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

def process_row(i, company_list, url_list, current_date):
    name = company_list[i].strip() if i < len(company_list) else ""
    url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None
    
    log(f"🔍 [{i + 1}] {name}")
    vals, status, sheet_url_used, browser_url_used = scrape_day(url)
    
    row_idx = i + 1
    row_payload = [
        {"range": f"A{row_idx}", "values": [[name]]},
        {"range": f"B{row_idx}", "values": [[current_date]]},
        {"range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}", "values": [vals]},
        {"range": f"{STATUS_COL}{row_idx}", "values": [[status]]},
        {"range": f"{SHEET_URL_COL}{row_idx}", "values": [[sheet_url_used]]},
        {"range": f"{BROWSER_URL_COL}{row_idx}", "values": [[browser_url_used]]}
    ]
    return row_payload, (status == "OK")

try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)
    log(f"✅ Processing Rows {last_i + 1} to {min(END_ROW, len(company_list))}")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}")
    sys.exit(1)

retry_indices = []
batch_list = []
current_date = date.today().strftime("%m/%d/%Y")
loop_end = min(END_ROW, len(company_list))

# --- FIRST PASS ---
try:
    for i in range(last_i, loop_end):
        payload, success = process_row(i, company_list, url_list, current_date)
        batch_list.extend(payload)
        
        if not success:
            retry_indices.append(i)

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if (i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if len(batch_list) // 6 >= BATCH_SIZE:
            log(f"🚀 Uploading batch of {BATCH_SIZE} rows...")
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

finally:
    if batch_list:
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
        batch_list = []

# --- SECOND PASS (RETRIES) ---
if retry_indices:
    log(f"🔁 Starting Retry Pass for {len(retry_indices)} symbols...")
    restart_driver() # Fresh start for retries
    
    for idx, i in enumerate(retry_indices):
        payload, success = process_row(i, company_list, url_list, current_date)
        batch_list.extend(payload)
        
        if (idx + 1) % 10 == 0: # Restart driver more frequently on retries
            restart_driver()
            
        if len(batch_list) // 6 >= 10: # Smaller batch for retries
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

    if batch_list:
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")

restart_driver()
log("🏁 DAY SHARD COMPLETED (Including Retries).")
