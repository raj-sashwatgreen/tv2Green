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

# --- COLUMN MAPPING ---
NAME_COL, DATE_COL, DATA_START_COL, DATA_END_COL = "A", "B", "C", "V"

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

driver = None

# ---------------- DRIVER LOGIC ---------------- #
def create_driver():
    log(f"🌐 [DAY] [Shard {SHARD_INDEX}] Range {START_ROW+1}-{END_ROW} | Initializing browser...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(90)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            time.sleep(2)
            with open(COOKIE_FILE, "r") as f:
                cookies = json.load(f)
            for c in cookies:
                drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
            drv.refresh()
            log("✅ Cookies applied.")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:50]}")
    return drv

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def restart_driver():
    global driver
    if driver:
        try: driver.quit()
        except: pass
    driver = None
    time.sleep(2)

# ---------------- SCRAPING HELPERS ---------------- #
def get_values(drv):
    try:
        elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
        return [el.text.strip() for el in elems if el.is_displayed() and el.text.strip()]
    except: return []

def scrape_day(url):
    if not url: return []
    log(f"    📡 Navigating DAY: {url}")
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            WebDriverWait(drv, 20).until(lambda d: len(get_values(d)) >= EXPECTED_COUNT)
            vals = get_values(drv)
            if len(vals) >= EXPECTED_COUNT: return vals[:EXPECTED_COUNT]
        except Exception:
            restart_driver()
    return []

# ---------------- SHEETS LOGIC ---------------- #
def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    
    # FIX 1: Auto-expand grid if necessary
    required_rows = END_ROW + 10
    if sh_data.row_count < required_rows:
        log(f"Extending sheet to {required_rows} rows...")
        sh_data.add_rows(required_rows - sh_data.row_count)
        
    return sh_main, sh_data

# ---------------- MAIN EXECUTION ---------------- #
try:
    sheet_main, sheet_data = connect_sheets()
    company_list = sheet_main.col_values(1)
    url_day_list = sheet_main.col_values(4)
    log(f"✅ Ready. Starting from Row {last_i + 1}")
except Exception as e:
    log(f"❌ Connection Error: {e}")
    sys.exit(1)

batch_list = []
buffered_rows = 0
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list, buffered_rows
    if not batch_list: return True
    log(f"🚀 UPLOADING BATCH: {buffered_rows} rows...")
    
    for attempt in range(4): # Increased retries
        try:
            # FIX 2: Explicit range without string concatenation risks
            sheet_data.batch_update(batch_list, value_input_option="USER_ENTERED")
            log("✅ Batch written.")
            batch_list = []
            buffered_rows = 0
            return True
        except gspread.exceptions.APIError as e:
            code = e.response.status_code
            log(f"⚠️ API Error {code} on attempt {attempt+1}")
            if code == 429: # FIX 3: Longer backoff for Quota
                time.sleep(30 * (attempt + 1)) 
            else:
                time.sleep(10)
    return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        u_day = url_day_list[i].strip() if i < len(url_day_list) and url_day_list[i].startswith("http") else None
        
        log(f"--- [ROW {i+1}] {name} ---")
        vals_day = scrape_day(u_day)
        
        row_idx = i + 1
        # FIX 4: Simplified range construction to avoid recursion
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"B{row_idx}", "values": [[current_date]]})
        
        final_vals = vals_day if len(vals_day) == EXPECTED_COUNT else [""] * EXPECTED_COUNT
        batch_list.append({
            "range": f"{DATA_START_COL}{row_idx}:{DATA_END_COL}{row_idx}", 
            "values": [final_vals]
        })

        buffered_rows += 1
        with open(checkpoint_file, "w") as f: f.write(str(row_idx))

        if buffered_rows >= BATCH_SIZE:
            if not flush_batch(): break
            restart_driver()

        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

finally:
    flush_batch()
    restart_driver()
    log("🏁 DAY Shard Completed.")
