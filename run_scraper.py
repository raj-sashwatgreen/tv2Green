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
RESTART_EVERY_ROWS = 20

# Column Mapping
NAME_COL, DATE_COL, DATA_START_COL, DATA_END_COL = "A", "B", "C", "V"

COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- PERSISTENCE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- ROBUST API WRAPPER ---------------- #
def robust_api_call(func, *args, **kwargs):
    """Handles Read/Write errors and Quota limits with exponential backoff."""
    for attempt in range(6):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait_time = (2 ** attempt) + random.random()
            log(f"⚠️ API Error: {str(e)[:100]}. Retrying in {wait_time:.1f}s...")
            time.sleep(wait_time)
    return func(*args, **kwargs)

# ---------------- DRIVER LOGIC ---------------- #
driver = None

def create_driver():
    log(f"🌐 [DAY] Initializing session {START_ROW+1}-{END_ROW}...")
    opts = Options()
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            drv.refresh()
            time.sleep(1)
        except: pass
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

# ---------------- SCRAPING ---------------- #
def get_values(drv):
    try:
        elems = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
        return [el.text.strip() for el in elems if el.is_displayed() and el.text.strip()]
    except: return []

def scrape_day(url):
    if not url: return []
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            # Wait for any value element to appear
            WebDriverWait(drv, 15).until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']")))
            
            # Allow stabilization
            time.sleep(1.5)
            vals = get_values(drv)
            
            if len(vals) < EXPECTED_COUNT:
                drv.execute_script("window.scrollTo(0, 400);")
                time.sleep(1)
                vals = get_values(drv)

            if len(vals) >= EXPECTED_COUNT:
                extracted = vals[:EXPECTED_COUNT]
                log(f"   📊 Extracted {len(extracted)} values: {extracted}")
                return extracted
        except Exception as e:
            log(f"   ⚠️ Scrape attempt {attempt+1} failed: {str(e)[:50]}")
            restart_driver()
    return []

# ---------------- SHEETS LOGIC ---------------- #
def connect_sheets():
    log("📊 Connecting to Google Sheets...")
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    
    # Check if sheet is large enough
    if sh_data.row_count < END_ROW:
        log(f"⚙️ Expanding sheet to {END_ROW} rows...")
        robust_api_call(sh_data.add_rows, END_ROW - sh_data.row_count)
        
    return sh_main, sh_data

# ---------------- MAIN EXECUTION ---------------- #
try:
    sheet_main, sheet_data = connect_sheets()
    # Using robust wrapper for initial data reads
    company_list = robust_api_call(sheet_main.col_values, 1)
    url_day_list = robust_api_call(sheet_main.col_values, 4)
    log(f"✅ Data Loaded. Starting from index {last_i}")
except Exception as e:
    log(f"❌ Initialization Failed: {e}"); sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return True
    log(f"🚀 UPLOADING: Sending {len(batch_list)//3} rows to MV2 DAY...")
    success = robust_api_call(sheet_data.batch_update, batch_list, value_input_option="USER_ENTERED")
    if success:
        batch_list = []
        return True
    return False

try:
    loop_end = min(END_ROW, len(company_list))
    for i in range(last_i, loop_end):
        name = company_list[i].strip()
        u_day = url_day_list[i].strip() if i < len(url_day_list) and "http" in url_day_list[i] else None
        
        log(f"--- [ROW {i+1}/{loop_end}] Processing: {name} ---")
        vals_day = scrape_day(u_day)
        
        row_idx = i + 1
        final_vals = (vals_day + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
        
        # Buffer the updates
        batch_list.append({"range": f"{NAME_COL}{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"{DATE_COL}{row_idx}", "values": [[current_date]]})
        batch_list.append({
            "range": f"{DATA_START_COL}{row_idx}:{DATA_END_COL}{row_idx}", 
            "values": [final_vals]
        })

        # Save checkpoint
        with open(checkpoint_file, "w") as f: f.write(str(row_idx))

        if len(batch_list) // 3 >= BATCH_SIZE:
            flush_batch()
            restart_driver()

        if (i - last_i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

finally:
    if batch_list:
        flush_batch()
    restart_driver()
    log("🏁 DAY Shard Completed.")
