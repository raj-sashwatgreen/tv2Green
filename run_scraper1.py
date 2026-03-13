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
from selenium.common.exceptions import TimeoutException, WebDriverException
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
checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_week_{SHARD_INDEX}.txt")

EXPECTED_COUNT = 15 
BATCH_SIZE = 100  # Increased for efficiency
RESTART_EVERY_ROWS = 20
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

WEEK_OUTPUT_START_COL = 3 

# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        last_i = max(int(open(checkpoint_file).read().strip()), START_ROW)
    except:
        last_i = START_ROW
else:
    last_i = START_ROW

# ---------------- UTILS ---------------- #
def col_num_to_letter(n):
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result

WEEK_START_COL_LETTER = col_num_to_letter(WEEK_OUTPUT_START_COL)
WEEK_END_COL_LETTER = col_num_to_letter(WEEK_OUTPUT_START_COL + EXPECTED_COUNT - 1)

def api_retry(func, *args, **kwargs):
    """Exponential backoff for Google Sheets API calls."""
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:100]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return func(*args, **kwargs) # Final attempt

# ---------------- DRIVER ---------------- #
driver = None

def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing browser...")
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
                    drv.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            drv.refresh()
            time.sleep(2)
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

# ---------------- SCRAPER ---------------- #
def get_values(drv):
    try:
        elements = drv.find_elements(By.CSS_SELECTOR, "div[class*='valueValue']")
        return [el.text.strip() for el in elements if el.text.strip()]
    except: return []

def scrape_week(url):
    if not url: return []
    for attempt in range(2):
        try:
            drv = ensure_driver()
            drv.get(url)
            
            # Smart Wait: Wait specifically for the data container
            wait = WebDriverWait(drv, 15)
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']")))
            
            # Stability check
            time.sleep(1.5)
            vals = get_values(drv)
            
            if len(vals) < EXPECTED_COUNT:
                drv.execute_script("window.scrollTo(0, 500);")
                time.sleep(1)
                vals = get_values(drv)

            if len(vals) >= EXPECTED_COUNT:
                return vals[:EXPECTED_COUNT]
                
        except Exception as e:
            log(f"   ❌ Scrape Attempt {attempt+1} Failed: {str(e)[:50]}")
            restart_driver()
    return []

# ---------------- MAIN ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 WEEK").worksheet("Sheet1")
    return sh_main, sh_data

try:
    sheet_main, sheet_data = connect_sheets()
    # Robust read with API retry
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 8)
    log(f"✅ Ready. Processing Rows {last_i + 1} to {min(END_ROW, len(company_list))}")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}"); sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

try:
    for i in range(last_i, min(END_ROW, len(company_list))):
        name = company_list[i].strip()
        url = url_list[i].strip() if i < len(url_list) and "http" in url_list[i] else None
        
        log(f"🔍 [{i+1}] {name}")
        vals = scrape_week(url)
        
        row_idx = i + 1
        padded_vals = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
        
        # Build batch updates
        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"B{row_idx}", "values": [[current_date]]})
        batch_list.append({
            "range": f"{WEEK_START_COL_LETTER}{row_idx}:{WEEK_END_COL_LETTER}{row_idx}",
            "values": [padded_vals]
        })

        # Checkpoint every row
        with open(checkpoint_file, "w") as f: f.write(str(i + 1))
        
        if (i + 1) % RESTART_EVERY_ROWS == 0: restart_driver()

        if len(batch_list) // 3 >= BATCH_SIZE:
            log(f"🚀 Uploading batch of {BATCH_SIZE}...")
            api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
            batch_list = []

finally:
    if batch_list:
        log("🚀 Uploading final batch...")
        api_retry(sheet_data.batch_update, batch_list, value_input_option="RAW")
    restart_driver()
    log("🏁 SHARD COMPLETED.")
