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
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "25")) 

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log(f"🌐 [Shard {SHARD_INDEX}] Initializing Chrome...")
    opts = Options()
    opts.page_load_strategy = "normal" 
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(90) # Increased timeout for slow connections

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(5)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except: continue
            driver.refresh()
            time.sleep(5)
            log("✅ Cookies applied.")
        except: 
            log("⚠️ Cookie error.")
    return driver

# ---------------- SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"   📡 Navigating to {url_type}: {url}")
    for attempt in range(3):
        try:
            driver.get(url)
            
            # 1. HARD WAIT FOR UI ELEMENTS (Wait up to 60s)
            try:
                WebDriverWait(driver, 60).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "[class*='valueValue']"))
                )
            except TimeoutException:
                log(f"   ⏳ Elements didn't appear quickly on {url_type}, trying to force load...")

            # 2. SCROLL DOWN/UP to trigger lazy-loading elements
            driver.execute_script("window.scrollTo(0, 400);")
            time.sleep(2)
            driver.execute_script("window.scrollTo(0, 0);")

            # 3. INCREASED JS RENDER WAIT (25 Seconds)
            # This ensures complex technical indicators have time to calculate and display
            time.sleep(25) 
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            v1 = [el.get_text().strip() for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            v2 = [el.get_text().strip() for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            v3 = [el.text.strip() for el in driver.find_elements(By.XPATH, "//div[contains(@class, 'value') and contains(@class, 'Value')]")]
            
            raw_values = v1 or v2 or v3
            final_values = [str(v) for v in raw_values if v is not None]
            
            if final_values:
                preview = ", ".join(final_values[:5])
                log(f"   ✅ {url_type} SUCCESS: Found {len(final_values)} values [{preview}..]")
                return final_values
            else:
                log(f"   ⚠️ {url_type} EMPTY (Attempt {attempt+1}/3). Retrying...")
                driver.refresh()
                time.sleep(10) # Wait longer after refresh
        except Exception as e:
            log(f"   ❌ {url_type} ERROR: {str(e)[:80]}")
            time.sleep(5)
    return []

# ---------------- ANTI-COLLISION STARTUP ---------------- #
startup_wait = random.uniform(5, 60)
log(f"⏳ Startup Jitter: Waiting {startup_wait:.1f}s...")
time.sleep(startup_wait)

# ---------------- SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")
    
    company_list = sheet_main.col_values(1)
    url_d_list = sheet_main.col_values(4)
    url_h_list = sheet_main.col_values(8)
    log(f"✅ Data loaded: {len(company_list)} tickers.")
except Exception as e:
    log(f"❌ Connection Error: {e}"); sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = None
batch_list = []
BATCH_SIZE = 100 
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    jitter = random.uniform(5, 15)
    log(f"🚀 [Shard {SHARD_INDEX}] BUFFER FULL. Jittering {jitter:.1f}s before saving...")
    time.sleep(jitter) 
    for attempt in range(5):
        try:
            sheet_data.batch_update(batch_list, value_input_option='RAW')
            log(f"✅ [Shard {SHARD_INDEX}] Batch Written Successfully.")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            wait = 60 if "429" in msg else 15
            log(f"⚠️ API Error: {msg[:50]}. Retrying in {wait}s...")
            time.sleep(wait + random.uniform(2, 8))

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        name = company_list[i].strip()
        log(f"--- [ROW {i+1}] {name} ---")

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
        
        # Buffer progress
        cur_len = len(batch_list) // 3
        log(f"📊 Buffer Status: {cur_len}/{BATCH_SIZE}")

        if len(batch_list) >= (BATCH_SIZE * 3):
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
        
        time.sleep(2)

finally:
    if batch_list:
        flush_batch()
    if driver: 
        driver.quit()
    log("🏁 Shard Completed.")
