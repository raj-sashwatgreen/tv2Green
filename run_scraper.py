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
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from bs4 import BeautifulSoup
import gspread
from webdriver_manager.chrome import ChromeDriverManager

def log(msg):
    print(msg, flush=True)

# ---------------- CONFIG & SHARDING ---------------- #
SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_STEP  = int(os.getenv("SHARD_STEP", "1"))

checkpoint_file = os.getenv("CHECKPOINT_FILE", f"checkpoint_{SHARD_INDEX}.txt")
last_i = int(open(checkpoint_file).read().strip()) if os.path.exists(checkpoint_file) else 0

CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()
    opts.page_load_strategy = "normal"  # Ensuring full JS execution
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--disable-notifications")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(60)

    if os.path.exists("cookies.json"):
        try:
            driver.get("https://in.tradingview.com/")
            time.sleep(3)
            with open("cookies.json", "r") as f:
                cookies = json.load(f)
            for c in cookies:
                try:
                    driver.add_cookie({k: v for k, v in c.items() if k in ("name", "value", "path", "secure", "expiry")})
                except:
                    continue
            driver.refresh()
            time.sleep(2)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver

# ---------------- SCRAPER ENGINE ---------------- #
def scrape_tradingview(driver, url, url_type=""):
    log(f"🔗 {url_type}-URL: {url}")
    
    for attempt in range(3):
        try:
            driver.get(url)
            
            # --- THE FIX: WAIT FOR DIGITS ---
            # This waits up to 25 seconds for an element with a number (0-9) to appear
            wait = WebDriverWait(driver, 25)
            try:
                wait.until(lambda d: any(re.search(r'\d', el.text) for el in d.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")))
            except TimeoutException:
                log(f"   ⏳ Data delay on {url_type}... attempting extraction anyway.")

            time.sleep(3) # Settle time
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            raw_elements = soup.find_all("div", class_=lambda x: x and 'valueValue' in x)
            
            values = []
            for el in raw_elements:
                txt = el.get_text(strip=True).replace('−', '-').replace('∅', 'None')
                # If we catch a placeholder, we ignore it
                if txt and txt != 'None' and any(char.isdigit() for char in txt):
                    values.append(txt)
            
            if values:
                log(f"   ✅ SUCCESS {url_type}: {len(values)} values found!")
                return values
            else:
                log(f"   ⚠️ Attempt {attempt+1}: Only None/Empty values found. Retrying...")
                driver.refresh()
                time.sleep(5)
                
        except Exception as e:
            log(f"   ❌ Attempt {attempt+1} failed: {str(e)[:60]}")
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

    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver_d_global = None
batch_list = []
BATCH_SIZE = 100 
current_date = date.today().strftime("%m/%d/%Y")

def flush_batch():
    global batch_list
    if not batch_list: return
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates to Sheet2")
            batch_list = []
            return
        except Exception as e:
            log(f"⚠️ API Error: {str(e)[:100]}")
            time.sleep(60 if "429" in str(e) else 5)

def get_row_data(i):
    global driver_d_global
    if driver_d_global is None: driver_d_global = create_driver()
    
    # Process D-URL
    url_d = (url_d_list[i] if i < len(url_d_list) else "").strip()
    val_d = scrape_tradingview(driver_d_global, url_d, "D") if url_d.startswith("http") else []
    
    # Process H-URL (Fresh driver to avoid state conflicts)
    temp_h_driver = create_driver()
    url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
    val_h = scrape_tradingview(temp_h_driver, url_h, "H") if url_h.startswith("http") else []
    temp_h_driver.quit()
    
    return val_d + val_h

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        name = company_list[i].strip()
        log(f"🔍 [{i+1}/{len(company_list)}] Processing: {name}")

        final_values = get_row_data(i)
        target_row = i + 1

        # Prepare Batch
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
        
        if final_values:
            batch_list.append({"range": f"K{target_row}", "values": [final_values]})
        
        if len(batch_list) >= BATCH_SIZE:
            flush_batch()

        # Update Checkpoint
        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))
            
finally:
    flush_batch()
    if driver_d_global: driver_d_global.quit()
    log("🏁 Process Finished.")
