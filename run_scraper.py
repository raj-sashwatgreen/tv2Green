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
    opts.page_load_strategy = "normal" # Changed to normal to ensure full JS execution
    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")
    opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    driver.set_page_load_timeout(50)

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

# ---------------- URL LOGGING SCRAPER ---------------- #
def scrape_tradingview(driver, url, url_type="", row_num=0):
    log(f"🔗 {url_type}-URL: {url}")
    
    for attempt in range(3):
        try:
            log(f"    📡 Visiting {url_type} (Attempt {attempt+1}/3)...")
            driver.get(url)
            
            # --- NUMERIC GUARD: Wait for actual numbers to replace skeleton loaders ---
            wait = WebDriverWait(driver, 30)
            try:
                # This waits until at least one element contains a digit (0-9)
                wait.until(lambda d: any(re.search(r'\d', el.text) for el in d.find_elements(By.CSS_SELECTOR, "[class*='valueValue']")))
            except TimeoutException:
                log(f"    ⏳ Data load slow for {url_type}, proceeding with current state...")

            time.sleep(5) # Final settle time
            
            soup = BeautifulSoup(driver.page_source, "html.parser")
            
            # Extraction logic (Original Paths Maintained)
            v1 = [el.get_text().strip().replace('−', '-').replace('∅', 'None') 
                  for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")]
            
            v2 = [el.get_text().strip().replace('−', '-').replace('∅', 'None') 
                  for el in soup.find_all("div", class_=lambda x: x and 'valueValue' in x)]
            
            v3_els = driver.find_elements(By.XPATH, "//div[contains(@class, 'value') and contains(@class, 'Value')]")
            v3 = [el.text.strip().replace('−', '-').replace('∅', 'None') for el in v3_els]
            
            raw_values = v1 or v2 or v3
            
            # Filter: Ensure we only keep strings that have actual data (digits)
            final_values = [v for v in raw_values if v and v != 'None' and any(char.isdigit() for char in v)]
            
            if final_values:
                log(f"    ✅ SUCCESS {url_type}: {len(final_values)} numeric values found!")
                return final_values
            else:
                log(f"    ⚠️ No numeric values found on {url_type}. Refreshing...")
                driver.refresh()
                time.sleep(3)
                
        except Exception as e:
            log(f"    ❌ Attempt {attempt+1} failed: {str(e)[:60]}")
            time.sleep(2)
    
    log(f"    ❌ {url_type} FAILED after 3 attempts")
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
driver = None
batch_list = []
BATCH_SIZE = 100 # Reduced batch size for more frequent saves
current_date = date.today().strftime("%m/%d/%Y")
ROW_SLEEP = 0.2

def flush_batch():
    global batch_list
    if not batch_list: return
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ API Error: {msg[:100]}")
            time.sleep(60 if "429" in msg else 5)

def ensure_driver():
    global driver
    if driver is None: driver = create_driver()
    return driver

def get_all_values_for_row(i):
    # D-URL
    d_driver = ensure_driver()
    url_d = (url_d_list[i] if i < len(url_d_list) else "").strip()
    vals_d = scrape_tradingview(d_driver, url_d, "D", i+1) if url_d.startswith("http") else []
    
    # H-URL (Fresh instance per row to prevent memory leaks/sticky UI)
    h_driver = create_driver()
    url_h = (url_h_list[i] if i < len(url_h_list) else "").strip()
    vals_h = scrape_tradingview(h_driver, url_h, "H", i+1) if url_h.startswith("http") else []
    h_driver.quit()
    
    return vals_d + vals_h

try:
    for i in range(last_i, len(company_list)):
        if i % SHARD_STEP != SHARD_INDEX: continue

        name = company_list[i].strip()
        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name}")

        combined_values = get_all_values_for_row(i)
        target_row = i + 1

        # Always update name and date
        batch_list.append({"range": f"A{target_row}", "values": [[name]]})
        batch_list.append({"range": f"J{target_row}", "values": [[current_date]]})
        
        if combined_values:
            batch_list.append({"range": f"K{target_row}", "values": [combined_values]})
            log(f"✅ Combined: {len(combined_values)} values | Buffer: {len(batch_list)}/{BATCH_SIZE*3}")
        
        if len(batch_list) >= (BATCH_SIZE * 3):
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        time.sleep(ROW_SLEEP)

finally:
    flush_batch()
    if driver: driver.quit()
    log("🏁 Scraping completed successfully!")
