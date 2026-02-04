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
last_i = int(open(checkpoint_file).read()) if os.path.exists(checkpoint_file) else 0

# ✅ Speed: resolve chromedriver path ONCE (restart will reuse it)
CHROME_DRIVER_PATH = ChromeDriverManager().install()

# ---------------- BROWSER FACTORY ---------------- #
def create_driver():
    log("🌐 Initializing Hardened Chrome Instance...")
    opts = Options()

    # ✅ Faster: don't wait for full page assets
    # (does not change your extractor)
    opts.page_load_strategy = "eager"

    opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_experimental_option("excludeSwitches", ["enable-logging"])

    # ✅ small extra speed/consistency flags (no extractor change)
    opts.add_argument("--disable-notifications")
    opts.add_argument("--disable-popup-blocking")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--disable-background-networking")
    opts.add_argument("--disable-background-timer-throttling")
    opts.add_argument("--disable-renderer-backgrounding")
    opts.add_argument("--mute-audio")

    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    driver = webdriver.Chrome(
        service=Service(CHROME_DRIVER_PATH),
        options=opts
    )
    driver.set_page_load_timeout(40)

    # ---- COOKIE LOGIC ----
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
            time.sleep(1)
            log("✅ Cookies applied successfully")
        except Exception as e:
            log(f"⚠️ Cookie error: {str(e)[:80]}")

    return driver

# ---------------- SCRAPER LOGIC ---------------- #
# ❌ NOT CHANGED (as you asked)
def scrape_tradingview(driver, url):
    try:
        driver.get(url)
        WebDriverWait(driver, 45).until(
            EC.visibility_of_element_located((
                By.XPATH,
                '/html/body/div[2]/div/div[5]/div/div[1]/div/div[2]/div[1]/div[2]/div/div[1]/div[2]/div[2]/div[2]/div[2]/div'
            ))
        )
        soup = BeautifulSoup(driver.page_source, "html.parser")
        values = [
            el.get_text().replace('−', '-').replace('∅', 'None')
            for el in soup.find_all("div", class_="valueValue-l31H9iuA apply-common-tooltip")
        ]
        return values
    except (TimeoutException, NoSuchElementException):
        return []
    except WebDriverException:
        log("🛑 Browser Crash Detected")
        return "RESTART"

# ---------------- INITIAL SETUP ---------------- #
log("📊 Connecting to Google Sheets...")
try:
    gc = gspread.service_account("credentials.json")
    sheet_main = gc.open("Stock List").worksheet("Sheet1")
    sheet_data = gc.open("MV2 for SQL").worksheet("Sheet2")

    company_list = sheet_main.col_values(3)  # URLs
    name_list = sheet_main.col_values(1)     # Names

    log(f"✅ Setup complete | Shard {SHARD_INDEX}/{SHARD_STEP} | Resume index {last_i} | Total {len(company_list)}")
except Exception as e:
    log(f"❌ Setup Error: {e}")
    sys.exit(1)

# ---------------- MAIN LOOP ---------------- #
driver = create_driver()
batch_list = []

# ✅ Faster: bigger batch = fewer API calls
BATCH_SIZE = 300

# ✅ Faster: compute date once (same run)
current_date = date.today().strftime("%m/%d/%Y")

# ✅ Faster: reduce sleep (or make 0)
ROW_SLEEP = 0.05

def flush_batch():
    global batch_list
    if not batch_list:
        return
    for attempt in range(3):
        try:
            sheet_data.batch_update(batch_list)
            log(f"🚀 Saved {len(batch_list)} updates")
            batch_list = []
            return
        except Exception as e:
            msg = str(e)
            log(f"⚠️ API Error: {msg[:160]}")
            if "429" in msg:
                log("⏳ Quota hit, sleeping 60s...")
                time.sleep(60)
            else:
                time.sleep(3)

try:
    # ✅ Run till END (removed fixed 2500 break)
    for i in range(last_i, len(company_list)):

        # ✅ Keep your sharding (only skips rows that belong to OTHER shards)
        if i % SHARD_STEP != SHARD_INDEX:
            continue

        url = (company_list[i] or "").strip()
        name = (name_list[i] if i < len(name_list) else f"Row {i+1}").strip()

        # ✅ Skip only truly invalid URL rows (otherwise it wastes 45s timeout)
        if not url.startswith("http"):
            log(f"⏭️ Row {i+1}: invalid/blank URL -> skipped")
            with open(checkpoint_file, "w") as f:
                f.write(str(i + 1))
            continue

        log(f"🔍 [{i+1}/{len(company_list)}] Scraping: {name}")

        # ✅ Reliability: retry ONCE if empty (extractor unchanged)
        values = scrape_tradingview(driver, url)
        if values == []:
            try:
                driver.refresh()
                time.sleep(0.7)
            except:
                pass
            values = scrape_tradingview(driver, url)

        # Restart browser if crashed
        if values == "RESTART":
            try:
                driver.quit()
            except:
                pass
            driver = create_driver()
            values = scrape_tradingview(driver, url)
            if values == "RESTART":
                values = []

        target_row = i + 1

        if isinstance(values, list) and values:
            # ✅ Only write A, C, and G onward. DO NOT touch B / D / E / F.
            batch_list.append({"range": f"A{target_row}", "values": [[name]]})
            batch_list.append({"range": f"C{target_row}", "values": [[current_date]]})
            batch_list.append({"range": f"G{target_row}", "values": [values]})
            log(f"✅ Values: {len(values)} cells | Buffered: {len(batch_list)}/{BATCH_SIZE}")
        else:
            # Keep your behavior: still write name/date even if values missing
            batch_list.append({"range": f"A{target_row}", "values": [[name]]})
            batch_list.append({"range": f"C{target_row}", "values": [[current_date]]})
            log(f"⚠️ No values found for {name} (A/C updated only)")

        if len(batch_list) >= BATCH_SIZE:
            flush_batch()

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if ROW_SLEEP:
            time.sleep(ROW_SLEEP)

finally:
    flush_batch()
    try:
        driver.quit()
    except:
        pass
    log("🏁 Scraping completed successfully")
