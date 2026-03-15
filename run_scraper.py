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

EXPECTED_COUNT = 22
BATCH_SIZE = 100
RESTART_EVERY_ROWS = 15
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3

# debug columns after C:X
STATUS_COL = "Y"
SHEET_URL_COL = "Z"
BROWSER_URL_COL = "AA"
PAGE_TITLE_COL = "AB"

# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        with open(checkpoint_file, "r") as f:
            last_i = max(int(f.read().strip()), START_ROW)
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

DAY_START_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL)
DAY_END_COL_LETTER = col_num_to_letter(DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1)

def api_retry(func, *args, **kwargs):
    for attempt in range(5):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            wait = (2 ** attempt) + random.random()
            log(f"⚠️ API Issue: {str(e)[:120]}. Retrying in {wait:.1f}s...")
            time.sleep(wait)
    return None

def build_not_found_row(label="Not Found"):
    return [label] + [""] * (EXPECTED_COUNT - 1)

def safe_trim(text, max_len=500):
    if text is None:
        return ""
    text = str(text).strip()
    return text[:max_len]

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

    opts.page_load_strategy = "eager"

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(35)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    cookie_data = {}
                    for k in ("name", "value", "path", "secure", "expiry", "domain"):
                        if k in c:
                            cookie_data[k] = c[k]
                    drv.add_cookie(cookie_data)
                except:
                    continue

            drv.refresh()
            time.sleep(2)
        except Exception as e:
            log(f"⚠️ Cookie load issue: {str(e)[:120]}")

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
    selectors = [
        "div[class*='valueValue']",
        "div[data-name='legend-series-item-value']",
        "div[class*='valueItem']",
        "div[class*='valuesWrapper'] div",
    ]

    collected = []

    for selector in selectors:
        try:
            elements = drv.find_elements(By.CSS_SELECTOR, selector)
            for el in elements:
                try:
                    txt = el.text.strip()
                    if txt and txt not in ("—", "") and txt not in collected:
                        collected.append(txt)
                except:
                    pass
            if len(collected) >= 5:
                break
        except:
            pass

    return collected

def scrape_day(url, stock_name=""):
    debug_browser_url = ""
    debug_page_title = ""

    if not url or "http" not in str(url):
        log(f"    ❌ SHEET URL for {stock_name}: {url}")
        return build_not_found_row("Bad URL"), "Bad URL", safe_trim(url), debug_browser_url, debug_page_title

    for attempt in range(3):
        try:
            drv = ensure_driver()

            log(f"    🔗 SHEET URL for {stock_name}: {url}")

            try:
                drv.get(url)
            except TimeoutException:
                log(f"    ⚠️ Page load timeout on attempt {attempt + 1}")
                try:
                    drv.execute_script("window.stop();")
                except:
                    pass

            debug_browser_url = safe_trim(drv.current_url)
            debug_page_title = safe_trim(drv.title)

            log(f"    🌍 BROWSER URL for {stock_name}: {debug_browser_url}")
            log(f"    📄 PAGE TITLE for {stock_name}: {debug_page_title}")

            WebDriverWait(drv, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            time.sleep(3)

            for y in [300, 700, 1100, 1500]:
                try:
                    drv.execute_script(f"window.scrollTo(0, {y});")
                except:
                    pass
                time.sleep(1.5)

            vals = []
            for _ in range(10):
                vals = get_values(drv)
                if len(vals) >= 8:
                    break
                try:
                    drv.execute_script("window.scrollBy(0, 250);")
                except:
                    pass
                time.sleep(1.5)

            if len(vals) == 0:
                time.sleep(3)
                vals = get_values(drv)

            if len(vals) > 0:
                vals = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]
                return vals, "OK", safe_trim(url), debug_browser_url, debug_page_title

        except TimeoutException:
            log(f"    ❌ Attempt {attempt + 1} timeout for {stock_name}")
            status = "Timeout"
        except WebDriverException as e:
            log(f"    ❌ Attempt {attempt + 1} webdriver error for {stock_name} | {str(e)[:120]}")
            status = "WebDriver Error"
        except Exception as e:
            log(f"    ❌ Attempt {attempt + 1} failed for {stock_name} | {str(e)[:120]}")
            status = "Failed"

        restart_driver()
        time.sleep(2)

    return build_not_found_row("Not Found"), status if 'status' in locals() else "Not Found", safe_trim(url), debug_browser_url, debug_page_title

# ---------------- SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data

# ---------------- MAIN ---------------- #
try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)

    if company_list is None:
        raise Exception("Could not read company list from column A")
    if url_list is None:
        raise Exception("Could not read URL list from column D")

    log(f"✅ Connection Stable. Processing {len(company_list)} symbols.")
except Exception as e:
    log(f"❌ Initial Connection Error: {e}")
    sys.exit(1)

batch_list = []
current_date = date.today().strftime("%m/%d/%Y")

try:
    loop_end = min(END_ROW, len(company_list))

    for i in range(last_i, loop_end):
        name = company_list[i].strip() if i < len(company_list) and company_list[i].strip() else f"Row-{i+1}"
        url = url_list[i].strip() if i < len(url_list) and url_list[i].strip() else ""

        log(f"🧾 ROW {i+1} | NAME: {name} | URL FROM SHEET: {url}")

        vals, status, sheet_url_used, browser_url_used, page_title_used = scrape_day(url, name)

        if not isinstance(vals, list):
            vals = build_not_found_row("Not Found")
            status = "Not Found"

        vals = (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]

        filled_count = len([v for v in vals if v and v not in ("Not Found", "Bad URL")])
        log(f"🔍 [{i+1}/{loop_end}] {name} | Status: {status} | Found {filled_count} values")

        row_idx = i + 1

        batch_list.append({
            "range": f"A{row_idx}",
            "values": [[name]]
        })
        batch_list.append({
            "range": f"B{row_idx}",
            "values": [[current_date]]
        })
        batch_list.append({
            "range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}",
            "values": [vals]
        })

        # debug info
        batch_list.append({
            "range": f"{STATUS_COL}{row_idx}",
            "values": [[status]]
        })
        batch_list.append({
            "range": f"{SHEET_URL_COL}{row_idx}",
            "values": [[sheet_url_used]]
        })
        batch_list.append({
            "range": f"{BROWSER_URL_COL}{row_idx}",
            "values": [[browser_url_used]]
        })
        batch_list.append({
            "range": f"{PAGE_TITLE_COL}{row_idx}",
            "values": [[page_title_used]]
        })

        with open(checkpoint_file, "w") as f:
            f.write(str(i + 1))

        if (i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if len(batch_list) >= (BATCH_SIZE * 7):
            log(f"🚀 Pushing {BATCH_SIZE} rows to Google Sheets...")
            result = api_retry(sheet_data.batch_update, batch_list, value_input_option="USER_ENTERED")
            if result:
                batch_list = []
            else:
                log("❌ Batch update failed. Keeping rows for final retry.")

finally:
    if batch_list:
        log("🚀 Pushing final rows...")
        result = api_retry(sheet_data.batch_update, batch_list, value_input_option="USER_ENTERED")
        if not result:
            log("❌ Final batch push failed.")

    restart_driver()
    log("🏁 SHARD COMPLETED.")
