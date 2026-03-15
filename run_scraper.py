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
RESTART_EVERY_ROWS = 20
COOKIE_FILE = os.getenv("COOKIE_FILE", "cookies.json")
CHROME_DRIVER_PATH = ChromeDriverManager().install()

DAY_OUTPUT_START_COL = 3  # Column C


# ---------------- STATE ---------------- #
if os.path.exists(checkpoint_file):
    try:
        saved = int(open(checkpoint_file).read().strip())
        last_i = max(saved, START_ROW)
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
            log(f"⚠️ API issue: {str(e)[:120]} | retry in {wait:.1f}s")
            time.sleep(wait)
    return func(*args, **kwargs)


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
    opts.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )

    drv = webdriver.Chrome(service=Service(CHROME_DRIVER_PATH), options=opts)
    drv.set_page_load_timeout(60)

    if os.path.exists(COOKIE_FILE):
        try:
            drv.get("https://in.tradingview.com/")
            with open(COOKIE_FILE, "r", encoding="utf-8") as f:
                cookies = json.load(f)

            for c in cookies:
                try:
                    clean_cookie = {}
                    for k in ("name", "value", "path", "secure", "expiry", "domain"):
                        if k in c:
                            clean_cookie[k] = c[k]
                    drv.add_cookie(clean_cookie)
                except:
                    continue

            drv.refresh()
            time.sleep(2)
        except Exception as e:
            log(f"⚠️ Cookie load failed: {str(e)[:80]}")

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
        values = []
        for el in elements:
            txt = el.text.strip()
            if txt:
                values.append(txt)
        return values
    except:
        return []


def wait_for_stable_values(drv, min_count=8, max_tries=6, delay=1.2):
    """
    Keep checking until values stabilize or best possible count found.
    """
    best = []

    for _ in range(max_tries):
        vals = get_values(drv)

        if len(vals) > len(best):
            best = vals

        if len(best) >= EXPECTED_COUNT:
            break

        if len(best) >= min_count:
            time.sleep(delay)
            new_vals = get_values(drv)
            if len(new_vals) <= len(best):
                break
            if len(new_vals) > len(best):
                best = new_vals
        else:
            time.sleep(delay)

    return best


def scrape_day(url):
    if not url:
        log("   ⚠️ No URL found")
        return [""] * EXPECTED_COUNT

    for attempt in range(3):
        try:
            drv = ensure_driver()
            drv.get(url)

            wait = WebDriverWait(drv, 20)
            wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='valueValue']"))
            )

            time.sleep(2)

            vals = wait_for_stable_values(drv)

            if len(vals) < EXPECTED_COUNT:
                try:
                    drv.execute_script("window.scrollTo(0, 700);")
                    time.sleep(2)
                    vals2 = wait_for_stable_values(drv)
                    if len(vals2) > len(vals):
                        vals = vals2
                except:
                    pass

            if len(vals) > 0:
                if len(vals) < EXPECTED_COUNT:
                    log(f"   ⚠️ Partial values found: {len(vals)}/{EXPECTED_COUNT}")
                return (vals + [""] * EXPECTED_COUNT)[:EXPECTED_COUNT]

            log(f"   ⚠️ No values captured on attempt {attempt + 1}")

        except (TimeoutException, WebDriverException, Exception) as e:
            log(f"   ❌ Scrape attempt {attempt + 1} failed: {str(e)[:100]}")
            restart_driver()
            time.sleep(2)

    return [""] * EXPECTED_COUNT


# ---------------- SHEETS ---------------- #
def connect_sheets():
    gc = gspread.service_account("credentials.json")
    sh_main = gc.open("Stock List").worksheet("Sheet1")
    sh_data = gc.open("MV2 DAY").worksheet("Sheet1")
    return sh_main, sh_data


try:
    sheet_main, sheet_data = connect_sheets()
    company_list = api_retry(sheet_main.col_values, 1)
    url_list = api_retry(sheet_main.col_values, 4)  # Column D
    log(f"✅ Ready. Processing Rows {last_i + 1} to {min(END_ROW, len(company_list))}")
except Exception as e:
    log(f"❌ Initial connection error: {e}")
    sys.exit(1)


# ---------------- MAIN ---------------- #
batch_list = []
rows_in_batch = 0
batch_start_i = None
current_date = date.today().strftime("%m/%d/%Y")
loop_end = min(END_ROW, len(company_list))


def upload_batch(sheet, updates, upto_index):
    if not updates:
        return

    api_retry(sheet.batch_update, updates, value_input_option="RAW")

    # checkpoint only after successful upload
    with open(checkpoint_file, "w") as f:
        f.write(str(upto_index + 1))

    log(f"✅ Uploaded rows up to {upto_index + 1}")


try:
    for i in range(last_i, loop_end):
        name = company_list[i].strip() if i < len(company_list) else ""
        raw_url = url_list[i].strip() if i < len(url_list) else ""
        url = raw_url if raw_url.startswith("http") else None

        log(f"🔍 [{i + 1}/{loop_end}] {name}")

        vals = scrape_day(url)
        row_idx = i + 1

        if batch_start_i is None:
            batch_start_i = i

        batch_list.append({"range": f"A{row_idx}", "values": [[name]]})
        batch_list.append({"range": f"B{row_idx}", "values": [[current_date]]})
        batch_list.append({
            "range": f"{DAY_START_COL_LETTER}{row_idx}:{DAY_END_COL_LETTER}{row_idx}",
            "values": [vals]
        })

        rows_in_batch += 1

        if (i + 1) % RESTART_EVERY_ROWS == 0:
            restart_driver()

        if rows_in_batch >= BATCH_SIZE:
            log(f"🚀 Uploading batch of {rows_in_batch} rows...")
            upload_batch(sheet_data, batch_list, i)
            batch_list = []
            rows_in_batch = 0
            batch_start_i = None

finally:
    try:
        if batch_list:
            log(f"🚀 Uploading final batch of {rows_in_batch} rows...")
            upload_batch(sheet_data, batch_list, loop_end - 1)
    finally:
        restart_driver()
        log("🏁 DAY SHARD COMPLETED.")
