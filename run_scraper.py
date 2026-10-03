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

import gspread
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# LOGGING
# ============================================================

def log(msg):
    t = time.strftime("%H:%M:%S")
    print(f"[{t}] {msg}", flush=True)


# ============================================================
# CONFIG
# ============================================================

SHARD_INDEX = int(os.getenv("SHARD_INDEX", "0"))
SHARD_SIZE = int(os.getenv("SHARD_SIZE", "500"))

START_ROW = SHARD_INDEX * SHARD_SIZE
END_ROW = START_ROW + SHARD_SIZE

CHECKPOINT_FILE = os.getenv(
    "CHECKPOINT_FILE",
    f"checkpoint_{SHARD_INDEX}.txt"
)

COOKIE_FILE = os.getenv(
    "COOKIE_FILE",
    "cookies.json"
)

EXPECTED_COUNT = 18

BATCH_SIZE = 50

RESTART_EVERY_ROWS = 20

DAY_OUTPUT_START_COL = 3  # C


# ============================================================
# COLUMN CALCULATION
# ============================================================

def col_num_to_letter(n):

    result = ""

    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result

    return result


DAY_START_COL_LETTER = col_num_to_letter(
    DAY_OUTPUT_START_COL
)

DAY_END_COL_LETTER = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT - 1
)

STATUS_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT
)

SHEET_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 1
)

BROWSER_URL_COL = col_num_to_letter(
    DAY_OUTPUT_START_COL + EXPECTED_COUNT + 2
)


log(
    f"📌 Values: {DAY_START_COL_LETTER}:{DAY_END_COL_LETTER}"
)

log(
    f"📌 Status: {STATUS_COL}"
)

log(
    f"📌 Sheet URL: {SHEET_URL_COL}"
)

log(
    f"📌 Browser URL: {BROWSER_URL_COL}"
)


# ============================================================
# GOOGLE API RETRY
# ============================================================

def api_retry(func, *args, **kwargs):

    for attempt in range(5):

        try:
            return func(*args, **kwargs)

        except Exception as e:

            wait = (2 ** attempt) + random.random()

            log(
                f"⚠️ API error: {str(e)[:120]}"
            )

            log(
                f"🔄 Retrying in {wait:.1f}s..."
            )

            time.sleep(wait)

    return func(*args, **kwargs)


# ============================================================
# CHECKPOINT
# ============================================================

if os.path.exists(CHECKPOINT_FILE):

    try:

        with open(
            CHECKPOINT_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            last_i = max(
                int(f.read().strip()),
                START_ROW
            )

    except Exception:

        last_i = START_ROW

else:

    last_i = START_ROW


# ============================================================
# GLOBAL DRIVER
# ============================================================

driver = None


# ============================================================
# CREATE DRIVER
# ============================================================

def create_driver():

    log(
        f"🌐 [Shard {SHARD_INDEX}] "
        f"Initializing Chrome..."
    )

    options = Options()

    # GitHub Actions
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Rendering
    options.add_argument(
        "--window-size=1920,1080"
    )

    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-popup-blocking")

    # Browser language
    options.add_argument("--lang=en-US")

    # Browser UA
    options.add_argument(
        "--user-agent=Mozilla/5.0 "
        "(Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 "
        "(KHTML, like Gecko) "
        "Chrome/154.0.0.0 Safari/537.36"
    )

    # Selenium automation setting
    options.add_argument(
        "--disable-blink-features=AutomationControlled"
    )

    drv = webdriver.Chrome(
        service=Service(
            ChromeDriverManager().install()
        ),
        options=options
    )

    # ========================================================
    # Browser-side configuration
    # ========================================================

    try:

        drv.execute_cdp_cmd(
            "Page.addScriptToEvaluateOnNewDocument",
            {
                "source": """
                    Object.defineProperty(
                        navigator,
                        'webdriver',
                        {
                            get: () => undefined
                        }
                    );

                    Object.defineProperty(
                        navigator,
                        'languages',
                        {
                            get: () => ['en-US', 'en']
                        }
                    );

                    Object.defineProperty(
                        navigator,
                        'platform',
                        {
                            get: () => 'Win32'
                        }
                    );
                """
            }
        )

    except Exception as e:

        log(
            f"⚠️ Browser configuration warning: "
            f"{str(e)[:100]}"
        )

    # ========================================================
    # LOAD COOKIES
    # ========================================================

    load_cookies(drv)

    return drv


# ============================================================
# LOAD TRADINGVIEW COOKIES
# ============================================================

def load_cookies(drv):

    if not os.path.exists(COOKIE_FILE):

        log(
            f"ℹ️ Cookie file not found: "
            f"{COOKIE_FILE}"
        )

        return

    try:

        log(
            "🍪 Opening TradingView "
            "before applying cookies..."
        )

        drv.get(
            "https://in.tradingview.com/"
        )

        time.sleep(3)

        with open(
            COOKIE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            cookies = json.load(f)

        if not isinstance(cookies, list):

            log(
                "⚠️ cookies.json is not a list."
            )

            return

        loaded = 0
        failed = 0

        for cookie_data in cookies:

            try:

                cookie = {}

                # --------------------------------------------
                # Required fields
                # --------------------------------------------

                if "name" not in cookie_data:
                    continue

                if "value" not in cookie_data:
                    continue

                cookie["name"] = cookie_data["name"]
                cookie["value"] = cookie_data["value"]

                # --------------------------------------------
                # Optional fields
                # --------------------------------------------

                if cookie_data.get("path"):
                    cookie["path"] = cookie_data["path"]

                if "secure" in cookie_data:
                    cookie["secure"] = bool(
                        cookie_data["secure"]
                    )

                # --------------------------------------------
                # Domain
                # --------------------------------------------

                domain = cookie_data.get("domain")

                if domain:

                    # Only TradingView cookies
                    if (
                        "tradingview.com"
                        in domain
                    ):

                        cookie["domain"] = domain

                # --------------------------------------------
                # Expiry
                # --------------------------------------------

                if cookie_data.get("expiry"):

                    try:

                        cookie["expiry"] = int(
                            cookie_data["expiry"]
                        )

                    except Exception:
                        pass

                # --------------------------------------------
                # SameSite
                # --------------------------------------------

                same_site = cookie_data.get(
                    "sameSite"
                )

                if same_site in (
                    "Strict",
                    "Lax",
                    "None"
                ):

                    cookie["sameSite"] = same_site

                # --------------------------------------------
                # Add cookie
                # --------------------------------------------

                drv.add_cookie(cookie)

                loaded += 1

            except Exception:

                failed += 1

        log(
            f"🍪 Cookies loaded: {loaded}"
        )

        if failed:

            log(
                f"⚠️ Cookies rejected: {failed}"
            )

        # Important:
        # refresh AFTER cookies have been applied
        drv.refresh()

        time.sleep(4)

        log(
            "🍪 TradingView session refreshed "
            "after cookies."
        )

    except Exception as e:

        log(
            f"⚠️ Cookie loading error: "
            f"{str(e)[:150]}"
        )


# ============================================================
# DRIVER MANAGEMENT
# ============================================================

def ensure_driver():

    global driver

    if driver is None:

        driver = create_driver()

    return driver


def restart_driver():

    global driver

    if driver is not None:

        log("♻️ Closing Chrome...")

        try:
            driver.quit()

        except Exception:
            pass

    driver = None


# ============================================================
# WAIT FOR PAGE LOAD
# ============================================================

def wait_for_document(drv, timeout=20):

    start = time.time()

    while (
        time.time() - start
        < timeout
    ):

        try:

            state = drv.execute_script(
                "return document.readyState"
            )

            if state == "complete":
                return True

        except Exception:
            pass

        time.sleep(0.5)

    return False


# ============================================================
# NORMAL POPUP HANDLER
# ============================================================

def close_normal_popups(drv):

    selectors = [

        "button[aria-label='Close']",

        "button[aria-label='close']",

        "[data-name='close']",

        "[data-name='close-button']",

        "[role='dialog'] "
        "button[aria-label='Close']",

        "[class*='dialog'] "
        "button[aria-label='Close']",

        "[class*='modal'] "
        "button[aria-label='Close']",

    ]

    closed = 0

    for selector in selectors:

        try:

            elements = drv.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            for element in elements:

                try:

                    if element.is_displayed():

                        drv.execute_script(
                            "arguments[0].click();",
                            element
                        )

                        closed += 1

                        time.sleep(0.3)

                except Exception:
                    pass

        except Exception:
            pass

    if closed:

        log(
            f"🧹 Closed {closed} "
            f"normal popup(s)"
        )


# ============================================================
# REAL VISIBLE VERIFICATION CHECK
# ============================================================

def check_real_verification(drv):

    """
    IMPORTANT:
    Do NOT inspect page_source for the word
    'captcha'.

    TradingView can contain that word inside
    JavaScript/resources even on a perfectly
    working chart.

    We inspect visible body text instead.
    """

    try:

        current_url = drv.current_url.lower()

        title = drv.title.lower()

        # A TradingView chart with a valid title
        # is not automatically considered blocked.
        if (
            "/chart/" in current_url
            and title
            and "tradingview" not in title
        ):

            pass

        body = drv.find_element(
            By.TAG_NAME,
            "body"
        )

        visible_text = body.text.lower().strip()

        if not visible_text:
            return False

        real_verification_messages = [

            "verify you are human",

            "checking your browser",

            "access denied",

            "unusual traffic",

            "enable javascript and cookies",

        ]

        for message in real_verification_messages:

            if message in visible_text:

                log(
                    f"🚨 Visible verification message: "
                    f"{message}"
                )

                return True

        return False

    except Exception:

        return False


# ============================================================
# FIND LEAF VALUE ELEMENTS
# ============================================================

def get_leaf_value_elements(
    drv,
    selector
):

    try:

        elements = drv.find_elements(
            By.CSS_SELECTOR,
            selector
        )

    except Exception:

        return []

    if not elements:

        return []

    leaves = []

    for element in elements:

        try:

            children = element.find_elements(
                By.CSS_SELECTOR,
                selector
            )

            # Parent/container:
            # don't read it because its .text may contain
            # all 18 child values.
            if children:

                continue

            leaves.append(element)

        except Exception:

            continue

    return leaves


# ============================================================
# EXTRACT TRADINGVIEW VALUES
# ============================================================

def get_tradingview_values(drv):

    selectors = [

        "[class*='valueValue']",

        "[class*='valueValue-']",

        "[data-name='legend-source-item-value']",

        "[data-name='legend-series-item-value']",

    ]

    best_values = []

    for selector in selectors:

        elements = get_leaf_value_elements(
            drv,
            selector
        )

        if not elements:
            continue

        values = []

        processed_elements = set()

        for element in elements:

            try:

                element_id = element.id

                if element_id in processed_elements:
                    continue

                processed_elements.add(
                    element_id
                )

                text = element.text.strip()

                if not text:
                    continue

                # If a leaf itself contains multiple lines,
                # split them.
                lines = [
                    line.strip()
                    for line in text.splitlines()
                    if line.strip()
                ]

                # IMPORTANT:
                # Do not remove duplicate text.
                #
                # Example:
                # 1
                # 10
                # 1
                #
                # Both "1" values are valid.
                values.extend(lines)

            except Exception:

                continue

        if len(values) > len(best_values):

            best_values = values

        log(
            f"🔎 {selector} -> "
            f"{len(values)} values"
        )

        if len(values) >= EXPECTED_COUNT:

            return values[
                :EXPECTED_COUNT
            ]

    return best_values[
        :EXPECTED_COUNT
    ]


# ============================================================
# WAIT FOR 18 VALUES
# ============================================================

def wait_for_values(
    drv,
    timeout=40
):

    start = time.time()

    best_values = []

    while (
        time.time() - start
        < timeout
    ):

        try:

            close_normal_popups(drv)

            values = get_tradingview_values(
                drv
            )

            if len(values) > len(best_values):

                best_values = values

                log(
                    f"📊 Values found: "
                    f"{len(best_values)}/"
                    f"{EXPECTED_COUNT}"
                )

            if (
                len(best_values)
                >= EXPECTED_COUNT
            ):

                return best_values[
                    :EXPECTED_COUNT
                ]

        except Exception as e:

            log(
                f"⚠️ Extraction error: "
                f"{str(e)[:100]}"
            )

        time.sleep(1)

    return best_values[
        :EXPECTED_COUNT
    ]


# ============================================================
# SCRAPE ONE URL
# ============================================================

def scrape_day(url):

    if not url:

        return (
            [""] * EXPECTED_COUNT,
            "NOT OK",
            "",
            ""
        )

    for attempt in range(1, 4):

        try:

            drv = ensure_driver()

            log(
                f"🌐 Opening URL "
                f"(attempt {attempt}/3)"
            )

            log(
                f"🔗 {url}"
            )

            drv.get(url)

            # Initial TradingView rendering
            time.sleep(5)

            wait_for_document(
                drv,
                timeout=20
            )

            log(
                f"📄 Title: {drv.title}"
            )

            log(
                f"🌐 Current URL: "
                f"{drv.current_url}"
            )

            # ------------------------------------------------
            # Actual visible verification check
            # ------------------------------------------------

            if check_real_verification(drv):

                log(
                    "⚠️ Actual visible verification "
                    "page detected."
                )

                restart_driver()

                if attempt < 3:

                    time.sleep(
                        5 + attempt
                    )

                    continue

                return (
                    [""] * EXPECTED_COUNT,
                    "NOT OK",
                    url,
                    ""
                )

            # ------------------------------------------------
            # Close ordinary UI popups
            # ------------------------------------------------

            close_normal_popups(drv)

            # ------------------------------------------------
            # Give dynamic chart more time
            # ------------------------------------------------

            time.sleep(3)

            # ------------------------------------------------
            # Extract values
            # ------------------------------------------------

            values = wait_for_values(
                drv,
                timeout=40
            )

            # ------------------------------------------------
            # Additional scrolling
            # ------------------------------------------------

            if len(values) < EXPECTED_COUNT:

                log(
                    f"🔄 Only "
                    f"{len(values)}/"
                    f"{EXPECTED_COUNT} values."
                )

                scroll_positions = [
                    0,
                    300,
                    600,
                    900,
                    1200,
                    1600,
                    2000,
                    2500,
                ]

                for scroll_y in scroll_positions:

                    try:

                        drv.execute_script(
                            f"window.scrollTo("
                            f"0, {scroll_y});"
                        )

                    except Exception:
                        pass

                    time.sleep(1.5)

                    new_values = (
                        get_tradingview_values(
                            drv
                        )
                    )

                    if (
                        len(new_values)
                        > len(values)
                    ):

                        values = new_values

                        log(
                            f"📈 Now found "
                            f"{len(values)}/"
                            f"{EXPECTED_COUNT}"
                        )

                    if (
                        len(values)
                        >= EXPECTED_COUNT
                    ):

                        break

            browser_url = drv.current_url

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if len(values) >= EXPECTED_COUNT:

                values = values[
                    :EXPECTED_COUNT
                ]

                log(
                    f"✅ SUCCESS: "
                    f"{len(values)}/"
                    f"{EXPECTED_COUNT}"
                )

                for number, value in enumerate(
                    values,
                    start=1
                ):

                    log(
                        f"   {number:02d}: {value}"
                    )

                return (
                    values,
                    "OK",
                    url,
                    browser_url
                )

            # ------------------------------------------------
            # FAILED EXTRACTION
            # ------------------------------------------------

            log(
                f"⚠️ Only "
                f"{len(values)}/"
                f"{EXPECTED_COUNT} values found."
            )

            if values:

                log(
                    "📋 Values detected:"
                )

                for number, value in enumerate(
                    values,
                    start=1
                ):

                    log(
                        f"   {number:02d}: {value}"
                    )

            if attempt < 3:

                restart_driver()

                wait_time = 3 + (
                    attempt * 2
                )

                log(
                    f"🔄 Retrying in "
                    f"{wait_time}s..."
                )

                time.sleep(
                    wait_time
                )

                continue

            # Pad to exactly 18 cells
            values = (
                values
                + [""] * (
                    EXPECTED_COUNT
                    - len(values)
                )
            )

            return (
                values[:EXPECTED_COUNT],
                "NOT OK",
                url,
                browser_url
            )

        except Exception as e:

            log(
                f"❌ Attempt {attempt}/3 "
                f"failed: "
                f"{str(e)[:200]}"
            )

            restart_driver()

            if attempt < 3:

                time.sleep(
                    3 + attempt
                )

    return (
        [""] * EXPECTED_COUNT,
        "NOT OK",
        url,
        ""
    )


# ============================================================
# GOOGLE SHEETS
# ============================================================

def connect_sheets():

    gc = gspread.service_account(
        "credentials.json"
    )

    sh_main = gc.open(
        "STOCKLIST 2"
    ).worksheet(
        "Sheet1"
    )

    sh_data = gc.open(
        "MV2 DAY"
    ).worksheet(
        "Sheet1"
    )

    return sh_main, sh_data


# ============================================================
# PROCESS ROW
# ============================================================

def process_row(
    i,
    company_list,
    url_list,
    current_date
):

    name = (
        company_list[i].strip()
        if i < len(company_list)
        else ""
    )

    url = (

        url_list[i].strip()

        if (
            i < len(url_list)
            and "http" in url_list[i]
        )

        else None

    )

    log(
        f"🔍 [{i + 1}] "
        f"{name} | "
        f"URL: "
        f"{url if url else 'No URL'}"
    )

    values, status, source_url, browser_url = (
        scrape_day(url)
    )

    # ALWAYS exactly 18 values
    values = values[:EXPECTED_COUNT]

    values += [
        ""
    ] * (
        EXPECTED_COUNT
        - len(values)
    )

    assert len(values) == EXPECTED_COUNT

    row_idx = i + 1

    payload = [

        {
            "range": f"A{row_idx}",
            "values": [
                [name]
            ]
        },

        {
            "range": f"B{row_idx}",
            "values": [
                [current_date]
            ]
        },

        {
            "range":
                f"{DAY_START_COL_LETTER}{row_idx}:"
                f"{DAY_END_COL_LETTER}{row_idx}",

            "values": [
                values
            ]
        },

        {
            "range": f"{STATUS_COL}{row_idx}",
            "values": [
                [status]
            ]
        },

        {
            "range": f"{SHEET_URL_COL}{row_idx}",
            "values": [
                [source_url]
            ]
        },

        {
            "range": f"{BROWSER_URL_COL}{row_idx}",
            "values": [
                [browser_url]
            ]
        },

    ]

    return (
        payload,
        status == "OK"
    )


# ============================================================
# CONNECT
# ============================================================

try:

    sheet_main, sheet_data = (
        connect_sheets()
    )

    company_list = api_retry(
        sheet_main.col_values,
        1
    )

    url_list = api_retry(
        sheet_main.col_values,
        4
    )

    log(
        f"✅ Starting rows "
        f"{last_i + 1} to "
        f"{min(END_ROW, len(company_list))}"
    )

except Exception as e:

    log(
        f"❌ Connection Error: {e}"
    )

    sys.exit(1)


# ============================================================
# MAIN
# ============================================================

retry_indices = []

batch_list = []

current_date = date.today().strftime(
    "%m/%d/%Y"
)

loop_end = min(
    END_ROW,
    len(company_list)
)


# ============================================================
# FIRST PASS
# ============================================================

for i in range(
    last_i,
    loop_end
):

    payload, success = process_row(
        i,
        company_list,
        url_list,
        current_date
    )

    batch_list.extend(
        payload
    )

    if not success:

        retry_indices.append(i)

    # Save checkpoint
    with open(
        CHECKPOINT_FILE,
        "w",
        encoding="utf-8"
    ) as f:

        f.write(
            str(i + 1)
        )

    # Periodic Chrome restart
    if (
        (i + 1)
        % RESTART_EVERY_ROWS
        == 0
    ):

        log(
            "♻️ Scheduled Chrome restart..."
        )

        restart_driver()

    # Upload every 50 rows
    if (
        len(batch_list) // 6
        >= BATCH_SIZE
    ):

        log(
            "🚀 Uploading batch..."
        )

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )

        batch_list = []


# ============================================================
# REMAINING FIRST PASS
# ============================================================

if batch_list:

    log(
        "🚀 Uploading remaining batch..."
    )

    api_retry(
        sheet_data.batch_update,
        batch_list,
        value_input_option="RAW"
    )

    batch_list = []


# ============================================================
# RETRY PASS
# ============================================================

if retry_indices:

    log(
        f"🔁 Retrying "
        f"{len(retry_indices)} "
        f"NOT OK rows..."
    )

    restart_driver()

    batch_list = []

    for retry_number, i in enumerate(
        retry_indices,
        start=1
    ):

        payload, success = process_row(
            i,
            company_list,
            url_list,
            current_date
        )

        batch_list.extend(
            payload
        )

        if retry_number % 10 == 0:

            log(
                "♻️ Restarting Chrome "
                "during retry pass..."
            )

            restart_driver()

            if batch_list:

                api_retry(
                    sheet_data.batch_update,
                    batch_list,
                    value_input_option="RAW"
                )

                batch_list = []

    if batch_list:

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )


# ============================================================
# CLEANUP
# ============================================================

restart_driver()

log(
    "🏁 SCRAPING COMPLETED."
)
