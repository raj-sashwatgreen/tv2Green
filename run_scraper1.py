import sys
import os
import time
import json
import random
import traceback
from datetime import date

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException

import gspread
from webdriver_manager.chrome import ChromeDriverManager


# ============================================================
# LOG
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
    f"checkpoint_week_{SHARD_INDEX}.txt"
)

EXPECTED_COUNT = 24

BATCH_SIZE = 100

RESTART_EVERY_ROWS = 20

COOKIE_FILE = os.getenv(
    "COOKIE_FILE",
    "cookies.json"
)

CHROME_DRIVER_PATH = ChromeDriverManager().install()

WEEK_OUTPUT_START_COL = 3  # C


# ============================================================
# OPTIONAL INDICATOR
# ============================================================

INDICATOR_TITLE = os.getenv(
    "INDICATOR_TITLE",
    ""
).strip()


# ============================================================
# TIMEOUTS
# ============================================================

VALUES_TIMEOUT = int(
    os.getenv(
        "VALUES_TIMEOUT",
        "40"
    )
)

PAGE_LOAD_TIMEOUT = int(
    os.getenv(
        "PAGE_LOAD_TIMEOUT",
        "60"
    )
)


# ============================================================
# FAILURE CONTROL
# ============================================================

MAX_CONSECUTIVE_FAILS = int(
    os.getenv(
        "MAX_CONSECUTIVE_FAILS",
        "5"
    )
)


# ============================================================
# DEBUG
# ============================================================

DEBUG_DIR = os.getenv(
    "DEBUG_DIR",
    "debug_week"
)

MAX_DEBUG_SAVES = int(
    os.getenv(
        "MAX_DEBUG_SAVES",
        "5"
    )
)


# ============================================================
# NUMBER CLEANING
# ============================================================

CLEAN_NUMBERS = (
    os.getenv(
        "CLEAN_NUMBERS",
        "0"
    ) == "1"
)


# ============================================================
# COLUMNS
# ============================================================

def col_num_to_letter(n):

    result = ""

    while n > 0:

        n, rem = divmod(
            n - 1,
            26
        )

        result = (
            chr(65 + rem)
            + result
        )

    return result


WEEK_START_COL_LETTER = (
    col_num_to_letter(
        WEEK_OUTPUT_START_COL
    )
)

WEEK_END_COL_LETTER = (
    col_num_to_letter(
        WEEK_OUTPUT_START_COL
        + EXPECTED_COUNT
        - 1
    )
)


log(
    f"📌 WEEK output columns: "
    f"{WEEK_START_COL_LETTER}:"
    f"{WEEK_END_COL_LETTER}"
)

log(
    f"📌 Expected values: "
    f"{EXPECTED_COUNT}"
)


# ============================================================
# JAVASCRIPT INDICATOR EXTRACTION
# ============================================================

GET_INDICATOR_VALUES_JS = """
const wantedTitle = arguments[0];

const titles = [
    ...document.querySelectorAll('[class*="title-"]')
].filter(
    e => (e.innerText || '').trim() === wantedTitle
);

if (!titles.length) {
    return {
        found: false,
        values: []
    };
}

let node = titles[0];

while (
    node &&
    node.querySelectorAll(
        '[class*="valueValue-"]'
    ).length === 0
) {
    node = node.parentElement;
}

if (!node) {
    return {
        found: true,
        values: []
    };
}

const elements = [
    ...node.querySelectorAll(
        '[class*="valueValue-"]'
    )
];

const values = [];

for (const e of elements) {

    const text = (
        e.innerText || ''
    ).trim();

    if (!text) {
        continue;
    }

    const lines = text
        .split(/\\n+/)
        .map(x => x.trim())
        .filter(Boolean);

    for (const line of lines) {
        values.push(line);
    }
}

return {
    found: true,
    values: values
};
"""


# ============================================================
# API RETRY
# ============================================================

def api_retry(
    func,
    *args,
    **kwargs
):

    for attempt in range(5):

        try:

            return func(
                *args,
                **kwargs
            )

        except Exception as e:

            wait = (
                2 ** attempt
            ) + random.random()

            log(
                f"⚠️ API Issue: "
                f"{str(e)[:100]}"
            )

            log(
                f"🔄 Retrying in "
                f"{wait:.1f}s..."
            )

            time.sleep(wait)

    return func(
        *args,
        **kwargs
    )


# ============================================================
# ERROR DESCRIPTION
# ============================================================

def describe_error(e):

    msg = (
        str(e)
        .strip()
        .replace(
            "\n",
            " "
        )
    )

    return (
        f"{type(e).__name__}: "
        f"{msg[:150] if msg else '(no message)'}"
    )


# ============================================================
# DEBUG
# ============================================================

_debug_saves = 0


def save_debug(
    drv,
    label
):

    global _debug_saves

    if (
        drv is None
        or _debug_saves >= MAX_DEBUG_SAVES
    ):

        return

    _debug_saves += 1

    try:

        os.makedirs(
            DEBUG_DIR,
            exist_ok=True
        )

        safe = "".join(
            ch
            if ch.isalnum()
            else "_"
            for ch in label
        )[:60]

        base = os.path.join(
            DEBUG_DIR,
            f"{_debug_saves:02d}_{safe}"
        )

        try:

            log(
                f"   🧾 Page title: "
                f"{drv.title!r}"
            )

            log(
                f"   🧾 Current URL: "
                f"{drv.current_url}"
            )

        except Exception:
            pass

        drv.save_screenshot(
            base + ".png"
        )

        with open(
            base + ".html",
            "w",
            encoding="utf-8"
        ) as f:

            f.write(
                drv.page_source
            )

        log(
            f"   🧾 Debug saved: "
            f"{base}.png / {base}.html"
        )

    except Exception as e:

        log(
            f"   ⚠️ Debug save failed: "
            f"{describe_error(e)}"
        )


# ============================================================
# CHECKPOINT
# ============================================================

if os.path.exists(
    CHECKPOINT_FILE
):

    try:

        with open(
            CHECKPOINT_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            last_i = max(
                int(
                    f.read().strip()
                ),
                START_ROW
            )

    except Exception:

        last_i = START_ROW

else:

    last_i = START_ROW


# ============================================================
# DRIVER
# ============================================================

driver = None


# ============================================================
# COOKIE LOADER
# ============================================================

def load_tradingview_cookies(
    drv
):

    if not os.path.exists(
        COOKIE_FILE
    ):

        log(
            f"⚠️ Cookie file not found: "
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

        if not isinstance(
            cookies,
            list
        ):

            log(
                "⚠️ cookies.json is not a list."
            )

            return

        added = 0
        failed = 0
        expired = 0

        now = time.time()

        for raw_cookie in cookies:

            try:

                if (
                    "name"
                    not in raw_cookie
                    or "value"
                    not in raw_cookie
                ):

                    continue

                cookie = {
                    "name":
                        raw_cookie["name"],
                    "value":
                        raw_cookie["value"]
                }

                # Path
                if raw_cookie.get(
                    "path"
                ):

                    cookie["path"] = (
                        raw_cookie["path"]
                    )

                # Secure
                if "secure" in raw_cookie:

                    cookie["secure"] = bool(
                        raw_cookie["secure"]
                    )

                # Domain
                domain = raw_cookie.get(
                    "domain"
                )

                if domain:

                    if (
                        "tradingview.com"
                        in domain
                    ):

                        cookie["domain"] = domain

                # Expiry
                expiry = (
                    raw_cookie.get(
                        "expiry"
                    )
                    or
                    raw_cookie.get(
                        "expirationDate"
                    )
                )

                if expiry:

                    try:

                        expiry_float = float(
                            expiry
                        )

                        if (
                            expiry_float
                            < now
                        ):

                            expired += 1

                        else:

                            cookie["expiry"] = int(
                                expiry_float
                            )

                    except Exception:
                        pass

                # SameSite
                same_site = raw_cookie.get(
                    "sameSite"
                )

                if same_site in (
                    "Strict",
                    "Lax",
                    "None"
                ):

                    cookie["sameSite"] = (
                        same_site
                    )

                drv.add_cookie(
                    cookie
                )

                added += 1

            except Exception:

                failed += 1

        log(
            f"🍪 Cookies: "
            f"{added} added, "
            f"{failed} failed, "
            f"{expired} expired"
        )

        if expired:

            log(
                "⚠️ Some cookies are expired. "
                "Update cookies.json if login "
                "is not retained."
            )

        drv.refresh()

        time.sleep(4)

        log(
            "🍪 TradingView refreshed "
            "after cookies."
        )

    except Exception as e:

        log(
            f"⚠️ Cookie loading failed: "
            f"{describe_error(e)}"
        )


# ============================================================
# CREATE DRIVER
# ============================================================

def create_driver():

    log(
        f"🌐 [WEEK Shard {SHARD_INDEX}] "
        f"Initializing Chrome..."
    )

    opts = Options()

    opts.add_argument(
        "--headless=new"
    )

    opts.add_argument(
        "--no-sandbox"
    )

    opts.add_argument(
        "--disable-dev-shm-usage"
    )

    opts.add_argument(
        "--window-size=1920,1080"
    )

    opts.add_argument(
        "--disable-gpu"
    )

    opts.add_argument(
        "--disable-extensions"
    )

    opts.add_argument(
        "--disable-notifications"
    )

    opts.add_argument(
        "--disable-popup-blocking"
    )

    opts.add_argument(
        "--lang=en-US"
    )

    opts.add_argument(
        "--disable-blink-features="
        "AutomationControlled"
    )

    opts.add_experimental_option(
        "excludeSwitches",
        [
            "enable-automation",
            "enable-logging"
        ]
    )

    drv = webdriver.Chrome(
        service=Service(
            CHROME_DRIVER_PATH
        ),
        options=opts
    )

    drv.set_page_load_timeout(
        PAGE_LOAD_TIMEOUT
    )

    # Browser UA
    try:

        ua = drv.execute_script(
            "return navigator.userAgent"
        )

        ua = ua.replace(
            "HeadlessChrome",
            "Chrome"
        )

        drv.execute_cdp_cmd(
            "Network.setUserAgentOverride",
            {
                "userAgent": ua
            }
        )

        log(
            f"   🧭 Browser: {ua}"
        )

    except Exception as e:

        log(
            f"   ⚠️ Could not set UA: "
            f"{describe_error(e)}"
        )

    # Cookie login
    load_tradingview_cookies(
        drv
    )

    return drv


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

    if driver:

        try:
            driver.quit()

        except Exception:
            pass

    driver = None


# ============================================================
# NORMAL POPUPS
# ============================================================

def close_normal_popups(
    drv
):

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

                        time.sleep(
                            0.3
                        )

                except Exception:
                    pass

        except Exception:
            pass

    if closed:

        log(
            f"🧹 Closed "
            f"{closed} normal popup(s)"
        )


# ============================================================
# REAL VERIFICATION DETECTION
# ============================================================

def check_real_verification(
    drv
):

    """
    IMPORTANT:

    Never use:

        'captcha' in drv.page_source

    TradingView can contain the word
    captcha in JavaScript/resources even
    when the chart is completely valid.

    We only inspect visible page text.
    """

    try:

        current_url = (
            drv.current_url
            .lower()
        )

        body = drv.find_element(
            By.TAG_NAME,
            "body"
        )

        visible_text = (
            body.text
            .lower()
            .strip()
        )

        if not visible_text:

            return False

        real_messages = [

            "verify you are human",

            "checking your browser",

            "access denied",

            "unusual traffic",

            "enable javascript and cookies",

        ]

        for message in real_messages:

            if message in visible_text:

                log(
                    f"🚨 Actual visible "
                    f"verification detected: "
                    f"{message}"
                )

                return True

        # We intentionally DO NOT check
        # page_source for "captcha".

        return False

    except Exception:

        return False


# ============================================================
# LEAF VALUE ELEMENTS
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

            # Parent/container contains
            # other value elements.
            #
            # Ignore it.
            if children:

                continue

            leaves.append(
                element
            )

        except Exception:

            continue

    return leaves


# ============================================================
# GET VALUES
# ============================================================

def get_values(
    drv
):

    """
    Returns:

        values
        indicator_found

    indicator_found is None when using
    normal/all-values mode.
    """

    try:

        # ----------------------------------------------------
        # INDICATOR MODE
        # ----------------------------------------------------

        if INDICATOR_TITLE:

            result = drv.execute_script(
                GET_INDICATOR_VALUES_JS,
                INDICATOR_TITLE
            ) or {}

            values = []

            for value in result.get(
                "values",
                []
            ):

                if not value:
                    continue

                lines = [
                    x.strip()
                    for x in value.splitlines()
                    if x.strip()
                ]

                values.extend(
                    lines
                )

            return (
                values,
                bool(
                    result.get(
                        "found"
                    )
                )
            )

        # ----------------------------------------------------
        # NORMAL MODE
        # ----------------------------------------------------

        selectors = [

            "[class*='valueValue']",

            "[class*='valueValue-']",

            "[data-name='legend-source-item-value']",

            "[data-name='legend-series-item-value']",

        ]

        best_values = []

        for selector in selectors:

            elements = (
                get_leaf_value_elements(
                    drv,
                    selector
                )
            )

            if not elements:
                continue

            values = []

            seen_elements = set()

            for element in elements:

                try:

                    element_id = element.id

                    if (
                        element_id
                        in seen_elements
                    ):

                        continue

                    seen_elements.add(
                        element_id
                    )

                    text = (
                        element.text
                        .strip()
                    )

                    if not text:

                        continue

                    lines = [
                        x.strip()
                        for x
                        in text.splitlines()
                        if x.strip()
                    ]

                    # IMPORTANT:
                    # duplicates are preserved.
                    values.extend(
                        lines
                    )

                except Exception:

                    continue

            log(
                f"🔎 {selector} -> "
                f"{len(values)} values"
            )

            if (
                len(values)
                > len(best_values)
            ):

                best_values = values

            if (
                len(values)
                >= EXPECTED_COUNT
            ):

                return (
                    values[
                        :EXPECTED_COUNT
                    ],
                    None
                )

        return (
            best_values[
                :EXPECTED_COUNT
            ],
            None
        )

    except Exception as e:

        log(
            f"   ⚠️ Value extraction "
            f"error: "
            f"{describe_error(e)}"
        )

        return [], False


# ============================================================
# CLEAN VALUE
# ============================================================

def clean_value(
    value
):

    if not CLEAN_NUMBERS:

        return value

    return (
        value
        .replace(
            "−",
            "-"
        )
        .replace(
            ",",
            ""
        )
    )


# ============================================================
# WAIT FOR VALUES
# ============================================================

def wait_for_values(
    drv,
    timeout
):

    start = time.time()

    best_values = []

    while (
        time.time() - start
        < timeout
    ):

        try:

            close_normal_popups(
                drv
            )

            values, found = (
                get_values(
                    drv
                )
            )

            if (
                len(values)
                > len(best_values)
            ):

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

                return (
                    best_values[
                        :EXPECTED_COUNT
                    ],
                    found
                )

        except Exception as e:

            log(
                f"⚠️ Waiting error: "
                f"{describe_error(e)}"
            )

        time.sleep(1)

    return (
        best_values[
            :EXPECTED_COUNT
        ],
        None
    )


# ============================================================
# SCRAPE WEEK
# ============================================================

def scrape_week(
    url,
    label=""
):

    if not url:

        return [], False

    for attempt in range(
        1,
        4
    ):

        drv = None

        try:

            drv = ensure_driver()

            log(
                f"🌐 Opening URL "
                f"(attempt {attempt}/3)"
            )

            log(
                f"🔗 {url}"
            )

            drv.get(
                url
            )

            # ------------------------------------------------
            # Initial rendering
            # ------------------------------------------------

            time.sleep(5)

            log(
                f"📄 Title: "
                f"{drv.title}"
            )

            log(
                f"🌐 Current URL: "
                f"{drv.current_url}"
            )

            # ------------------------------------------------
            # IMPORTANT:
            # Don't use "captcha" in page_source.
            # ------------------------------------------------

            if check_real_verification(
                drv
            ):

                log(
                    "⚠️ Actual visible "
                    "verification page."
                )

                save_debug(
                    drv,
                    f"{label}_verification"
                )

                restart_driver()

                if attempt < 3:

                    time.sleep(
                        5 + attempt
                    )

                    continue

                return [], False

            # ------------------------------------------------
            # Close normal popups
            # ------------------------------------------------

            close_normal_popups(
                drv
            )

            # ------------------------------------------------
            # Wait for 24 values
            # ------------------------------------------------

            vals, found = (
                wait_for_values(
                    drv,
                    VALUES_TIMEOUT
                )
            )

            # ------------------------------------------------
            # SUCCESS
            # ------------------------------------------------

            if (
                len(vals)
                >= EXPECTED_COUNT
            ):

                vals = [
                    clean_value(v)
                    for v in vals[
                        :EXPECTED_COUNT
                    ]
                ]

                log(
                    f"✅ SUCCESS: "
                    f"{len(vals)}/"
                    f"{EXPECTED_COUNT}"
                )

                for number, value in enumerate(
                    vals,
                    start=1
                ):

                    log(
                        f"   {number:02d}: "
                        f"{value}"
                    )

                return vals, True

            # ------------------------------------------------
            # FAILURE REASON
            # ------------------------------------------------

            if (
                INDICATOR_TITLE
                and found is False
            ):

                reason = (
                    f"indicator "
                    f"'{INDICATOR_TITLE}' "
                    f"not found"
                )

            elif not vals:

                reason = (
                    "no values found"
                )

            else:

                reason = (
                    f"only "
                    f"{len(vals)}/"
                    f"{EXPECTED_COUNT} "
                    f"values found"
                )

            log(
                f"⚠️ Attempt {attempt}: "
                f"{reason}"
            )

            if vals:

                log(
                    "📋 Partial values:"
                )

                for number, value in enumerate(
                    vals,
                    start=1
                ):

                    log(
                        f"   {number:02d}: "
                        f"{value}"
                    )

            save_debug(
                drv,
                f"{label}_attempt{attempt}"
            )

            # ------------------------------------------------
            # Retry same browser once
            # ------------------------------------------------

            if attempt == 1:

                log(
                    "🔄 Retrying same "
                    "browser..."
                )

                time.sleep(3)

                continue

            # ------------------------------------------------
            # Restart browser for final retry
            # ------------------------------------------------

            if attempt == 2:

                log(
                    "♻️ Restarting Chrome "
                    "before final retry..."
                )

                restart_driver()

                time.sleep(5)

                continue

            # ------------------------------------------------
            # Final failure
            # ------------------------------------------------

            vals = [
                clean_value(v)
                for v in vals
            ]

            return vals, False

        except Exception as e:

            log(
                f"❌ Scrape Attempt "
                f"{attempt}/3 Failed: "
                f"{describe_error(e)}"
            )

            log(
                traceback.format_exc()
                .strip()[-800:]
            )

            save_debug(
                drv,
                f"{label}_error{attempt}"
            )

            restart_driver()

            if attempt < 3:

                time.sleep(
                    3 + attempt
                )

    return [], False


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
        else "Unknown"
    )

    url = (

        url_list[i].strip()

        if (
            i < len(url_list)
            and "http"
            in url_list[i]
        )

        else None
    )

    log(
        f"🔍 [{i + 1}] "
        f"{name} | "
        f"URL: "
        f"{url if url else 'No URL'}"
    )

    vals, is_success = scrape_week(
        url,
        label=f"{i + 1}_{name}"
    )

    # --------------------------------------------------------
    # EXACTLY 24 CELLS
    # --------------------------------------------------------

    padded_vals = (
        vals
        + [""] * EXPECTED_COUNT
    )[:EXPECTED_COUNT]

    assert (
        len(padded_vals)
        == EXPECTED_COUNT
    )

    row_idx = i + 1

    row_payload = [

        {
            "range":
                f"A{row_idx}",
            "values":
                [[name]]
        },

        {
            "range":
                f"B{row_idx}",
            "values":
                [[current_date]]
        },

        {
            "range":
                f"{WEEK_START_COL_LETTER}"
                f"{row_idx}:"
                f"{WEEK_END_COL_LETTER}"
                f"{row_idx}",

            "values":
                [padded_vals]
        }

    ]

    # Rows without URL are not failures
    return (
        row_payload,
        is_success,
        bool(url)
    )


# ============================================================
# GOOGLE SHEETS
# ============================================================

def connect_sheets():

    gc = gspread.service_account(
        "credentials.json"
    )

    sh_main = gc.open(
        "Stock List"
    ).worksheet(
        "Sheet1"
    )

    sh_data = gc.open(
        "MV2 WEEK"
    ).worksheet(
        "Sheet1"
    )

    return (
        sh_main,
        sh_data
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

    # WEEK URL = Column H
    url_list = api_retry(
        sheet_main.col_values,
        8
    )

    loop_end = min(
        END_ROW,
        len(company_list)
    )

    log(
        f"✅ Ready. Processing "
        f"Rows {last_i + 1} "
        f"to {loop_end}"
    )

    log(
        "   Mode: "
        + (
            "indicator "
            + repr(INDICATOR_TITLE)
            if INDICATOR_TITLE
            else
            "all values on page"
        )
    )

except Exception as e:

    log(
        f"❌ Initial Connection Error: "
        f"{describe_error(e)}"
    )

    sys.exit(1)


# ============================================================
# STATE
# ============================================================

retry_indices = []

batch_list = []

current_date = date.today().strftime(
    "%m/%d/%Y"
)

consecutive_fails = 0

streak_start = None

aborted = False


# ============================================================
# FIRST PASS
# ============================================================

try:

    for i in range(
        last_i,
        loop_end
    ):

        payload, success, had_url = (
            process_row(
                i,
                company_list,
                url_list,
                current_date
            )
        )

        batch_list.extend(
            payload
        )

        if not success:

            retry_indices.append(i)

        # --------------------------------------------
        # Consecutive failures
        # --------------------------------------------

        if (
            had_url
            and not success
        ):

            if (
                consecutive_fails
                == 0
            ):

                streak_start = i

            consecutive_fails += 1

        elif success:

            consecutive_fails = 0

            streak_start = None

        # --------------------------------------------
        # Checkpoint
        # --------------------------------------------

        with open(
            CHECKPOINT_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            f.write(
                str(i + 1)
            )

        # --------------------------------------------
        # Stop after consecutive failures
        # --------------------------------------------

        if (
            MAX_CONSECUTIVE_FAILS
            and
            consecutive_fails
            >= MAX_CONSECUTIVE_FAILS
        ):

            log(
                f"🛑 "
                f"{consecutive_fails} "
                f"symbols in a row failed."
            )

            log(
                "🛑 Stopping WEEK shard."
            )

            with open(
                CHECKPOINT_FILE,
                "w",
                encoding="utf-8"
            ) as f:

                f.write(
                    str(streak_start)
                )

            aborted = True

            break

        # --------------------------------------------
        # Periodic restart
        # --------------------------------------------

        if (
            (i + 1)
            % RESTART_EVERY_ROWS
            == 0
        ):

            log(
                "♻️ Scheduled Chrome "
                "restart..."
            )

            restart_driver()

        # --------------------------------------------
        # Upload batch
        # 3 ranges per row
        # --------------------------------------------

        if (
            len(batch_list) // 3
            >= BATCH_SIZE
        ):

            log(
                f"🚀 Uploading batch "
                f"of {BATCH_SIZE}..."
            )

            api_retry(
                sheet_data.batch_update,
                batch_list,
                value_input_option="RAW"
            )

            batch_list = []

finally:

    if batch_list:

        log(
            "🚀 Uploading remaining "
            "batch..."
        )

        api_retry(
            sheet_data.batch_update,
            batch_list,
            value_input_option="RAW"
        )

        batch_list = []


# ============================================================
# ABORT
# ============================================================

if aborted:

    restart_driver()

    log(
        "❌ WEEK SHARD ABORTED."
    )

    sys.exit(1)


# ============================================================
# RETRY PASS
# ============================================================

if retry_indices:

    log(
        f"🔁 Starting Retry Pass "
        f"for "
        f"{len(retry_indices)} "
        f"symbols..."
    )

    restart_driver()

    batch_list = []

    for idx, i in enumerate(
        retry_indices,
        start=1
    ):

        payload, success, _ = (
            process_row(
                i,
                company_list,
                url_list,
                current_date
            )
        )

        batch_list.extend(
            payload
        )

        # Restart every 10 retry rows
        if idx % 10 == 0:

            log(
                "♻️ Retry-pass Chrome "
                "restart..."
            )

            restart_driver()

        # Smaller retry batches
        if (
            len(batch_list) // 3
            >= 10
        ):

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
    "🏁 WEEK SHARD COMPLETED."
)
