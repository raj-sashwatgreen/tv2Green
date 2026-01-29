async function safeGoto(page, url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 60000 });
      await page.waitForSelector('[data-qa-id="legend"]', { timeout: 15000 });
      return true;
    } catch (err) {
      console.warn(`Retry ${i + 1} for ${url} – ${err.message}`);
      if (i === retries - 1) return false;
      await new Promise(r => setTimeout(r, 3000));
    }
  }
}

// always keep same column count
function fixedLength(arr, len, fill = "") {
  if (arr.length >= len) return arr.slice(0, len);
  return arr.concat(Array(len - arr.length).fill(fill));
}

// safe date builder (no //2025)
function buildDate(day, month, year) {
  if (!year) return "";
  if (!day && !month) return `${year}`;
  if (!day) day = "01";
  if (!month) month = "01";
  return `${String(day).padStart(2,"0")}/${String(month).padStart(2,"0")}/${year}`;
}

export async function scrapeChart(page, url) {
  const EXPECTED_VALUE_COUNT = 25; // change if your sheet needs more/less columns

  try {
    const success = await safeGoto(page, url);

    if (!success) {
      return ["", "", ...fixedLength(["NAVIGATION FAILED"], EXPECTED_VALUE_COUNT)];
    }

    // example date creation (edit source if you scrape real day/month/year)
    const now = new Date();
    const dateString = buildDate(
      now.getDate(),
      now.getMonth() + 1,
      now.getFullYear()
    );

    const values = await page.$$eval(
      '[data-qa-id="legend"] .item-l31H9iuA.study-l31H9iuA',
      (sections) => {
        const clubbed = [...sections].find((section) => {
          const title = section.querySelector(
            '[data-qa-id="legend-source-title"] .title-l31H9iuA'
          );
          const text = title?.innerText?.trim().toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!clubbed) return ["CLUBBED NOT FOUND"];

        const valueSpans = clubbed.querySelectorAll(".valueValue-l31H9iuA");

        return [...valueSpans].map((el) => {
          const t = el.innerText.trim();
          return t === "∅" ? "None" : t;
        });
      }
    );

    // first two blanks = shift by 2 columns
    // then fixed number of values so sheet never shifts
    return ["", "", dateString, ...fixedLength(values, EXPECTED_VALUE_COUNT - 1)];

  } catch (err) {
    console.error(`Error scraping ${url}:`, err.message);
    return ["", "", ...fixedLength(["ERROR"], EXPECTED_VALUE_COUNT)];
  }
}
