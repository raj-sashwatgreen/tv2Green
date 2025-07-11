import puppeteer from "puppeteer";
import fs from "fs";
import { login } from "./login.js";
import { scrapeChart } from "./scrape.js";
import { getChartLinks, writeBulkWithRetry } from "./sheets.js";

const COOKIE_PATH = "./cookies.json";

if (process.env.COOKIES_BASE64 && !fs.existsSync(COOKIE_PATH)) {
  const decoded = Buffer.from(process.env.COOKIES_BASE64, "base64").toString(
    "utf-8"
  );
  fs.writeFileSync(COOKIE_PATH, decoded);
  console.log("cookies.json restored from Base64");
}

async function saveCookies(cookies) {
  fs.writeFileSync(COOKIE_PATH, JSON.stringify(cookies, null, 2));
}

async function loadCookies(page) {
  if (fs.existsSync(COOKIE_PATH)) {
    const cookies = JSON.parse(fs.readFileSync(COOKIE_PATH));
    await page.setCookie(...cookies);
    return true;
  }
  return false;
}

(async () => {
  const chartLinks = await getChartLinks();

  const BATCH_INDEX = parseInt(
    process.argv[2] || process.env.BATCH_INDEX || "0",
    10
  );
  const ACCOUNT_START = parseInt(process.env.ACCOUNT_START || "0", 10);
  const ACCOUNT_END = parseInt(
    process.env.ACCOUNT_END || chartLinks.length.toString(),
    10
  );
  const BATCH_SIZE = parseInt(process.env.BATCH_SIZE || "100", 10);

  const accountLinks = chartLinks.slice(ACCOUNT_START, ACCOUNT_END);

  const start = BATCH_INDEX * BATCH_SIZE;
  const end = start + BATCH_SIZE;
  const batchLinks = accountLinks.slice(start, end);

  console.log(`Account range: ${ACCOUNT_START}–${ACCOUNT_END}`);
  console.log(`Processing batch ${BATCH_INDEX}: ${start} to ${end}`);

  const browser = await puppeteer.launch({
    headless: "new",
    slowMo: 50,
    args: ["--no-sandbox", "--disable-setuid-sandbox"],
  });

  let page = await browser.newPage();

  const hasCookies = await loadCookies(page);
  if (!hasCookies) {
    try {
      const cookies = await login(page);
      await saveCookies(cookies);
    } catch (err) {
      console.error("Login failed:", err.message);
      await browser.close();
      process.exit(1);
    }
  }

  let rowBuffer = [];
  let startRow = -1;

  for (let i = 0; i < batchLinks.length; i++) {
    const url = batchLinks[i];
    if (!url) continue;

    const globalIndex = ACCOUNT_START + BATCH_INDEX * BATCH_SIZE + i;
    console.log(`Scraping Row ${globalIndex + 2}: ${url}`);

    try {
      const values = await scrapeChart(page, url);
      const month = values[0];
      const day = values[1];
      const date = `${day}/${month}/2025`;
      const rowData = [date, ...values];

      if (rowBuffer.length === 0) startRow = globalIndex;
      rowBuffer.push(rowData);

      // Write in bulk every 10 rows
      if (rowBuffer.length === 10) {
        await writeBulkWithRetry(startRow, rowBuffer);
        rowBuffer = [];
        startRow = -1;
      }

      await new Promise((r) => setTimeout(r, 1000));
    } catch (err) {
      console.error(` Error scraping ${url}:`, err.message);
    }

    if ((i + 1) % 100 === 0) {
      console.log("Restarting page to clear memory...");
      await page.close();
      page = await browser.newPage();
      await loadCookies(page);
    }

    await new Promise((r) => setTimeout(r, 500));
  }

  // Write any remaining rows.
  if (rowBuffer.length > 0) {
    console.log(
      `Writing remaining ${rowBuffer.length} rows starting from ${startRow + 2}`
    );
    await writeBulkWithRetry(startRow, rowBuffer);
  }

  await browser.close();
})();
