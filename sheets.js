// sheets.js
import { google } from "googleapis";
import dotenv from "dotenv";
dotenv.config();

const auth = new google.auth.GoogleAuth({
  credentials: {
    client_email: process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL,
    private_key: process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, "\n"),
  },
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const sheets = google.sheets({ version: "v4", auth });

export async function getChartLinks() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: process.env.SHEET_ID,
    range: `${process.env.SOURCE_SHEET}!D2:D`,
  });
  return res.data.values.map(([url]) => url.replace(/"/g, "").trim());
}

export async function writeBulkValuesToSheet(startRow, rows) {
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: process.env.OUTPUT_SHEET_ID,
    requestBody: {
      valueInputOption: "RAW",
      data: [
        {
          range: `${process.env.OUTPUT_SHEET}!B${startRow + 2}`,
          values: rows,
        },
      ],
    },
  });
}

export async function writeBulkWithRetry(startRow, rows, retries = 5) {
  console.log(
    `📝 Attempting to write ${rows.length} rows starting from row ${
      startRow + 2
    }`
  );
  const delay = (ms) => new Promise((res) => setTimeout(res, ms));

  for (let attempt = 0; attempt < retries; attempt++) {
    try {
      await writeBulkValuesToSheet(startRow, rows);
      return;
    } catch (err) {
      const msg = err.message || JSON.stringify(err);
      if (
        msg.includes("Quota exceeded") ||
        msg.includes("USER_RATE_LIMIT_EXCEEDED")
      ) {
        const wait = 1000 * Math.pow(2, attempt);
        console.warn(`Bulk quota exceeded. Retrying in ${wait / 1000}s...`);
        await delay(wait);
      } else {
        throw err;
      }
    }
  }

  console.error(`Failed to write bulk rows starting at ${startRow + 2}`);
}
