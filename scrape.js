async function safeGoto(page, url, retries = 3) {
  for (let i = 0; i < retries; i++) {
    try {
      await page.goto(url, { waitUntil: "networkidle2", timeout: 60000 });
      const content = await page.content();
      if (!content.includes("legend")) {
        throw new Error("Possibly logged out or blocked.");
      }
      return true;
    } catch (err) {
      console.warn(`Retry ${i + 1} for ${url} – ${err.message}`);
      if (i === retries - 1) return false;
      await new Promise((resolve) => setTimeout(resolve, 3000));
    }
  }
}

export async function scrapeChart(page, url) {
  try {
    const success = await safeGoto(page, url);
    if (!success) {
      console.error(`Failed to load ${url}`);
      return ["NAVIGATION FAILED"];
    }

    await page.waitForSelector('[data-name="legend"]', { timeout: 15000 });

    await page.waitForFunction(
      () => {
        const sections = document.querySelectorAll(
          '[data-name="legend"] .item-l31H9iuA.study-l31H9iuA'
        );
        for (const section of sections) {
          const title = section.querySelector(
            '[data-name="legend-source-title"] .title-l31H9iuA'
          );
          if (
            title?.innerText?.toLowerCase() === "clubbed" ||
            title?.innerText?.toLowerCase() === "l"
          ) {
            const values = section.querySelectorAll(".valueValue-l31H9iuA");
            return Array.from(values).some(
              (el) => el.innerText.trim() && el.innerText.trim() !== "∅"
            );
          }
        }
        return false;
      },
      { timeout: 15000 }
    );

    const values = await page.$$eval(
      '[data-name="legend"] .item-l31H9iuA.study-l31H9iuA',
      (studySections) => {
        const clubbed = [...studySections].find((section) => {
          const titleDiv = section.querySelector(
            '[data-name="legend-source-title"] .title-l31H9iuA'
          );
          const text = titleDiv?.innerText?.toLowerCase();
          return text === "clubbed" || text === "l";
        });

        if (!clubbed) return ["CLUBBED NOT FOUND"];

        const valueSpans = clubbed.querySelectorAll(".valueValue-l31H9iuA");
        const allValues = [...valueSpans].map((el) => {
          const text = el.innerText.trim();
          return text === "∅" ? "None" : text;
        });
        return allValues.slice(1);
      }
    );

    return values;
  } catch (err) {
    console.error(`Error scraping ${url}:`, err.message);
    return ["ERROR"];
  }
}
