// login.js
import dotenv from "dotenv";
dotenv.config();

const EMAIL = process.env.EMAIL;
const PASSWORD = process.env.PASSWORD;

const delay = (ms) => new Promise((res) => setTimeout(res, ms));

export async function login(page) {
  console.log("Navigating to TradingView Login...");

  // Set user agent to avoid bot detection
  await page.setUserAgent(
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
  );

  // Navigate to the login page
  await page.goto("https://www.tradingview.com/accounts/signin/", {
    waitUntil: "networkidle2",
    timeout: 30000,
  });

  // Wait for page to fully load
  await delay(5000);

  // Take a screenshot to debug what's on the page
  try {
    await page.screenshot({ path: "login_page_debug.png", fullPage: true });
    console.log("Debug screenshot saved as login_page_debug.png");
  } catch (e) {
    console.warn("Could not take screenshot:", e.message);
  }

  // Check if we're already logged in
  const isLoggedIn = await page.evaluate(() => {
    return (
      !window.location.href.includes("/signin") &&
      !window.location.href.includes("/login")
    );
  });

  if (isLoggedIn) {
    console.log("Already logged in!");
    return await page.cookies();
  }

  // Debug: Check what elements are available
  const pageInfo = await page.evaluate(() => {
    const buttons = Array.from(document.querySelectorAll("button")).map(
      (btn) => ({
        text: btn.textContent?.trim(),
        name: btn.name,
        className: btn.className,
        id: btn.id,
        type: btn.type,
        outerHTML: btn.outerHTML.substring(0, 200), // First 200 chars
      })
    );

    const inputs = Array.from(document.querySelectorAll("input")).map(
      (inp) => ({
        name: inp.name,
        type: inp.type,
        placeholder: inp.placeholder,
        className: inp.className,
        id: inp.id,
      })
    );

    const forms = Array.from(document.querySelectorAll("form")).map((form) => ({
      action: form.action,
      method: form.method,
      className: form.className,
      id: form.id,
    }));

    return {
      url: window.location.href,
      title: document.title,
      buttons: buttons.filter((btn) => btn.text || btn.name),
      inputs,
      forms,
      hasEmailButton: !!document.querySelector('button[name="Email"]'),
      hasUsernameInput: !!document.querySelector('input[name="id_username"]'),
      hasPasswordInput: !!document.querySelector('input[name="id_password"]'),
    };
  });

  console.log("Page Analysis:");
  console.log("URL:", pageInfo.url);
  console.log("Title:", pageInfo.title);
  console.log("Has Email Button:", pageInfo.hasEmailButton);
  console.log("Has Username Input:", pageInfo.hasUsernameInput);
  console.log("Has Password Input:", pageInfo.hasPasswordInput);
  console.log("Available buttons:", pageInfo.buttons.length);
  console.log("Available inputs:", pageInfo.inputs.length);

  if (pageInfo.buttons.length > 0) {
    console.log("Button details:");
    pageInfo.buttons.forEach((btn, i) => {
      console.log(
        `  ${i + 1}. "${btn.text}" (name: ${btn.name}, class: ${btn.className})`
      );
    });
  }

  // Strategy 1: Try to find and click the Email button
  let emailButtonClicked = false;

  // Multiple selectors for the email button
  const emailButtonSelectors = [
    'button[name="Email"]',
    'button[data-name="Email"]',
    'button:has-text("Email")',
    'button:has-text("Continue with Email")',
    'button[aria-label*="email"]',
    'button[title*="email"]',
    '.tv-button:has-text("Email")',
  ];

  for (const selector of emailButtonSelectors) {
    try {
      console.log(`Trying selector: ${selector}`);

      // Special handling for :has-text selectors
      if (selector.includes(":has-text(")) {
        const text = selector.match(/has-text\("([^"]+)"\)/)[1];
        const element = await page.evaluateHandle((searchText) => {
          const buttons = Array.from(document.querySelectorAll("button"));
          return buttons.find(
            (btn) =>
              btn.textContent &&
              btn.textContent.toLowerCase().includes(searchText.toLowerCase())
          );
        }, text);

        if (element && (await element.asElement())) {
          console.log(`Found email button with text: ${text}`);
          await element.asElement().click();
          emailButtonClicked = true;
          break;
        }
      } else {
        // Regular selector
        const element = await page.$(selector);
        if (element) {
          console.log(`Found email button with selector: ${selector}`);
          await element.click();
          emailButtonClicked = true;
          break;
        }
      }
    } catch (e) {
      console.log(`Selector ${selector} failed: ${e.message}`);
    }
  }

  if (emailButtonClicked) {
    console.log("Email button clicked, waiting for form...");
    await delay(3000);
  } else {
    console.log(
      "No email button found, checking if form is already visible..."
    );
  }

  // Strategy 2: Find and fill the username/email field
  const usernameSelectors = [
    'input[name="id_username"]',
    'input[name="username"]',
    'input[name="email"]',
    'input[type="email"]',
    'input[placeholder*="email" i]',
    'input[placeholder*="username" i]',
    'input[id*="username"]',
    'input[id*="email"]',
  ];

  let usernameFieldFound = false;
  for (const selector of usernameSelectors) {
    try {
      await page.waitForSelector(selector, { visible: true, timeout: 5000 });

      // Clear the field first
      await page.click(selector, { clickCount: 3 });
      await page.keyboard.press("Backspace");

      // Type the email
      await page.type(selector, EMAIL, { delay: 100 });

      console.log(`Username filled using selector: ${selector}`);
      usernameFieldFound = true;
      break;
    } catch (e) {
      console.log(`Username selector ${selector} failed: ${e.message}`);
    }
  }

  if (!usernameFieldFound) {
    throw new Error("Could not find username/email input field");
  }

  // Strategy 3: Find and fill the password field
  await delay(1000);

  const passwordSelectors = [
    'input[name="id_password"]',
    'input[name="password"]',
    'input[type="password"]',
    'input[id*="password"]',
    'input[placeholder*="password" i]',
  ];

  let passwordFieldFound = false;
  for (const selector of passwordSelectors) {
    try {
      await page.waitForSelector(selector, { visible: true, timeout: 5000 });

      // Clear the field first
      await page.click(selector, { clickCount: 3 });
      await page.keyboard.press("Backspace");

      // Type the password
      await page.type(selector, PASSWORD, { delay: 100 });

      console.log(`Password filled using selector: ${selector}`);
      passwordFieldFound = true;
      break;
    } catch (e) {
      console.log(`Password selector ${selector} failed: ${e.message}`);
    }
  }

  if (!passwordFieldFound) {
    throw new Error("Could not find password input field");
  }

  // Strategy 4: Submit the form
  await delay(1000);

  const submitSelectors = [
    'button[type="submit"]',
    'input[type="submit"]',
    "button.submitButton-LQwxK8Bm",
    'button[data-name="submit"]',
    "form button:last-child",
    'button:has-text("Sign in")',
    'button:has-text("Login")',
    'button:has-text("Continue")',
  ];

  let formSubmitted = false;
  for (const selector of submitSelectors) {
    try {
      if (selector.includes(":has-text(")) {
        const text = selector.match(/has-text\("([^"]+)"\)/)[1];
        const element = await page.evaluateHandle((searchText) => {
          const buttons = Array.from(
            document.querySelectorAll('button, input[type="submit"]')
          );
          return buttons.find(
            (btn) =>
              (btn.textContent &&
                btn.textContent
                  .toLowerCase()
                  .includes(searchText.toLowerCase())) ||
              (btn.value &&
                btn.value.toLowerCase().includes(searchText.toLowerCase()))
          );
        }, text);

        if (element && (await element.asElement())) {
          console.log(`Submitting form with text: ${text}`);
          await element.asElement().click();
          formSubmitted = true;
          break;
        }
      } else {
        const element = await page.$(selector);
        if (element) {
          console.log(`Submitting form with selector: ${selector}`);
          await element.click();
          formSubmitted = true;
          break;
        }
      }
    } catch (e) {
      console.log(`Submit selector ${selector} failed: ${e.message}`);
    }
  }

  if (!formSubmitted) {
    console.log("No submit button found, trying Enter key...");
    await page.keyboard.press("Enter");
    formSubmitted = true;
  }

  console.log("Waiting for login to complete...");

  // Wait for navigation or URL change
  let loginSuccessful = false;
  try {
    // Wait for either navigation or URL change
    await Promise.race([
      page.waitForNavigation({ waitUntil: "networkidle2", timeout: 30000 }),
      page.waitForFunction(
        () =>
          !window.location.href.includes("/signin") &&
          !window.location.href.includes("/login"),
        { timeout: 20000 }
      ),
    ]);
    loginSuccessful = true;
  } catch (e) {
    console.warn("Navigation timeout, checking current state...");
  }

  // Check final state
  const finalUrl = page.url();
  console.log("Final URL:", finalUrl);

  if (finalUrl.includes("/signin") || finalUrl.includes("/login")) {
    // Take screenshot to debug
    try {
      await page.screenshot({ path: "login_failed_debug.png", fullPage: true });
      console.log("Login failed screenshot saved as login_failed_debug.png");
    } catch (e) {
      // Ignore
    }

    // Check for error messages
    const errorMessage = await page.evaluate(() => {
      const errorSelectors = [
        ".error-message",
        ".alert-danger",
        ".tv-dialog__error",
        '[data-name="error"]',
        ".login-error",
        ".form-error",
        ".validation-error",
      ];

      for (const selector of errorSelectors) {
        const element = document.querySelector(selector);
        if (element && element.textContent.trim()) {
          return element.textContent.trim();
        }
      }
      return null;
    });

    if (errorMessage) {
      throw new Error(`Login failed: ${errorMessage}`);
    } else {
      throw new Error("Login failed: Still on login page");
    }
  }

  console.log("Login successful! Saving cookies...");
  const cookies = await page.cookies();
  return cookies;
}
