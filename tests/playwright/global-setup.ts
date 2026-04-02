/**
 * Global setup: login to Odoo and save auth state for all tests.
 */
import { chromium, FullConfig } from '@playwright/test';
import path from 'path';

const STORAGE_STATE = path.join(__dirname, '.auth', 'admin.json');

async function globalSetup(config: FullConfig) {
  const baseURL = config.projects[0].use.baseURL || 'http://localhost:8069';

  const browser = await chromium.launch();
  const page = await browser.newPage();

  // Navigate to login page
  await page.goto(`${baseURL}/web/login`);

  // Fill login form
  await page.fill('input[name="login"]', process.env.ODOO_LOGIN || 'admin');
  await page.fill('input[name="password"]', process.env.ODOO_PASSWORD || 'admin');

  // Select database if dropdown exists
  const dbSelect = page.locator('select[name="db"]');
  if (await dbSelect.isVisible()) {
    await dbSelect.selectOption(process.env.ODOO_DB || 'odoo_demo');
  }

  // Submit login
  await page.click('button[type="submit"]');

  // Wait for navigation to complete (Odoo loads slowly)
  await page.waitForURL('**/web**', { timeout: 30_000 });

  // Save auth state
  await page.context().storageState({ path: STORAGE_STATE });

  await browser.close();
}

export default globalSetup;
export { STORAGE_STATE };
