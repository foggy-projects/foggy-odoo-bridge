import { defineConfig, devices } from '@playwright/test';

/**
 * Foggy Odoo Bridge — Playwright E2E Test Configuration
 *
 * Tests the Odoo web UI: Settings, Setup Wizard, API Keys, AI Chat.
 * Requires Odoo running at http://localhost:8069.
 */
export default defineConfig({
  testDir: '.',
  fullyParallel: false,  // Sequential — tests may share Odoo state
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 1 : 0,
  workers: 1,
  reporter: [
    ['html', { outputFolder: '../results/playwright-report' }],
    ['list'],
  ],
  timeout: 60_000,
  use: {
    baseURL: process.env.ODOO_URL || 'http://localhost:8069',
    trace: 'retain-on-failure',
    screenshot: 'on',
    video: 'retain-on-failure',
  },
  outputDir: '../results/playwright-output',

  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],

  /* Global setup: login once and share cookies */
  globalSetup: './global-setup.ts',
});
