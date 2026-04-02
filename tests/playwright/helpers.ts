/**
 * Shared helpers for Playwright tests.
 */
import { Page, expect } from '@playwright/test';
import path from 'path';

export const STORAGE_STATE = path.join(__dirname, '.auth', 'admin.json');

/**
 * Navigate to Settings → Foggy MCP tab.
 */
export async function navigateToFoggySettings(page: Page) {
  await page.goto('/web#action=base_setup.action_general_configuration');
  await page.waitForLoadState('networkidle');

  // Click the Foggy MCP tab
  const tab = page.locator('.app_settings_header .tab', { hasText: 'Foggy MCP' });
  if (await tab.isVisible()) {
    await tab.click();
    await page.waitForTimeout(500);
  }
}

/**
 * Navigate to Foggy AI Chat page.
 */
export async function navigateToAiChat(page: Page) {
  // Try direct URL first
  await page.goto('/web#action=foggy_mcp.action_foggy_chat');
  await page.waitForLoadState('networkidle');
  await page.waitForTimeout(1000);
}

/**
 * Navigate to API Key management.
 */
export async function navigateToApiKeys(page: Page) {
  await page.goto('/web#action=foggy_mcp.action_foggy_api_key');
  await page.waitForLoadState('networkidle');
}

/**
 * Take a screenshot with a descriptive name.
 */
export async function screenshot(page: Page, name: string) {
  const dir = path.join(__dirname, '..', 'results', 'screenshots');
  await page.screenshot({
    path: path.join(dir, `${name}.png`),
    fullPage: true,
  });
}
