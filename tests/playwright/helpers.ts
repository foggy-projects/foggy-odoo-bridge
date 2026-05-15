/**
 * Shared helpers for Playwright tests.
 */
import { Page, expect } from '@playwright/test';
import fs from 'fs';
import path from 'path';

export const STORAGE_STATE = path.join(__dirname, '.auth', 'admin.json');

/**
 * Navigate to Settings → Foggy MCP tab.
 */
export async function navigateToFoggySettings(page: Page) {
  await page.goto('/web#action=base_setup.action_general_configuration', {
    waitUntil: 'domcontentloaded',
  });
  await expect(page.locator('.o_form_view')).toBeVisible({ timeout: 20_000 });

  const foggyTab = page.getByRole('tab', { name: /Foggy MCP/i });
  await expect(foggyTab).toBeVisible({ timeout: 10_000 });
  await foggyTab.click();
  await expect(page.getByText('Engine Mode').first()).toBeVisible({ timeout: 10_000 });
}

/**
 * Navigate to API Key management.
 */
export async function navigateToApiKeys(page: Page) {
  await page.goto('/web#action=foggy_mcp.foggy_my_api_key_action', {
    waitUntil: 'domcontentloaded',
  });
  await expect(page.locator('.o_list_view').or(page.locator('.o_kanban_view')))
    .toBeVisible({ timeout: 20_000 });
}

/**
 * Take a screenshot with a descriptive name.
 */
export async function screenshot(page: Page, name: string) {
  const dir = path.join(__dirname, '..', 'results', 'screenshots');
  fs.mkdirSync(dir, { recursive: true });
  await page.screenshot({
    path: path.join(dir, `${name}.png`),
    fullPage: true,
  });
}
