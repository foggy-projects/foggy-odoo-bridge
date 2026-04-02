/**
 * Playwright tests: API Key Management
 *
 * Covers:
 * - Key list page renders
 * - Existing fmcp_ keys are displayed
 * - New key creation form opens
 */
import { test, expect } from '@playwright/test';
import { STORAGE_STATE, navigateToApiKeys, screenshot } from './helpers';

test.use({ storageState: STORAGE_STATE });

test.describe('API Key Management', () => {

  test.beforeEach(async ({ page }) => {
    await navigateToApiKeys(page);
  });

  test('API key list page renders', async ({ page }) => {
    // Should see a list view or tree view
    const listView = page.locator('.o_list_view').or(page.locator('.o_kanban_view'));
    await expect(listView).toBeVisible({ timeout: 15_000 });
    await screenshot(page, 'api-keys-list');
  });

  test('At least one fmcp_ key exists', async ({ page }) => {
    // Look for fmcp_ prefix in the table
    const keyCell = page.locator('td', { hasText: 'fmcp_' }).or(
      page.locator('.o_data_cell', { hasText: 'fmcp_' })
    );
    await expect(keyCell.first()).toBeVisible({ timeout: 10_000 });
  });

  test('New button is available', async ({ page }) => {
    const newBtn = page.locator('button.o_list_button_add').or(
      page.locator('button', { hasText: /New|新建/ })
    );
    await expect(newBtn).toBeVisible();
  });
});
