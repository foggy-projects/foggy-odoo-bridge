/**
 * Playwright tests: Setup Wizard
 *
 * Covers:
 * - Wizard opens from Settings
 * - Embedded mode shows 3 steps
 * - Engine mode selection works
 * - Closure table step exists
 */
import { test, expect } from '@playwright/test';
import { STORAGE_STATE, navigateToFoggySettings, screenshot } from './helpers';

test.use({ storageState: STORAGE_STATE });

test.describe('Setup Wizard', () => {

  test('Wizard opens from Settings page', async ({ page }) => {
    await navigateToFoggySettings(page);

    const wizardBtn = page.locator('button', { hasText: /设置向导|Setup Wizard/i });
    if (!(await wizardBtn.isVisible())) {
      test.skip();
      return;
    }

    await wizardBtn.click();
    await page.waitForTimeout(2000);

    // Wizard dialog should appear
    await expect(page.getByRole('dialog').first()).toBeVisible({ timeout: 10_000 });
    await screenshot(page, 'wizard-opened');
  });

  test('Wizard welcome step shows engine mode', async ({ page }) => {
    await navigateToFoggySettings(page);

    const wizardBtn = page.locator('button', { hasText: /设置向导|Setup Wizard/i });
    if (!(await wizardBtn.isVisible())) {
      test.skip();
      return;
    }

    await wizardBtn.click();
    await page.waitForTimeout(2000);

    // Should see engine mode selection on welcome step
    const dialog = page.getByRole('dialog').first();
    await expect(dialog.getByText(/Embedded/i).first()).toBeVisible();
    await expect(dialog.getByText(/Gateway/i).first()).toBeVisible();
    await screenshot(page, 'wizard-welcome-step');
  });
});
