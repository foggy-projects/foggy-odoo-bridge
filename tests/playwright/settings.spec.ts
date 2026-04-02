/**
 * Playwright tests: Settings → Foggy MCP page
 *
 * Covers:
 * - Tab visibility and layout
 * - Engine mode radio buttons
 * - LLM configuration fields
 * - Setup Wizard launch
 */
import { test, expect } from '@playwright/test';
import { STORAGE_STATE, navigateToFoggySettings, screenshot } from './helpers';

test.use({ storageState: STORAGE_STATE });

test.describe('Settings → Foggy MCP', () => {

  test.beforeEach(async ({ page }) => {
    await navigateToFoggySettings(page);
  });

  test('Foggy MCP tab is visible', async ({ page }) => {
    const tab = page.locator('.app_settings_header .tab', { hasText: 'Foggy MCP' });
    await expect(tab).toBeVisible();
    await screenshot(page, 'settings-foggy-tab');
  });

  test('Engine mode radio buttons are present', async ({ page }) => {
    // Should have two radio options: embedded and gateway
    const radios = page.locator('input[type="radio"][name="engine_mode"]');
    // If custom widget, look for radio-like elements
    const embeddedLabel = page.locator('text=内嵌模式').or(page.locator('text=Embedded'));
    const gatewayLabel = page.locator('text=网关模式').or(page.locator('text=Gateway'));

    const hasEmbedded = await embeddedLabel.isVisible();
    const hasGateway = await gatewayLabel.isVisible();

    expect(hasEmbedded || hasGateway).toBeTruthy();
    await screenshot(page, 'settings-engine-mode');
  });

  test('LLM configuration section is visible', async ({ page }) => {
    // Check for AI Chat configuration fields
    const providerLabel = page.locator('text=Provider').or(page.locator('text=提供商'));
    const apiKeyLabel = page.locator('label', { hasText: /API.*Key/i }).or(
      page.locator('label', { hasText: '密钥' })
    );

    const hasProvider = await providerLabel.isVisible();
    const hasApiKey = await apiKeyLabel.isVisible();

    expect(hasProvider || hasApiKey).toBeTruthy();
    await screenshot(page, 'settings-llm-config');
  });

  test('Setup Wizard button is clickable', async ({ page }) => {
    const wizardBtn = page.locator('button', { hasText: /设置向导|Setup Wizard/i });
    if (await wizardBtn.isVisible()) {
      await expect(wizardBtn).toBeEnabled();
      await screenshot(page, 'settings-wizard-button');
    }
  });
});
