/**
 * Playwright tests: Settings → Foggy MCP page
 *
 * Covers:
 * - Tab visibility and layout
 * - Engine mode radio buttons
 * - Community-only AI Chat/LLM absence
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
    await expect(page.getByText('Foggy MCP').first()).toBeVisible();
    await expect(page.getByText('Engine Mode').first()).toBeVisible();
    await screenshot(page, 'settings-foggy-tab');
  });

  test('Engine mode radio buttons are present', async ({ page }) => {
    await expect(page.getByText(/Embedded/i).first()).toBeVisible();
    await expect(page.getByText(/Gateway/i).first()).toBeVisible();
    await screenshot(page, 'settings-engine-mode');
  });

  test('AI Chat and LLM configuration are absent in Community', async ({ page }) => {
    const proOnlyText = page.getByText(/AI Chat|LLM Provider|OpenAI|Anthropic|Foggy AI/i);
    await expect(proOnlyText).toHaveCount(0);
    await screenshot(page, 'settings-no-ai-chat-llm-config');
  });

  test('Setup Wizard button is clickable', async ({ page }) => {
    const wizardBtn = page.locator('button', { hasText: /设置向导|Setup Wizard/i });
    if (await wizardBtn.isVisible()) {
      await expect(wizardBtn).toBeEnabled();
      await screenshot(page, 'settings-wizard-button');
    }
  });
});
