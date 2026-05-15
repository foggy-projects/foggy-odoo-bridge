#!/usr/bin/env node
/**
 * Generate Odoo Apps listing images for the Community addon.
 *
 * Outputs:
 *   foggy_mcp/static/description/banner.png
 *   foggy_mcp/static/description/screenshot_settings.png
 *   foggy_mcp/static/description/screenshot_setup_wizard.png
 *   foggy_mcp/static/description/screenshot_api_keys.png
 *
 * The banner is rendered from local HTML/CSS. The screenshots are captured
 * from a running Odoo instance so they reflect the actual Community UI.
 */

import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const require = createRequire(import.meta.url);
const { chromium } = require('../tests/playwright/node_modules/playwright');

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.resolve(__dirname, '..');
const outputDir = path.join(root, 'foggy_mcp', 'static', 'description');

const ODOO_URL = process.env.ODOO_URL || 'http://localhost:8077';
const ODOO_DB = process.env.ODOO_DB || 'community_smoke';
const ODOO_LOGIN = process.env.ODOO_LOGIN || 'admin';
const ODOO_PASSWORD = process.env.ODOO_PASSWORD || 'admin';

fs.mkdirSync(outputDir, { recursive: true });

function out(name) {
  return path.join(outputDir, name);
}

async function renderBanner(browser) {
  const page = await browser.newPage({ viewport: { width: 1280, height: 720 } });
  await page.setContent(`
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <style>
    * { box-sizing: border-box; }
    body {
      margin: 0;
      width: 1280px;
      height: 720px;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: #f7f8fb;
      color: #172033;
    }
    .frame {
      width: 1280px;
      height: 720px;
      padding: 56px 64px;
      background:
        linear-gradient(135deg, rgba(99,102,241,0.14), transparent 34%),
        linear-gradient(315deg, rgba(20,184,166,0.16), transparent 38%),
        #f7f8fb;
      position: relative;
      overflow: hidden;
    }
    .topline {
      display: flex;
      align-items: center;
      gap: 14px;
      color: #4b5563;
      font-size: 23px;
      font-weight: 650;
      letter-spacing: 0;
    }
    .mark {
      width: 46px;
      height: 46px;
      border-radius: 8px;
      display: grid;
      place-items: center;
      color: white;
      background: #4f46e5;
      font-weight: 800;
      font-size: 25px;
      box-shadow: 0 14px 32px rgba(79,70,229,0.28);
    }
    h1 {
      margin: 48px 0 18px;
      max-width: 730px;
      font-size: 64px;
      line-height: 1.03;
      letter-spacing: 0;
      font-weight: 760;
    }
    .subtitle {
      max-width: 720px;
      font-size: 25px;
      line-height: 1.42;
      color: #475569;
      margin: 0;
    }
    .chips {
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
      margin-top: 34px;
    }
    .chip {
      height: 38px;
      padding: 0 16px;
      border-radius: 7px;
      display: inline-flex;
      align-items: center;
      background: white;
      border: 1px solid #dce3ee;
      color: #334155;
      font-size: 17px;
      font-weight: 650;
      box-shadow: 0 8px 22px rgba(15,23,42,0.06);
    }
    .panel {
      position: absolute;
      right: 64px;
      top: 106px;
      width: 430px;
      background: white;
      border: 1px solid #d9e2ef;
      border-radius: 8px;
      box-shadow: 0 28px 70px rgba(15,23,42,0.14);
      overflow: hidden;
    }
    .panel-head {
      height: 58px;
      background: #111827;
      color: #e5e7eb;
      display: flex;
      align-items: center;
      padding: 0 22px;
      font-size: 17px;
      font-weight: 700;
    }
    .panel-body {
      padding: 22px;
      display: grid;
      gap: 14px;
    }
    .row {
      display: grid;
      grid-template-columns: 42px 1fr;
      gap: 14px;
      align-items: center;
      padding: 15px;
      border: 1px solid #e2e8f0;
      border-radius: 7px;
      background: #fbfdff;
    }
    .icon {
      width: 42px;
      height: 42px;
      border-radius: 7px;
      display: grid;
      place-items: center;
      color: white;
      font-size: 21px;
      font-weight: 800;
    }
    .blue { background: #4f46e5; }
    .green { background: #059669; }
    .amber { background: #d97706; }
    .label {
      font-size: 16px;
      font-weight: 760;
      color: #1f2937;
      margin-bottom: 3px;
    }
    .desc {
      font-size: 13px;
      line-height: 1.35;
      color: #64748b;
    }
    .footer {
      position: absolute;
      left: 64px;
      bottom: 50px;
      display: flex;
      align-items: center;
      gap: 22px;
      color: #64748b;
      font-size: 18px;
      font-weight: 650;
    }
    .dot {
      width: 6px;
      height: 6px;
      border-radius: 50%;
      background: #94a3b8;
    }
  </style>
</head>
<body>
  <main class="frame">
    <div class="topline">
      <div class="mark">F</div>
      <div>Foggy MCP Gateway</div>
    </div>
    <h1>Community MCP access for Odoo 17 data</h1>
    <p class="subtitle">
      A restrained open-source gateway for external MCP clients, embedded query
      execution, PostgreSQL-backed Odoo data, and Odoo-native permissions.
    </p>
    <div class="chips">
      <div class="chip">Odoo 17 Community</div>
      <div class="chip">API Key Auth</div>
      <div class="chip">Embedded Engine</div>
      <div class="chip">12 Query Models</div>
    </div>
    <section class="panel">
      <div class="panel-head">Community runtime boundary</div>
      <div class="panel-body">
        <div class="row">
          <div class="icon blue">M</div>
          <div>
            <div class="label">External MCP clients</div>
            <div class="desc">Claude Desktop, Cursor, and compatible tools connect through JSON-RPC.</div>
          </div>
        </div>
        <div class="row">
          <div class="icon green">O</div>
          <div>
            <div class="label">Odoo permission layer</div>
            <div class="desc">User groups, access rules, and multi-company filters are applied server-side.</div>
          </div>
        </div>
        <div class="row">
          <div class="icon amber">P</div>
          <div>
            <div class="label">PostgreSQL query engine</div>
            <div class="desc">Read-only semantic queries execute against the Odoo database.</div>
          </div>
        </div>
      </div>
    </section>
    <div class="footer">
      <span>No built-in chat</span><span class="dot"></span>
      <span>No provider SDK required</span><span class="dot"></span>
      <span>Best-effort Community maintenance</span>
    </div>
  </main>
</body>
</html>`, { waitUntil: 'load' });
  await page.screenshot({ path: out('banner.png'), fullPage: false });
  await page.close();
}

async function login(page) {
  await page.goto(`${ODOO_URL}/web/login`, { waitUntil: 'domcontentloaded' });
  await page.fill('input[name="login"]', ODOO_LOGIN);
  await page.fill('input[name="password"]', ODOO_PASSWORD);

  const dbSelect = page.locator('select[name="db"]');
  if (await dbSelect.isVisible()) {
    await dbSelect.selectOption(ODOO_DB);
  }

  await page.click('button[type="submit"]');
  await page.waitForURL('**/web**', { timeout: 30_000 });
  await page.waitForLoadState('networkidle', { timeout: 20_000 }).catch(() => {});
}

async function maskSecrets(page) {
  await page.evaluate(() => {
    const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
    const nodes = [];
    while (walker.nextNode()) {
      nodes.push(walker.currentNode);
    }
    for (const node of nodes) {
      if (node.textContent && node.textContent.includes('fmcp_')) {
        node.textContent = node.textContent.replace(/fmcp_[A-Za-z0-9_.=-]+/g, 'fmcp_************************');
      }
    }
  });
}

async function captureOdooScreenshots(browser) {
  const context = await browser.newContext({
    baseURL: ODOO_URL,
    viewport: { width: 1440, height: 900 },
    deviceScaleFactor: 1,
  });
  const page = await context.newPage();
  await login(page);

  await page.goto(`${ODOO_URL}/web#action=base_setup.action_general_configuration`, {
    waitUntil: 'domcontentloaded',
  });
  await page.waitForLoadState('networkidle', { timeout: 30_000 }).catch(() => {});
  await page.locator('.o_form_view').waitFor({ timeout: 60_000 });
  const foggyTab = page.getByRole('tab', { name: /Foggy MCP/i });
  await foggyTab.waitFor({ timeout: 30_000 });
  await foggyTab.click();
  await page.getByText('Engine Mode').first().waitFor({ timeout: 30_000 });
  await page.screenshot({ path: out('screenshot_settings.png'), fullPage: false });

  const wizardBtn = page.locator('button', { hasText: /设置向导|Setup Wizard/i });
  if (await wizardBtn.isVisible()) {
    await wizardBtn.click();
    await page.getByRole('dialog').first().waitFor({ timeout: 30_000 });
    await page.screenshot({ path: out('screenshot_setup_wizard.png'), fullPage: false });
  }

  await page.goto(`${ODOO_URL}/web#action=foggy_mcp.foggy_my_api_key_action`, {
    waitUntil: 'domcontentloaded',
  });
  await page.waitForLoadState('networkidle', { timeout: 30_000 }).catch(() => {});
  await page.locator('.o_list_view, .o_kanban_view').first().waitFor({ timeout: 60_000 });
  await maskSecrets(page);
  await page.screenshot({ path: out('screenshot_api_keys.png'), fullPage: false });

  await context.close();
}

async function main() {
  const browser = await chromium.launch();
  try {
    await renderBanner(browser);
    await captureOdooScreenshots(browser);
  } finally {
    await browser.close();
  }

  for (const name of [
    'banner.png',
    'screenshot_settings.png',
    'screenshot_setup_wizard.png',
    'screenshot_api_keys.png',
  ]) {
    const stat = fs.statSync(out(name));
    console.log(`${name}: ${stat.size} bytes`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
