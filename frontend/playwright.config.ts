import { defineConfig } from '@playwright/test';
export default defineConfig({
  testDir: './e2e',
  fullyParallel: false,
  workers: 1,
  timeout: 120000,
  expect: { timeout: 45000 },
  retries: 0,
  reporter: [
    ['list'],
    ['json', { outputFile: '../backend/.cache/e2e-results.json' }],
  ],
  outputDir: '../backend/.cache/playwright',
  use: {
    baseURL: process.env.TEST_HTTP_URL ?? 'http://127.0.0.1:8081',
    channel: process.env.PLAYWRIGHT_CHANNEL,
    viewport: { width: 1280, height: 800 },
    trace: 'retain-on-failure',
  },
});
